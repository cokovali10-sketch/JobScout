"""Data sources — HTTP, file, RSS, database connectors with retry and rate limiting."""

import json
import csv
import asyncio
import hashlib
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional, Set

import aiohttp

from .models import CollectedItem


class DataSource(ABC):
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self.name = name
        self.config = config or {}

    @abstractmethod
    async def fetch(self) -> AsyncIterator[CollectedItem]:
        yield  # type: ignore

    async def close(self):
        pass


class HTTPSource(DataSource):
    def __init__(self, name: str, url: str, headers: Optional[Dict] = None,
                 parser: str = "json", item_path: str = "",
                 retries: int = 3, retry_delay: float = 1.0,
                 rate_limit: float = 0, **kwargs):
        super().__init__(name, kwargs)
        self.url = url
        self.headers = headers or {}
        self.parser = parser
        self.item_path = item_path
        self.retries = retries
        self.retry_delay = retry_delay
        self.rate_limit = rate_limit
        self._last_request = 0.0

    async def _rate_wait(self):
        if self.rate_limit > 0:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request
            if elapsed < self.rate_limit:
                await asyncio.sleep(self.rate_limit - elapsed)
            self._last_request = asyncio.get_event_loop().time()

    async def fetch(self) -> AsyncIterator[CollectedItem]:
        last_error = None
        for attempt in range(self.retries):
            try:
                await self._rate_wait()
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status == 429:
                            retry_after = float(resp.headers.get("Retry-After", self.retry_delay * (attempt + 1)))
                            await asyncio.sleep(retry_after)
                            continue
                        if resp.status != 200:
                            raise RuntimeError(f"HTTP {resp.status} from {self.url}")

                        if self.parser == "json":
                            data = await resp.json()
                            items = self._resolve_path(data)
                            if not isinstance(items, list):
                                items = [items]
                            for item in items:
                                if isinstance(item, dict):
                                    yield CollectedItem(source=self.name, data=item)

                        elif self.parser == "text":
                            text = await resp.text()
                            yield CollectedItem(source=self.name, data={"text": text})

                        elif self.parser == "csv":
                            text = await resp.text()
                            reader = csv.DictReader(text.splitlines())
                            for row in reader:
                                yield CollectedItem(source=self.name, data=dict(row))

                        return  # Success, exit retry loop
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < self.retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))

        raise RuntimeError(f"Failed after {self.retries} retries: {last_error}")

    def _resolve_path(self, data: Any) -> Any:
        if not self.item_path:
            return data
        for key in self.item_path.split("."):
            if isinstance(data, dict) and key in data:
                data = data[key]
            else:
                return data
        return data


class FileSource(DataSource):
    def __init__(self, name: str, path: str, format: str = "json",
                 encoding: str = "utf-8", **kwargs):
        super().__init__(name, kwargs)
        self.path = path
        self.format = format
        self.encoding = encoding

    def _read_sync(self) -> List[CollectedItem]:
        items = []
        with open(self.path, "r", encoding=self.encoding) as f:
            if self.format == "json":
                data = json.load(f)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict):
                            items.append(CollectedItem(source=self.name, data=item))
                elif isinstance(data, dict):
                    items.append(CollectedItem(source=self.name, data=data))

            elif self.format == "jsonl":
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            item = json.loads(line)
                            if isinstance(item, dict):
                                items.append(CollectedItem(source=self.name, data=item))
                        except json.JSONDecodeError:
                            pass

            elif self.format == "csv":
                reader = csv.DictReader(f)
                for row in reader:
                    items.append(CollectedItem(source=self.name, data=dict(row)))
        return items

    async def fetch(self) -> AsyncIterator[CollectedItem]:
        loop = asyncio.get_event_loop()
        items = await loop.run_in_executor(None, self._read_sync)
        for item in items:
            yield item


class RSSSource(DataSource):
    def __init__(self, name: str, url: str, retries: int = 3,
                 retry_delay: float = 1.0, **kwargs):
        super().__init__(name, kwargs)
        self.url = url
        self.retries = retries
        self.retry_delay = retry_delay

    async def fetch(self) -> AsyncIterator[CollectedItem]:
        import re
        last_error = None
        for attempt in range(self.retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self.url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status != 200:
                            raise RuntimeError(f"HTTP {resp.status} from {self.url}")
                        text = await resp.text()
                        item_blocks = re.findall(r"<item>(.*?)</item>", text, re.DOTALL)
                        for block in item_blocks:
                            data = {}
                            for tag in ["title", "link", "description", "pubDate", "author", "category"]:
                                match = re.search(rf"<{tag}>(.*?)</{tag}>", block, re.DOTALL)
                                if match:
                                    data[tag] = match.group(1).strip()
                            if data:
                                yield CollectedItem(source=self.name, data=data)
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = e
                if attempt < self.retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
        raise RuntimeError(f"Failed after {self.retries} retries: {last_error}")


class Collector:
    def __init__(self):
        self.sources: List[DataSource] = []
        self.results: List[CollectedItem] = []
        self.errors: List[Dict[str, Any]] = []
        self._seen_hashes: Set[str] = set()

    def add_source(self, source: DataSource):
        self.sources.append(source)
        return self

    async def collect_all(self) -> List[CollectedItem]:
        self.results.clear()
        self.errors.clear()
        self._seen_hashes.clear()
        for source in self.sources:
            try:
                async for item in source.fetch():
                    if not self._is_duplicate(item):
                        self.results.append(item)
            except Exception as e:
                self.errors.append({"source": source.name, "error": str(e)})
        return self.results

    async def collect_concurrent(self, max_concurrent: int = 5) -> List[CollectedItem]:
        self.results.clear()
        self.errors.clear()
        self._seen_hashes.clear()
        sem = asyncio.Semaphore(max_concurrent)
        lock = asyncio.Lock()

        async def _fetch_one(src: DataSource):
            async with sem:
                try:
                    async for item in src.fetch():
                        async with lock:
                            if not self._is_duplicate(item):
                                self.results.append(item)
                except Exception as e:
                    async with lock:
                        self.errors.append({"source": src.name, "error": str(e)})

        await asyncio.gather(*[_fetch_one(s) for s in self.sources])
        return self.results

    def _is_duplicate(self, item: CollectedItem) -> bool:
        h = hashlib.md5(json.dumps(item.data, sort_keys=True, default=str).encode()).hexdigest()
        if h in self._seen_hashes:
            return True
        self._seen_hashes.add(h)
        return False

    def deduplicate(self) -> List[CollectedItem]:
        seen = set()
        unique = []
        for item in self.results:
            h = hashlib.md5(json.dumps(item.data, sort_keys=True, default=str).encode()).hexdigest()
            if h not in seen:
                seen.add(h)
                unique.append(item)
        self.results = unique
        return unique

    def filter_items(self, **criteria) -> List[CollectedItem]:
        return [item for item in self.results if item.matches(criteria)]

    def to_json(self) -> str:
        return json.dumps([item.to_dict() for item in self.results], indent=2, default=str)

    async def save_json(self, path: str):
        loop = asyncio.get_event_loop()
        content = self.to_json()
        await loop.run_in_executor(None, lambda: open(path, "w", encoding="utf-8").write(content))

    async def save_jsonl(self, path: str):
        loop = asyncio.get_event_loop()
        lines = "\n".join(item.to_json() for item in self.results)
        await loop.run_in_executor(None, lambda: open(path, "w", encoding="utf-8").write(lines))

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "total_sources": len(self.sources),
            "total_results": len(self.results),
            "total_errors": len(self.errors),
            "errors": self.errors,
        }

    async def close(self):
        for source in self.sources:
            await source.close()
