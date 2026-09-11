"""Data sources — HTTP, file, RSS, WebSocket connectors with retry, rate limiting, caching, and scheduling."""

import json
import csv
import asyncio
import hashlib
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, AsyncIterator, Callable, Dict, List, Optional, Set

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

                        return
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


class WebSocketSource(DataSource):
    def __init__(self, name: str, url: str, max_messages: int = 100,
                 message_field: str = "", parse_json: bool = True,
                 retries: int = 3, retry_delay: float = 1.0, **kwargs):
        super().__init__(name, kwargs)
        self.url = url
        self.max_messages = max_messages
        self.message_field = message_field
        self.parse_json = parse_json
        self.retries = retries
        self.retry_delay = retry_delay

    async def fetch(self) -> AsyncIterator[CollectedItem]:
        import websockets

        last_error = None
        for attempt in range(self.retries):
            try:
                async with websockets.connect(self.url) as ws:
                    count = 0
                    async for raw in ws:
                        if count >= self.max_messages:
                            break
                        try:
                            if self.parse_json:
                                data = json.loads(raw)
                                if self.message_field and isinstance(data, dict):
                                    data = data.get(self.message_field, data)
                            else:
                                data = {"message": raw}
                            if isinstance(data, dict):
                                yield CollectedItem(source=self.name, data=data)
                                count += 1
                            elif isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        yield CollectedItem(source=self.name, data=item)
                                        count += 1
                                        if count >= self.max_messages:
                                            break
                        except (json.JSONDecodeError, TypeError):
                            yield CollectedItem(source=self.name, data={"raw": str(raw)})
                            count += 1
                return
            except Exception as e:
                last_error = e
                if attempt < self.retries - 1:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))
        raise RuntimeError(f"Failed after {self.retries} retries: {last_error}")


@dataclass
class CacheEntry:
    items: List[CollectedItem]
    fetched_at: float
    ttl: float

    def is_expired(self) -> bool:
        return time.time() - self.fetched_at > self.ttl


class Collector:
    def __init__(self, cache_ttl: float = 0):
        self.sources: List[DataSource] = []
        self.results: List[CollectedItem] = []
        self.errors: List[Dict[str, Any]] = []
        self._seen_hashes: Set[str] = set()
        self._cache: Dict[str, CacheEntry] = {}
        self._cache_ttl = cache_ttl
        self._scheduler_task: Optional[asyncio.Task] = None

    def add_source(self, source: DataSource):
        self.sources.append(source)
        return self

    async def collect_all(self) -> List[CollectedItem]:
        self.results.clear()
        self.errors.clear()
        self._seen_hashes.clear()
        for source in self.sources:
            try:
                cached = self._get_cached(source.name)
                if cached is not None:
                    self.results.extend(cached)
                    continue
                async for item in source.fetch():
                    if not self._is_duplicate(item):
                        self.results.append(item)
                self._set_cache(source.name, self.results)
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
                    cached = self._get_cached(src.name)
                    if cached is not None:
                        async with lock:
                            for item in cached:
                                if not self._is_duplicate(item):
                                    self.results.append(item)
                        return
                    collected = []
                    async for item in src.fetch():
                        collected.append(item)
                    async with lock:
                        for item in collected:
                            if not self._is_duplicate(item):
                                self.results.append(item)
                    self._set_cache(src.name, collected)
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

    def _get_cached(self, key: str) -> Optional[List[CollectedItem]]:
        if self._cache_ttl <= 0:
            return None
        entry = self._cache.get(key)
        if entry and not entry.is_expired():
            return entry.items
        return None

    def _set_cache(self, key: str, items: List[CollectedItem]):
        if self._cache_ttl > 0:
            self._cache[key] = CacheEntry(items=list(items), fetched_at=time.time(), ttl=self._cache_ttl)

    def clear_cache(self):
        self._cache.clear()

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

    def deduplicate_fuzzy(self, threshold: float = 0.85) -> List[CollectedItem]:
        unique: List[CollectedItem] = []
        for item in self.results:
            is_dup = False
            for existing in unique:
                if self._fuzzy_match(item.data, existing.data, threshold):
                    is_dup = True
                    break
            if not is_dup:
                unique.append(item)
        self.results = unique
        return unique

    @staticmethod
    def _fuzzy_match(a: Dict[str, Any], b: Dict[str, Any], threshold: float) -> bool:
        keys = set(a.keys()) | set(b.keys())
        if not keys:
            return True
        scores = []
        for key in keys:
            va = str(a.get(key, ""))
            vb = str(b.get(key, ""))
            scores.append(SequenceMatcher(None, va, vb).ratio())
        return sum(scores) / len(scores) >= threshold

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

    def start_scheduler(self, interval: float, callback: Optional[Callable] = None):
        if self._scheduler_task and not self._scheduler_task.done():
            return

        async def _loop():
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.collect_concurrent()
                    if callback:
                        callback(self)
                except Exception:
                    pass

        self._scheduler_task = asyncio.ensure_future(_loop())

    def stop_scheduler(self):
        if self._scheduler_task and not self._scheduler_task.done():
            self._scheduler_task.cancel()
            self._scheduler_task = None

    async def close(self):
        self.stop_scheduler()
        for source in self.sources:
            await source.close()
