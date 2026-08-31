"""Data sources — HTTP, file, RSS, database connectors."""

import json
import csv
import asyncio
from abc import ABC, abstractmethod
from typing import Any, AsyncIterator, Dict, List, Optional

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
                 parser: str = "json", item_path: str = "", **kwargs):
        super().__init__(name, kwargs)
        self.url = url
        self.headers = headers or {}
        self.parser = parser
        self.item_path = item_path

    async def fetch(self) -> AsyncIterator[CollectedItem]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url, headers=self.headers) as resp:
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
    def __init__(self, name: str, path: str, format: str = "json", **kwargs):
        super().__init__(name, kwargs)
        self.path = path
        self.format = format

    async def fetch(self) -> AsyncIterator[CollectedItem]:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._read_sync)

    def _read_sync(self) -> List[CollectedItem]:
        items = []
        with open(self.path, "r", encoding="utf-8") as f:
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
        items = self._read_sync()
        for item in items:
            yield item


class RSSSource(DataSource):
    def __init__(self, name: str, url: str, **kwargs):
        super().__init__(name, kwargs)
        self.url = url

    async def fetch(self) -> AsyncIterator[CollectedItem]:
        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} from {self.url}")
                text = await resp.text()
                for item in self._parse_rss(text):
                    yield item

    def _parse_rss(self, xml_text: str) -> List[CollectedItem]:
        import re
        items = []
        item_blocks = re.findall(r"<item>(.*?)</item>", xml_text, re.DOTALL)
        for block in item_blocks:
            data = {}
            for tag in ["title", "link", "description", "pubDate", "author"]:
                match = re.search(rf"<{tag}>(.*?)</{tag}>", block, re.DOTALL)
                if match:
                    data[tag] = match.group(1).strip()
            if data:
                items.append(CollectedItem(source=self.name, data=data))
        return items


class Collector:
    def __init__(self):
        self.sources: List[DataSource] = []
        self.results: List[CollectedItem] = []
        self.errors: List[Dict[str, Any]] = []

    def add_source(self, source: DataSource):
        self.sources.append(source)
        return self

    async def collect_all(self) -> List[CollectedItem]:
        self.results.clear()
        self.errors.clear()
        for source in self.sources:
            try:
                async for item in source.fetch():
                    self.results.append(item)
            except Exception as e:
                self.errors.append({"source": source.name, "error": str(e)})
        return self.results

    async def collect_concurrent(self, max_concurrent: int = 5) -> List[CollectedItem]:
        self.results.clear()
        self.errors.clear()
        sem = asyncio.Semaphore(max_concurrent)

        async def _fetch_one(src: DataSource):
            async with sem:
                try:
                    async for item in src.fetch():
                        self.results.append(item)
                except Exception as e:
                    self.errors.append({"source": src.name, "error": str(e)})

        await asyncio.gather(*[_fetch_one(s) for s in self.sources])
        return self.results

    def filter_items(self, **criteria) -> List[CollectedItem]:
        return [item for item in self.results if item.matches(criteria)]

    def to_json(self) -> str:
        return json.dumps([item.to_dict() for item in self.results], indent=2, default=str)

    async def save_json(self, path: str):
        loop = asyncio.get_event_loop()
        content = self.to_json()
        await loop.run_in_executor(None, lambda: open(path, "w", encoding="utf-8").write(content))

    async def close(self):
        for source in self.sources:
            await source.close()
