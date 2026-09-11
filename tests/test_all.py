"""Tests for JobScout."""

import os
import sys
import json
import asyncio
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from jobscout.models import CollectedItem
from jobscout.collector import Collector, FileSource, WebSocketSource


def test_collected_item():
    item = CollectedItem(source="test", data={"key": "value"})
    assert item.source == "test"
    assert item.data["key"] == "value"
    assert item.item_id.startswith("test_")
    assert item.collected_at != ""


def test_item_to_dict():
    item = CollectedItem(source="test", data={"a": 1}, tags=["important"])
    d = item.to_dict()
    assert d["source"] == "test"
    assert d["tags"] == ["important"]
    assert "data" in d


def test_item_from_dict():
    d = {"source": "test", "data": {"x": 5}, "collected_at": "2024-01-01", "tags": [], "item_id": "abc"}
    item = CollectedItem.from_dict(d)
    assert item.source == "test"
    assert item.data["x"] == 5


def test_item_matches():
    item = CollectedItem(source="test", data={"type": "user", "active": True}, tags=["vip"])
    assert item.matches({"type": "user"})
    assert item.matches({"tags": ["vip"]})
    assert not item.matches({"type": "admin"})
    assert not item.matches({"tags": ["premium"]})


def test_file_source_json():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump([{"name": "Alice"}, {"name": "Bob"}], f)
        path = f.name

    async def run():
        source = FileSource("test", path, format="json")
        items = []
        async for item in source.fetch():
            items.append(item)
        return items

    items = asyncio.run(run())
    os.unlink(path)
    assert len(items) == 2
    assert items[0].data["name"] == "Alice"


def test_file_source_jsonl():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        f.write('{"a": 1}\n{"b": 2}\n')
        path = f.name

    async def run():
        source = FileSource("test", path, format="jsonl")
        items = []
        async for item in source.fetch():
            items.append(item)
        return items

    items = asyncio.run(run())
    os.unlink(path)
    assert len(items) == 2


def test_file_source_csv():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("name,age\nAlice,30\nBob,25\n")
        path = f.name

    async def run():
        source = FileSource("test", path, format="csv")
        items = []
        async for item in source.fetch():
            items.append(item)
        return items

    items = asyncio.run(run())
    os.unlink(path)
    assert len(items) == 2
    assert items[0].data["name"] == "Alice"


def test_collector_concurrent():
    collector = Collector()

    async def run():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([{"id": 1}, {"id": 2}], f)
            path1 = f.name
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([{"id": 3}], f)
            path2 = f.name

        collector.add_source(FileSource("s1", path1))
        collector.add_source(FileSource("s2", path2))
        results = await collector.collect_concurrent()
        os.unlink(path1)
        os.unlink(path2)
        return results

    results = asyncio.run(run())
    assert len(results) == 3


def test_collector_filter():
    collector = Collector()

    async def run():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([{"type": "a", "id": 1}, {"type": "b", "id": 2}, {"type": "a", "id": 3}], f)
            path = f.name
        collector.add_source(FileSource("test", path))
        await collector.collect_concurrent()
        os.unlink(path)
        return collector.filter_items(type="a")

    filtered = asyncio.run(run())
    assert len(filtered) >= 2


def test_collector_to_json():
    collector = Collector()

    async def run():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([{"x": 1}], f)
            path = f.name
        collector.add_source(FileSource("test", path))
        await collector.collect_concurrent()
        os.unlink(path)
        return collector.to_json()

    output = asyncio.run(run())
    data = json.loads(output)
    assert len(data) == 1


def test_cache_ttl():
    collector = Collector(cache_ttl=300)

    async def run():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([{"id": 1}], f)
            path = f.name
        source = FileSource("test", path)
        collector.add_source(source)

        results1 = await collector.collect_concurrent()
        assert len(results1) == 1
        assert "test" in collector._cache

        results2 = await collector.collect_concurrent()
        os.unlink(path)
        return results2

    results = asyncio.run(run())
    assert len(results) == 1


def test_deduplicate_fuzzy():
    collector = Collector()

    async def run():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump([
                {"name": "Alice Smith", "city": "New York"},
                {"name": "Alice Smyth", "city": "New York"},
                {"name": "Bob Jones", "city": "Boston"},
            ], f)
            path = f.name
        collector.add_source(FileSource("test", path))
        await collector.collect_concurrent()
        os.unlink(path)
        return collector.deduplicate_fuzzy(threshold=0.85)

    results = asyncio.run(run())
    assert len(results) == 2


def test_fuzzy_match():
    assert Collector._fuzzy_match({"a": "hello"}, {"a": "hello"}, 0.9)
    assert Collector._fuzzy_match({"a": "hello"}, {"a": "helo"}, 0.8)
    assert not Collector._fuzzy_match({"a": "hello"}, {"a": "xyz"}, 0.9)


def test_cache_expired():
    from jobscout.collector import CacheEntry
    entry = CacheEntry(items=[], fetched_at=0, ttl=1)
    assert entry.is_expired()


def test_websocket_source_init():
    source = WebSocketSource("ws", "ws://localhost:8080", max_messages=50)
    assert source.name == "ws"
    assert source.max_messages == 50


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for t in tests:
        try:
            t()
            print(f"  PASS: {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  FAIL: {t.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
