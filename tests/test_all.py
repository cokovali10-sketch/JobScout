"""Tests for JobScout."""

import os
import sys
import json
import asyncio
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from jobscout.models import CollectedItem
from jobscout.collector import Collector, FileSource


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
