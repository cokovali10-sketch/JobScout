# JobScout

Async structured data collector with retry logic, rate limiting, and deduplication.

**Version:** 1.0.0 | **Language:** Python | **License:** MIT

## Features

### Core Capabilities
- **Async collection** — gather data from multiple sources concurrently
- **Multiple formats** — HTTP/JSON, CSV, text, RSS/Atom feeds
- **File sources** — JSON, JSONL (line-delimited JSON), CSV files
- **JSON path extraction** — navigate nested API responses with dot notation

### Reliability
- **Automatic retries** — configurable retry count with exponential backoff
- **Rate limiting** — configurable delay between requests per source
- **HTTP 429 handling** — respects Retry-After headers
- **Timeout protection** — per-request timeout prevents hanging

### Data Quality
- **Automatic deduplication** — hash-based dedup across all collected items
- **Explicit dedup** — call `deduplicate()` to remove duplicates after collection
- **Filter by criteria** — filter collected items by tags or field values

### Export
- **JSON export** — structured output for programmatic use
- **JSONL export** — line-delimited for streaming/large datasets
- **Save to file** — async file writing

## Installation

```bash
pip install -e .
```

## CLI Usage

```bash
# Collect from HTTP API
jobscout http users "https://api.example.com/users" --path data.results

# Collect with retries and rate limiting
jobscout http users "https://api.example.com/users" --retries 5 --rate-limit 1.0

# Collect from RSS feed
jobscout rss news "https://news.ycombinator.com/rss"

# Collect from file
jobscout file data ./data.json --format json

# Run from config file
jobscout run sources.json
```

## Config File Format

```json
{
  "sources": [
    {
      "name": "users",
      "type": "http",
      "url": "https://api.example.com/users",
      "parser": "json",
      "item_path": "data.results",
      "retries": 3,
      "rate_limit": 0.5
    },
    {
      "name": "news",
      "type": "rss",
      "url": "https://news.ycombinator.com/rss",
      "retries": 5
    },
    {
      "name": "local",
      "type": "file",
      "path": "./data.csv",
      "format": "csv"
    }
  ]
}
```

## Programmatic API

```python
import asyncio
from jobscout.collector import Collector, HTTPSource, FileSource

async def main():
    collector = Collector()
    
    # Add HTTP source with retry and rate limiting
    collector.add_source(HTTPSource(
        name="users",
        url="https://api.example.com/users",
        retries=3,
        retry_delay=1.0,
        rate_limit=0.5,
        item_path="data.results"
    ))
    
    # Add file source
    collector.add_source(FileSource("local", "./data.json"))
    
    # Collect concurrently (max 5 at a time)
    results = await collector.collect_concurrent(max_concurrent=5)
    
    # Deduplicate
    collector.deduplicate()
    
    # Filter
    filtered = collector.filter_items(type="user")
    
    # Save
    await collector.save_json("output.json")
    await collector.save_jsonl("output.jsonl")
    
    # Stats
    print(collector.stats)
    
    await collector.close()

asyncio.run(main())
```

## Project Structure

```
JobScout/
├── jobscout/
│   ├── __init__.py      # Package metadata
│   ├── models.py        # CollectedItem data model
│   ├── collector.py     # Sources and collector with retry/dedup
│   └── cli.py           # CLI interface
├── tests/
│   └── test_all.py      # 12 unit tests
├── setup.py             # Package configuration
└── README.md
```

## License

MIT
