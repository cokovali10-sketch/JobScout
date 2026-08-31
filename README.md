# JobScout

Async structured data collector from HTTP APIs, files, and RSS feeds.

## Features

- Async collection from multiple sources concurrently
- HTTP/JSON, CSV, text, RSS parsers
- JSON path extraction for nested API responses
- File sources: JSON, JSONL, CSV
- Filter collected items by tags or field values
- CLI and programmatic API
- Config-driven batch collection

## Usage

```bash
# Collect from HTTP API
jobscout http users "https://api.example.com/users" --path data.results

# Collect from RSS
jobscout rss news "https://news.ycombinator.com/rss"

# Collect from file
jobscout file data ./data.json --format json

# Run from config
jobscout run sources.json
```

## Config File Format

```json
{
  "sources": [
    {"name": "users", "type": "http", "url": "https://api.example.com/users", "parser": "json", "item_path": "data.results"},
    {"name": "news", "type": "rss", "url": "https://news.ycombinator.com/rss"},
    {"name": "local", "type": "file", "path": "./data.csv", "format": "csv"}
  ]
}
```

## Programmatic Use

```python
import asyncio
from jobscout.collector import Collector, HTTPSource

async def main():
    collector = Collector()
    collector.add_source(HTTPSource("users", "https://api.example.com/users"))
    results = await collector.collect_concurrent()
    for item in results:
        print(item.data)
    await collector.close()

asyncio.run(main())
```

## License

MIT
