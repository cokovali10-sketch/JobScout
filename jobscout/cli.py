"""CLI interface for JobScout."""

import argparse
import asyncio
import json
import sys

from .collector import Collector, HTTPSource, FileSource, RSSSource


def main():
    parser = argparse.ArgumentParser(prog="jobscout", description="JobScout — Async data collector")
    sub = parser.add_subparsers(dest="command")

    p_http = sub.add_parser("http", help="Collect from HTTP endpoint")
    p_http.add_argument("name", help="Source name")
    p_http.add_argument("url", help="URL to fetch")
    p_http.add_argument("--parser", choices=["json", "text", "csv"], default="json")
    p_http.add_argument("--path", default="", help="JSON path to items (e.g. data.results)")
    p_http.add_argument("--header", action="append", default=[], help="Header: Key=Value")

    p_file = sub.add_parser("file", help="Collect from file")
    p_file.add_argument("name", help="Source name")
    p_file.add_argument("path", help="File path")
    p_file.add_argument("--format", choices=["json", "jsonl", "csv"], default="json")

    p_rss = sub.add_parser("rss", help="Collect from RSS feed")
    p_rss.add_argument("name", help="Source name")
    p_rss.add_argument("url", help="RSS URL")

    p_run = sub.add_parser("run", help="Run collection from config")
    p_run.add_argument("config", help="JSON config file")

    p_list = sub.add_parser("list", help="List sources")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    collector = Collector()

    if args.command == "http":
        headers = {}
        for h in args.header:
            k, v = h.split("=", 1)
            headers[k] = v
        collector.add_source(HTTPSource(args.name, args.url, headers=headers,
                                       parser=args.parser, item_path=args.path))

    elif args.command == "file":
        collector.add_source(FileSource(args.name, args.path, format=args.format))

    elif args.command == "rss":
        collector.add_source(RSSSource(args.name, args.url))

    elif args.command == "run":
        with open(args.config, "r") as f:
            config = json.load(f)
        for src_cfg in config.get("sources", []):
            src_type = src_cfg.pop("type")
            if src_type == "http":
                collector.add_source(HTTPSource(**src_cfg))
            elif src_type == "file":
                collector.add_source(FileSource(**src_cfg))
            elif src_type == "rss":
                collector.add_source(RSSSource(**src_cfg))

    async def _run():
        results = await collector.collect_concurrent()
        print(json.dumps([item.to_dict() for item in results], indent=2, default=str))
        if collector.errors:
            print(f"\nErrors: {len(collector.errors)}", file=sys.stderr)
            for err in collector.errors:
                print(f"  {err['source']}: {err['error']}", file=sys.stderr)
        await collector.close()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
