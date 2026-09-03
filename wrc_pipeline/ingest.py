"""Ingestion entrypoint: scrape WRC decisions for a date range into the landing zone.

Usage:
    python -m wrc_pipeline.ingest --start-date 2024-01-01 --end-date 2024-02-01
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings as ScrapySettings

from wrc_pipeline.config import get_settings
from wrc_pipeline.logging_utils import setup_json_logging
from wrc_pipeline.scraper.spiders.decisions import DecisionsSpider


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    cfg = get_settings()
    setup_json_logging(cfg.log_level)

    scrapy_settings = ScrapySettings()
    scrapy_settings.setmodule("wrc_pipeline.scraper.settings")

    process = CrawlerProcess(scrapy_settings, install_root_handler=False)
    crawler = process.create_crawler(DecisionsSpider)
    process.crawl(
        crawler,
        start_date=args.start_date.isoformat(),
        end_date=args.end_date.isoformat(),
        bodies=args.bodies,
        force="true" if args.force else None,
        partition_size=args.partition_size,
    )
    process.start()

    finish_reason = crawler.stats.get_value("finish_reason")
    return 0 if finish_reason == "finished" else 2


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape WRC decisions and determinations.")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat,
                        help="Range start, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, type=date.fromisoformat,
                        help="Range end, exclusive (YYYY-MM-DD)")
    parser.add_argument("--bodies", default=None,
                        help="Comma-separated subset of bodies (default: all four)")
    parser.add_argument("--partition-size", default=None, choices=["monthly", "weekly", "daily"],
                        help="Override PARTITION_SIZE from the environment")
    parser.add_argument("--force", action="store_true",
                        help="Re-fetch documents we already hold (detect changes via file hash)")
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
