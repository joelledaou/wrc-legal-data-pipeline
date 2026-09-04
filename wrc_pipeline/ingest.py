"""Scrape WRC decisions for a date range into the landing zone.

python -m wrc_pipeline.ingest --start-date 2024-01-01 --end-date 2024-02-01
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings as ScrapySettings

from wrc_pipeline.config import Settings
from wrc_pipeline.logging_utils import setup_json_logging
from wrc_pipeline.partitions import PARTITION_SIZES
from wrc_pipeline.scraper.spiders.decisions import DecisionsSpider


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    setup_json_logging(Settings().log_level)

    scrapy_settings = ScrapySettings()
    scrapy_settings.setmodule("wrc_pipeline.scraper.settings")

    process = CrawlerProcess(scrapy_settings, install_root_handler=False)
    crawler = process.create_crawler(DecisionsSpider)
    process.crawl(
        crawler,
        start_date=args.start_date.isoformat(),
        end_date=args.end_date.isoformat(),
        bodies=args.bodies,
        force_refetch="true" if args.force_refetch else None,
        partition_size=args.partition_size,
    )
    process.start()
    return 0 if crawler.stats.get_value("finish_reason") == "finished" else 2


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape WRC decisions and determinations.")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat, help="inclusive, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, type=date.fromisoformat, help="exclusive, YYYY-MM-DD")
    parser.add_argument("--bodies", help="comma-separated subset of bodies (default: all four)")
    parser.add_argument("--partition-size", choices=PARTITION_SIZES, help="overrides PARTITION_SIZE")
    parser.add_argument(
        "--force-refetch",
        action="store_true",
        help="re-fetch documents we already hold and detect changes via file hash",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    sys.exit(main())
