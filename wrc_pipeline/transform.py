"""Transformation: landing zone -> processed zone.

For every landing record whose partition_date falls in [start_date, end_date):

1. fetch its file from the landing bucket (landing data is never modified),
2. PDF/DOC files pass through unchanged; HTML files are reduced to the
   relevant decision content (navigation, header, footer, cookie banner,
   scripts, etc. stripped) with BeautifulSoup,
3. the file is renamed to <identifier>.<ext> and written to the processed
   bucket under the same folder as its landing object
   (<body>/<partition_date>/<identifier>.<ext>), its new SHA-256 hash is
   computed,
4. the enriched metadata (new path, new hash, quality info) is upserted into
   the processed collection.

Idempotent: re-running skips records whose landing file hash has not changed
since they were last transformed.

Usage:
    python -m wrc_pipeline.transform --start-date 2024-01-01 --end-date 2024-02-01
"""

from __future__ import annotations

import argparse
import hashlib
import io
import logging
import posixpath
import re
import sys
from datetime import date, datetime, timezone

from bs4 import BeautifulSoup

from wrc_pipeline.config import DECISION_CONTENT_SELECTORS, Settings
from wrc_pipeline.logging_utils import log_event, setup_json_logging
from wrc_pipeline.storage import (
    ensure_bucket,
    get_minio_client,
    get_mongo_collection,
    get_object,
    object_exists,
)

logger = logging.getLogger("wrc.transform")

# Everything outside the decision content column (header, nav, footer, cookie
# bar, scripts) is chrome and is dropped.
CHROME_SELECTORS = ["header", "footer", "nav", "script", "noscript", "style", "iframe",
                    "#globalCookieBar", ".social-banner", "#skippy"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transform landing-zone documents into the processed zone.")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat,
                        help="Range start, inclusive (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, type=date.fromisoformat,
                        help="Range end, exclusive (YYYY-MM-DD)")
    args = parser.parse_args(argv)

    setup_json_logging(Settings().log_level)
    stats = transform_range(args.start_date, args.end_date)
    return 0 if stats["failed"] == 0 else 2


def transform_range(start_date: date, end_date: date) -> dict:
    cfg = Settings()
    landing = get_mongo_collection(cfg, cfg.landing_collection)
    processed = get_mongo_collection(cfg, cfg.processed_collection)
    minio = get_minio_client(cfg)
    ensure_bucket(minio, cfg.processed_bucket)

    query = {
        "partition_date": {"$gte": start_date.isoformat(), "$lt": end_date.isoformat()},
        "file_path": {"$exists": True},
    }
    stats = {"fetched": 0, "transformed": 0, "passed_through": 0, "skipped_unchanged": 0, "failed": 0}
    failures: list[dict] = []

    log_event(logger, "transform_start", start_date=start_date.isoformat(), end_date=end_date.isoformat())

    for record in landing.find(query):
        stats["fetched"] += 1
        try:
            extension = "." + record["file_path"].rsplit(".", 1)[-1].lower()
            key = processed_key(record, extension)

            already = processed.find_one({"_id": record["_id"]}, {"source_file_hash": 1})
            if (
                already
                and already.get("source_file_hash") == record["file_hash"]
                and object_exists(minio, cfg.processed_bucket, key)
            ):
                stats["skipped_unchanged"] += 1
                continue

            raw = get_object(minio, cfg.landing_bucket, record["file_path"])

            if extension in (".html", ".htm"):
                content, quality = extract_relevant_html(raw, record.get("identifier", ""))
                content_type = "text/html; charset=utf-8"
                stats["transformed"] += 1
            else:
                content, quality = raw, {"extraction": "passthrough"}
                content_type = record.get("content_type", "application/octet-stream")
                stats["passed_through"] += 1

            minio.put_object(cfg.processed_bucket, key, io.BytesIO(content), length=len(content),
                             content_type=content_type)

            new_record = {k: v for k, v in record.items() if k != "_id"}
            new_record.update(
                file_bucket=cfg.processed_bucket,
                file_path=key,
                file_hash=hashlib.sha256(content).hexdigest(),
                file_size=len(content),
                content_type=content_type,
                source_file_path=record["file_path"],
                source_file_hash=record["file_hash"],
                quality=quality,
                transformed_at=datetime.now(timezone.utc).isoformat(),
            )
            processed.update_one({"_id": record["_id"]}, {"$set": new_record}, upsert=True)
            log_event(logger, "record_transformed", level=logging.DEBUG,
                      identifier=record.get("identifier"), file_path=key,
                      extraction=quality["extraction"])
        except Exception as exc:
            stats["failed"] += 1
            failures.append({"id": record["_id"], "identifier": record.get("identifier"), "error": str(exc)})
            log_event(logger, "transform_failed", level=logging.ERROR,
                      id=record["_id"], identifier=record.get("identifier"), error=str(exc))

    log_event(logger, "transform_summary", **stats, failures=failures)
    return stats


def extract_relevant_html(raw_html: bytes, title: str) -> tuple[bytes, dict]:
    """Return (cleaned standalone HTML document, quality info)."""
    soup = BeautifulSoup(raw_html, "lxml")
    quality: dict = {"extraction": "content-selector"}

    content = None
    for selector in DECISION_CONTENT_SELECTORS:
        content = soup.select_one(selector)
        if content is not None:
            break

    if content is None:
        # Unexpected page layout: keep the whole body but strip obvious chrome.
        quality["extraction"] = "fallback-body"
        content = soup.body or soup

    for selector in CHROME_SELECTORS:
        for element in content.select(selector):
            element.decompose()
    # "Return to Search" back-link is navigation, not decision content.
    for link in content.find_all("a", string=re.compile(r"Return to Search", re.I)):
        link.decompose()

    text = content.get_text(" ", strip=True)
    quality["content_chars"] = len(text)
    if len(text) < 100:
        quality["warning"] = "extracted content suspiciously short"

    document = (
        "<!DOCTYPE html>\n"
        f'<html lang="en"><head><meta charset="utf-8"><title>{title}</title></head>\n'
        f"<body>\n{content.decode()}\n</body></html>\n"
    )
    return document.encode("utf-8"), quality


def processed_key(record: dict, extension: str) -> str:
    """Processed object key: same folder as the landing object, file renamed to
    <identifier>.<ext>, so both buckets share one <body>/<partition_date>/
    layout and a processed file can always be traced back to its body."""
    folder = posixpath.dirname(record["file_path"])
    filename = safe_filename(record.get("identifier", ""), record["_id"].split("/")[-1]) + extension
    return posixpath.join(folder, filename) if folder else filename


def safe_filename(identifier: str, fallback: str) -> str:
    """Identifiers like "UD962/2014" or "IR - SC - 00001595" are not valid
    object names; collapse everything unsafe to single dashes."""
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", identifier)
    name = re.sub(r"-{2,}", "-", name).strip("-.")
    return name or fallback


if __name__ == "__main__":
    sys.exit(main())
