"""Landing zone to processed zone.

For every landing record whose partition_date falls in [start_date, end_date):
HTML files are reduced to the decision content with BeautifulSoup, PDF/DOC
files pass through unchanged, and the result is written to the processed
bucket as <body>/<partition_date>/<identifier>.<ext> with its metadata
upserted into the processed collection. When two records share an identifier
(the site occasionally publishes a case twice, or mislabels one), the later one
keeps the page name as a suffix so neither file is overwritten. The landing zone
is never modified.

Re-runs skip records whose landing file hash has not changed since they were
last transformed.

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
from wrc_pipeline.storage import ensure_bucket, get_minio_client, get_mongo_collection, get_object, object_exists

logger = logging.getLogger("wrc.transform")

CHROME_SELECTORS = ["header", "footer", "nav", "script", "noscript", "style", "iframe",
                    "#globalCookieBar", ".social-banner", "#skippy"]
BACK_LINK_RE = re.compile(r"Return to Search", re.I)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transform landing-zone documents into the processed zone.")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat, help="inclusive, YYYY-MM-DD")
    parser.add_argument("--end-date", required=True, type=date.fromisoformat, help="exclusive, YYYY-MM-DD")
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
    processed.create_index("file_path")

    query = {
        "partition_date": {"$gte": start_date.isoformat(), "$lt": end_date.isoformat()},
        "file_path": {"$exists": True},
    }
    stats = {"fetched": 0, "transformed": 0, "passed_through": 0, "skipped_unchanged": 0, "failed": 0}
    failures = []
    log_event(logger, "transform_start", start_date=start_date.isoformat(), end_date=end_date.isoformat())

    for record in landing.find(query):
        stats["fetched"] += 1
        try:
            extension = posixpath.splitext(record["file_path"])[1].lower()
            key = processed_key(record, extension)
            other = processed.find_one({"file_path": key, "_id": {"$ne": record["_id"]}}, {"_id": 1})
            if other:
                key = processed_key(record, extension, with_slug=True)
                log_event(logger, "identifier_collision", level=logging.WARNING,
                          id=record["_id"], identifier=record.get("identifier"), other_id=other["_id"], file_path=key)

            already = processed.find_one({"_id": record["_id"]}, {"source_file_hash": 1, "file_path": 1})
            if (
                already
                and already.get("file_path") == key
                and already.get("source_file_hash") == record["file_hash"]
                and object_exists(minio, cfg.processed_bucket, key)
            ):
                stats["skipped_unchanged"] += 1
                continue

            raw = get_object(minio, cfg.landing_bucket, record["file_path"])
            if extension in (".html", ".htm"):
                content, quality = extract_decision_html(raw, record.get("identifier", ""))
                content_type = "text/html; charset=utf-8"
                stats["transformed"] += 1
            else:
                content, quality = raw, {"extraction": "passthrough"}
                content_type = record.get("content_type", "application/octet-stream")
                stats["passed_through"] += 1

            minio.put_object(cfg.processed_bucket, key, io.BytesIO(content), length=len(content),
                             content_type=content_type)
            new_record = {
                **{k: v for k, v in record.items() if k != "_id"},
                "file_bucket": cfg.processed_bucket,
                "file_path": key,
                "file_hash": hashlib.sha256(content).hexdigest(),
                "file_size": len(content),
                "content_type": content_type,
                "source_file_path": record["file_path"],
                "source_file_hash": record["file_hash"],
                "quality": quality,
                "transformed_at": datetime.now(timezone.utc).isoformat(),
            }
            processed.update_one({"_id": record["_id"]}, {"$set": new_record}, upsert=True)
            log_event(logger, "record_transformed", level=logging.DEBUG,
                      identifier=record.get("identifier"), file_path=key, extraction=quality["extraction"])
        except Exception as exc:
            stats["failed"] += 1
            failures.append({"id": record["_id"], "identifier": record.get("identifier"), "error": str(exc)})
            log_event(logger, "transform_failed", level=logging.ERROR,
                      id=record["_id"], identifier=record.get("identifier"), error=str(exc))

    log_event(logger, "transform_summary", **stats, failures=failures)
    return stats


def extract_decision_html(raw_html: bytes, title: str) -> tuple[bytes, dict]:
    """Reduce a case page to its decision content. Returns (html, quality info)."""
    soup = BeautifulSoup(raw_html, "lxml")
    quality = {"extraction": "content-selector"}

    content = next((m for s in DECISION_CONTENT_SELECTORS if (m := soup.select_one(s)) is not None), None)
    if content is None:
        quality["extraction"] = "fallback-body"
        content = soup.body or soup

    for selector in CHROME_SELECTORS:
        for element in content.select(selector):
            element.decompose()
    for link in content.find_all("a", string=BACK_LINK_RE):
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


def processed_key(record: dict, extension: str, with_slug: bool = False) -> str:
    """Same folder as the landing object, file renamed to <identifier>.<ext>.

    `with_slug` appends the case page name, which is unique, for records whose
    identifier is already taken by another record.
    """
    folder = posixpath.dirname(record["file_path"])
    slug = posixpath.splitext(posixpath.basename(record["_id"]))[0]
    # Identifiers like "UD962/2014" or "IR - SC - 00001595" are not valid object names.
    name = re.sub(r"[^A-Za-z0-9._-]+", "-", record.get("identifier", ""))
    name = re.sub(r"-{2,}", "-", name).strip("-.")
    if with_slug and name:
        name = f"{name}-{slug}"
    return posixpath.join(folder, (name or slug) + extension)


if __name__ == "__main__":
    sys.exit(main())
