"""Hash each document, upload it to MinIO, upsert its metadata in MongoDB.

Objects are stored as <body>/<partition_date>/<page-slug>.<ext>, e.g.
    workplace-relations-commission/2024-01-01/adj-00047352.html
    employment-appeals-tribunal/2009-12-01/pw42_2009.pdf

Metadata is upserted on the record's _id (the document URL path), so re-runs
never create duplicates. The SHA-256 hash detects content changes: unchanged
files are not re-uploaded, changed ones overwrite the object.
"""

from __future__ import annotations

import hashlib
import io
import logging
import posixpath
import re
from datetime import UTC, datetime
from urllib.parse import urlparse

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from wrc_pipeline.config import Settings
from wrc_pipeline.logging_utils import log_event
from wrc_pipeline.storage import ensure_bucket, ensure_landing_indexes, get_minio_client, get_mongo_collection

logger = logging.getLogger("wrc.ingest")

# HTML comments carry server diagnostics that vary between requests ("Elapsed
# time: ...", "cached or not being index.aspx page"). Stripped before hashing,
# or pages would look changed from one run to the next.
HTML_COMMENT_RE = re.compile(rb"<!--.*?-->", re.S)


class MongoMinioStorePipeline:
    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def open_spider(self):
        self.cfg = Settings()
        self.landing = get_mongo_collection(self.cfg, self.cfg.landing_collection)
        self.minio = get_minio_client(self.cfg)
        ensure_landing_indexes(self.landing)
        ensure_bucket(self.minio, self.cfg.landing_bucket)

    def process_item(self, item):
        record = ItemAdapter(item).asdict()
        content = record.pop("content")
        content_type = record.pop("content_type") or "application/octet-stream"
        file_ext = record.pop("file_ext")
        if file_ext == ".html":
            content = HTML_COMMENT_RE.sub(b"", content)

        try:
            self._store(record, content, content_type, file_ext)
        except Exception as exc:
            error = f"store failed: {exc}"
            self.crawler.spider.run_stats.add_failure(
                record["partition_label"], record["body"], record["doc_url"], error, record["identifier"]
            )
            log_event(
                logger,
                "store_failed",
                level=logging.ERROR,
                identifier=record["identifier"],
                url=record["doc_url"],
                error=str(exc),
            )
            raise DropItem(f"storage failed for {record['identifier']}: {exc}") from exc
        return item

    def _store(self, record: dict, content: bytes, content_type: str, file_ext: str) -> None:
        stats = self.crawler.spider.run_stats.slice(record["partition_label"], record["body"])
        record_id = record.pop("record_id")
        file_hash = hashlib.sha256(content).hexdigest()
        file_path = object_key(record, file_ext)
        existing = self.landing.find_one({"_id": record_id}, {"file_hash": 1})
        previous_hash = existing.get("file_hash") if existing else None

        changed = previous_hash != file_hash
        if changed:
            self.minio.put_object(
                self.cfg.landing_bucket, file_path, io.BytesIO(content), length=len(content), content_type=content_type
            )
            stats.downloaded += 1
        else:
            stats.unchanged += 1

        now = datetime.now(UTC).isoformat()
        self.landing.update_one(
            {"_id": record_id},
            {
                "$set": {
                    **record,
                    "file_bucket": self.cfg.landing_bucket,
                    "file_path": file_path,
                    "file_hash": file_hash,
                    "file_size": len(content),
                    "content_type": content_type,
                    "scraped_at": now,
                    "last_seen_at": now,
                },
                "$setOnInsert": {"first_scraped_at": now},
            },
            upsert=True,
        )
        log_event(
            logger,
            "record_stored",
            level=logging.DEBUG,
            identifier=record["identifier"],
            partition=record["partition_label"],
            body=record["body"],
            file_path=file_path,
            file_hash=file_hash,
            previous_hash=previous_hash,
            changed=changed,
        )


def object_key(record: dict, file_ext: str) -> str:
    # Named after the case page so a record's object has the same name whether
    # the page itself or its attachment was stored.
    slug = posixpath.splitext(posixpath.basename(urlparse(record["doc_url"]).path))[0] or "document"
    return f"{record['body']}/{record['partition_date']}/{slug}{file_ext}"
