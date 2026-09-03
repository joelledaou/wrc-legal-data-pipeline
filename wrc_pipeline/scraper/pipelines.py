"""Item pipeline: hash the document, store it in MinIO, upsert metadata in MongoDB.

Landing-zone layout: <body>/<partition_date>/<page-slug>.<ext>, e.g.
    workplace-relations-commission/2024-01-01/adj-00047352.html
    employment-appeals-tribunal/2009-12-01/pw42_2009.pdf   (PDF attachment)

The slug is the case page's own filename, so a record's object is named the
same way whether the page itself or its PDF/DOC attachment was stored.

Idempotency: metadata is upserted on the record's stable _id (the document URL
path), so re-runs never create duplicates. The SHA-256 file hash detects
content changes: an unchanged file is not re-uploaded, a changed one
overwrites the object and updates the hash.
"""

from __future__ import annotations

import hashlib
import logging
import posixpath
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

from wrc_pipeline.config import get_settings
from wrc_pipeline.logging_utils import log_event
from wrc_pipeline.storage import (
    ensure_bucket,
    ensure_landing_indexes,
    get_minio_client,
    get_mongo_collection,
    put_object,
)

logger = logging.getLogger("wrc.ingest")

# The server appends a render-timing comment (<!-- Elapsed time: 0.12 -->)
# that differs on every request. It is diagnostics, not document content, and
# would make every page hash as "changed" on every run — so it is stripped
# before hashing and storing.
VOLATILE_HTML_RE = re.compile(rb"<!--\s*Elapsed time:[^>]*-->")


class MongoMinioStorePipeline:
    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def open_spider(self):
        self.cfg = get_settings()
        self.landing = get_mongo_collection(self.cfg, self.cfg.landing_collection)
        self.minio = get_minio_client(self.cfg)
        ensure_landing_indexes(self.landing)
        ensure_bucket(self.minio, self.cfg.landing_bucket)

    def process_item(self, item):
        record = ItemAdapter(item).asdict()
        content: bytes = record.pop("content")
        content_type: str = record.pop("content_type") or "application/octet-stream"
        file_ext: str = record.pop("file_ext")
        stats = self.crawler.spider.run_stats.slice(record["partition_label"], record["body"])

        try:
            if file_ext == ".html":
                content = VOLATILE_HTML_RE.sub(b"", content)
            file_hash = hashlib.sha256(content).hexdigest()
            file_path = self._object_key(record, file_ext)
            existing = self.landing.find_one({"_id": record["record_id"]}, {"file_hash": 1})

            now = datetime.now(timezone.utc).isoformat()
            if existing and existing.get("file_hash") == file_hash:
                # Same content as last run — refresh metadata only, keep the object.
                stats.unchanged += 1
                changed = False
            else:
                put_object(self.minio, self.cfg.landing_bucket, file_path, content, content_type)
                stats.downloaded += 1
                changed = True

            self.landing.update_one(
                {"_id": record["record_id"]},
                {
                    "$set": {
                        **{k: v for k, v in record.items() if k != "record_id"},
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
                previous_hash=existing.get("file_hash") if existing else None,
                changed=changed,
            )
        except Exception as exc:
            stats.failed.append({"url": record.get("doc_url"), "identifier": record.get("identifier"),
                                 "error": f"store failed: {exc}"})
            log_event(logger, "store_failed", level=logging.ERROR,
                      identifier=record.get("identifier"), url=record.get("doc_url"), error=str(exc))
            raise DropItem(f"storage failed for {record.get('identifier')}: {exc}") from exc

        return item

    @staticmethod
    def _object_key(record: dict, file_ext: str) -> str:
        slug = posixpath.splitext(posixpath.basename(urlparse(record["doc_url"]).path))[0] or "document"
        return f"{record['body']}/{record['partition_date']}/{slug}{file_ext}"
