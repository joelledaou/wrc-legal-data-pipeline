"""Spider for the WRC "Decisions and Determinations" search.

Strategy
--------
The search page is an ASP.NET form, but it also accepts plain GET queries
(the same ones its own pagination links use):

    /en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&body=<id>&pageNumber=N

so we never need to post ViewState. For every (partition, body) slice we fetch
page 1, read the total result count ("Shows 1 to 10 of N results"), and fan out
the remaining pages in parallel. Each result row is then resolved to its
document, which the item pipeline hashes and stores.

Idempotency: records are keyed by document URL path. A record we already hold
(with a stored file hash and the object present in MinIO) is skipped without
re-downloading; pass force=true / SCRAPER_FORCE_REFETCH=true to re-fetch and
use the file hash to detect content changes.
"""

from __future__ import annotations

import logging
import math
import posixpath
import re
from datetime import date, datetime, timezone
from urllib.parse import urlencode, urlparse

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError

from wrc_pipeline.config import WRC_BODIES, get_settings
from wrc_pipeline.logging_utils import log_event
from wrc_pipeline.partitions import Partition, build_partitions
from wrc_pipeline.scraper.items import DecisionItem
from wrc_pipeline.scraper.run_stats import RunStats
from wrc_pipeline.storage import get_minio_client, get_mongo_collection, object_exists

logger = logging.getLogger("wrc.ingest")

RESULT_COUNT_RE = re.compile(r"of\s+([\d,]+)\s+results?", re.S)
KNOWN_EXTENSIONS = {".pdf", ".doc", ".docx", ".html", ".htm"}
CONTENT_TYPE_EXT = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "text/html": ".html",
}


class DecisionsSpider(scrapy.Spider):
    name = "wrc_decisions"

    def __init__(
        self,
        start_date: str,
        end_date: str,
        bodies: str | None = None,
        force: str | None = None,
        partition_size: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cfg = get_settings()
        self.partitions = build_partitions(
            date.fromisoformat(start_date),
            date.fromisoformat(end_date),
            partition_size or self.cfg.partition_size,
        )
        # Spider arguments (strings, e.g. from `scrapy crawl -a`) override env config.
        if bodies:
            self.bodies = {}
            for name in bodies.split(","):
                key = name.strip().lower()
                if key not in WRC_BODIES:
                    raise ValueError(f"Unknown body '{key}'. Valid bodies: {', '.join(WRC_BODIES)}")
                self.bodies[key] = WRC_BODIES[key]
        else:
            self.bodies = self.cfg.selected_bodies()
        self.force_refetch = (
            force.strip().lower() in ("1", "true", "yes") if force is not None else self.cfg.force_refetch
        )
        self.run_stats = RunStats()
        # Used for the skip-check and last_seen touch; the item pipeline owns document writes.
        self._landing = get_mongo_collection(self.cfg, self.cfg.landing_collection)
        self._minio = get_minio_client(self.cfg)

    # ------------------------------------------------------------------ search

    async def start(self):
        log_event(
            logger,
            "run_start",
            partitions=[p.label for p in self.partitions],
            bodies=list(self.bodies),
            force_refetch=self.force_refetch,
        )
        for partition in self.partitions:
            for body_name, body_id in self.bodies.items():
                yield scrapy.Request(
                    self._search_url(partition, body_id, page=1),
                    callback=self.parse_search_page,
                    errback=self.on_search_error,
                    cb_kwargs={"partition": partition, "body_name": body_name, "body_id": body_id, "page": 1},
                )

    def _search_url(self, partition: Partition, body_id: int, page: int) -> str:
        params = {
            "decisions": "1",
            "from": partition.start.strftime("%d/%m/%Y"),
            "to": partition.end.strftime("%d/%m/%Y"),
            "body": str(body_id),
            "pageNumber": str(page),
        }
        return f"{self.cfg.search_url}?{urlencode(params)}"

    def parse_search_page(self, response, partition: Partition, body_name: str, body_id: int, page: int):
        stats = self.run_stats.slice(partition.label, body_name)

        if page == 1:
            match = RESULT_COUNT_RE.search(response.text)
            total = int(match.group(1).replace(",", "")) if match else 0
            stats.found = total
            log_event(
                logger,
                "partition_search",
                partition=partition.label,
                body=body_name,
                date_from=partition.start.isoformat(),
                date_to=partition.end.isoformat(),
                results_found=total,
            )
            # Fan out the remaining pages in parallel instead of walking "next" links.
            last_page = math.ceil(total / self.cfg.results_per_page)
            for extra_page in range(2, last_page + 1):
                yield scrapy.Request(
                    self._search_url(partition, body_id, extra_page),
                    callback=self.parse_search_page,
                    errback=self.on_search_error,
                    cb_kwargs={
                        "partition": partition,
                        "body_name": body_name,
                        "body_id": body_id,
                        "page": extra_page,
                    },
                )

        for row in response.css("#searchResult .each-item"):
            yield from self._handle_result_row(row, response, partition, body_name)

    # ----------------------------------------------------------------- records

    def _handle_result_row(self, row, response, partition: Partition, body_name: str):
        stats = self.run_stats.slice(partition.label, body_name)
        href = row.css("h2.title a::attr(href)").get() or row.css(".link a::attr(href)").get()
        if not href:
            stats.failed.append({"url": response.url, "error": "result row without document link"})
            log_event(logger, "record_parse_failed", level=logging.WARNING,
                      partition=partition.label, body=body_name, url=response.url)
            return

        doc_url = response.urljoin(href)
        record = {
            "record_id": urlparse(doc_url).path,
            "identifier": _clean(row.css("span.refNO::text").get() or row.css("h2.title a::text").get()),
            "title": _clean(row.css("h2.title a::text").get()),
            "description": _clean(
                row.css("p.description::text").get() or row.css("p.description::attr(title)").get()
            ),
            "published_date": self._parse_date(row.css("span.date::text").get()),
            "body": body_name,
            "doc_url": doc_url,
            "partition_date": partition.start.isoformat(),
            "partition_label": partition.label,
        }

        if not self.force_refetch and self._already_stored(record["record_id"]):
            stats.skipped += 1
            self._landing.update_one(
                {"_id": record["record_id"]},
                {"$set": {"last_seen_at": _utcnow()}},
            )
            return

        yield scrapy.Request(
            doc_url,
            callback=self.parse_document,
            errback=self.on_document_error,
            cb_kwargs={"record": record},
        )

    def _already_stored(self, record_id: str) -> bool:
        """True if the record exists in Mongo with a hash and its file is in MinIO."""
        existing = self._landing.find_one({"_id": record_id}, {"file_hash": 1, "file_path": 1})
        return bool(
            existing
            and existing.get("file_hash")
            and existing.get("file_path")
            and object_exists(self._minio, self.cfg.landing_bucket, existing["file_path"])
        )

    def parse_document(self, response, record: dict):
        item = DecisionItem(
            **record,
            content=response.body,
            content_type=(response.headers.get("Content-Type") or b"").decode("latin-1"),
            file_ext=self._file_extension(response),
        )
        yield item

    def _file_extension(self, response) -> str:
        ext = posixpath.splitext(urlparse(response.url).path)[1].lower()
        if ext in KNOWN_EXTENSIONS:
            return ".html" if ext == ".htm" else ext
        content_type = (response.headers.get("Content-Type") or b"").decode("latin-1").split(";")[0].strip()
        return CONTENT_TYPE_EXT.get(content_type, ".html")

    @staticmethod
    def _parse_date(raw: str | None) -> str | None:
        try:
            return datetime.strptime((raw or "").strip(), "%d/%m/%Y").date().isoformat()
        except ValueError:
            return None

    # ------------------------------------------------------------------ errors

    def on_search_error(self, failure):
        kwargs = failure.request.cb_kwargs
        partition, body_name = kwargs["partition"], kwargs["body_name"]
        error = self._describe_failure(failure)
        self.run_stats.slice(partition.label, body_name).failed.append(
            {"url": failure.request.url, "error": f"search page failed: {error}"}
        )
        log_event(logger, "search_page_failed", level=logging.ERROR,
                  partition=partition.label, body=body_name, url=failure.request.url, error=error)

    def on_document_error(self, failure):
        record = failure.request.cb_kwargs["record"]
        error = self._describe_failure(failure)
        self.run_stats.slice(record["partition_label"], record["body"]).failed.append(
            {"url": failure.request.url, "identifier": record["identifier"], "error": error}
        )
        log_event(logger, "download_failed", level=logging.WARNING,
                  partition=record["partition_label"], body=record["body"],
                  identifier=record["identifier"], url=failure.request.url, error=error)

    @staticmethod
    def _describe_failure(failure) -> str:
        if failure.check(HttpError):
            return f"HTTP {failure.value.response.status}"
        return f"{failure.type.__name__}: {failure.getErrorMessage()}"

    # ----------------------------------------------------------------- summary

    def closed(self, reason: str):
        summary = self.run_stats.summary()
        log_event(logger, "run_summary", reason=reason, **summary)


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(text: str | None) -> str:
    """Collapse the site's embedded newlines/whitespace runs to single spaces."""
    return " ".join((text or "").split())
