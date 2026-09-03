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

Every result row links to an HTML case page. For most records that page *is*
the decision and is stored as .html. Older records (Equality Tribunal up to
2002, Employment Appeals Tribunal up to 2012) are stub pages whose decision
content column links the real decision as a PDF (or DOC) attachment; in that
case the attachment is fetched and stored as the record's document instead.
If the attachment cannot be fetched (the site's robots.txt disallows the
Equality Tribunal import folder, for instance), the stub page is stored so the
record and its metadata are not lost, and the record is flagged with
`attachment_error`.

Idempotency: records are keyed by document URL path. A record we already hold
(with a stored file hash and the object present in MinIO) is skipped without
re-downloading; pass force=true / SCRAPER_FORCE_REFETCH=true to re-fetch and
use the file hash to detect content changes.
"""

from __future__ import annotations

import io
import logging
import math
import posixpath
import re
import zipfile
from datetime import date, datetime, timezone
from urllib.parse import urlencode, urlparse, urlunparse

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError

from wrc_pipeline.config import DECISION_CONTENT_SELECTORS, WRC_BODIES, get_settings
from wrc_pipeline.logging_utils import log_event
from wrc_pipeline.partitions import Partition, build_partitions
from wrc_pipeline.scraper.items import DecisionItem
from wrc_pipeline.scraper.run_stats import RunStats
from wrc_pipeline.storage import get_minio_client, get_mongo_collection, object_exists

logger = logging.getLogger("wrc.ingest")

RESULT_COUNT_RE = re.compile(r"of\s+([\d,]+)\s+results?", re.S)
# Extensions we know how to store and transform. A document that resolves to
# anything else (by URL suffix or Content-Type) is logged and not yielded.
KNOWN_EXTENSIONS = {".pdf", ".doc", ".docx", ".html", ".htm"}
CONTENT_TYPE_EXT = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "text/html": ".html",
    "application/xhtml+xml": ".html",
}
# A link with one of these extensions inside the decision content column is the
# record's real document (requirement 6a); the page around it is only a stub.
ATTACHMENT_EXTENSIONS = {".pdf", ".doc", ".docx"}
# A stored HTML page with less text than this and no attachment is almost
# certainly a layout we do not understand; it is kept but flagged.
MIN_CONTENT_CHARS = 100
# Magic bytes per format, used to reject truncated/corrupt downloads and
# mislabelled responses (e.g. an HTML error page served for a ".pdf" URL).
PDF_MAGIC = b"%PDF-"
OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"  # legacy .doc container
HTML_TAG_RE = re.compile(rb"<\s*(!doctype\s+html|html|head|body)\b", re.I)


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

    # -------------------------------------------------------------- crawl flow
    # start -> search pages -> result rows -> documents (stored by the item pipeline)

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

    def parse_document(self, response, record: dict, page_response=None):
        """Store the fetched document, or follow its PDF/DOC attachment first.

        `page_response` is set when `response` is an attachment fetched from
        that page; it also stops an HTML error page served for an attachment
        URL from being searched for attachments again."""
        content_type = (response.headers.get("Content-Type") or b"").decode("latin-1")
        file_ext = self._file_extension(response)
        if file_ext is None:
            self._reject_document(
                response, record, "unsupported_document_type", logging.WARNING,
                error=f"unsupported document type (Content-Type: {content_type or 'missing'})",
                content_type=content_type,
            )
            return

        if file_ext == ".html" and page_response is None:
            attachment_url = self._attachment_url(response)
            if attachment_url:
                # The page is a stub; the decision is the linked PDF/DOC. The page
                # travels along so it can be stored instead if the attachment fails.
                log_event(logger, "attachment_found", level=logging.DEBUG,
                          partition=record["partition_label"], body=record["body"],
                          identifier=record["identifier"], page_url=response.url, url=attachment_url)
                yield scrapy.Request(
                    attachment_url,
                    callback=self.parse_document,
                    errback=self.on_attachment_error,
                    cb_kwargs={"record": {**record, "attachment_url": attachment_url},
                               "page_response": response},
                )
                return
            content_chars = self._content_chars(response)
            if content_chars < MIN_CONTENT_CHARS:
                # Stored anyway (it is what the site serves), but flagged so a
                # layout change or a new attachment style does not go unnoticed.
                log_event(logger, "page_without_decision_content", level=logging.WARNING,
                          partition=record["partition_label"], body=record["body"],
                          identifier=record["identifier"], url=response.url,
                          content_chars=content_chars)

        yield from self._document_item(response, record, file_ext, content_type)

    def _document_item(self, response, record: dict, file_ext: str, content_type: str):
        """Validate the response body for its format and yield it as the record's document."""
        problem = self._content_problem(response.body, file_ext)
        if problem:
            self._reject_document(
                response, record, "corrupt_document", logging.ERROR,
                error=f"corrupt {file_ext} document: {problem}",
                content_type=content_type, file_ext=file_ext, size=len(response.body),
            )
            return

        yield DecisionItem(
            **record,
            file_url=response.url,
            content=response.body,
            content_type=content_type,
            file_ext=file_ext,
        )

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

    def on_attachment_error(self, failure):
        """The PDF/DOC attachment could not be fetched: keep the record by storing
        the stub page it was linked from, flagged with the reason, and count it
        separately in the summary so the gap is visible."""
        record = failure.request.cb_kwargs["record"]
        page = failure.request.cb_kwargs["page_response"]
        error = self._describe_failure(failure)
        self.run_stats.slice(record["partition_label"], record["body"]).attachment_unavailable += 1
        log_event(logger, "attachment_unavailable", level=logging.WARNING,
                  partition=record["partition_label"], body=record["body"],
                  identifier=record["identifier"], url=failure.request.url, page_url=page.url,
                  error=error)
        page_content_type = (page.headers.get("Content-Type") or b"").decode("latin-1")
        yield from self._document_item(page, {**record, "attachment_error": error}, ".html", page_content_type)

    # ----------------------------------------------------------------- summary

    def closed(self, reason: str):
        summary = self.run_stats.summary()
        log_event(logger, "run_summary", reason=reason, **summary)

    # ----------------------------------------------------------------- helpers
    # Listed in the order they are first used by the methods above.

    def _search_url(self, partition: Partition, body_id: int, page: int) -> str:
        params = {
            "decisions": "1",
            "from": partition.start.strftime("%d/%m/%Y"),
            "to": partition.end.strftime("%d/%m/%Y"),
            "body": str(body_id),
            "pageNumber": str(page),
        }
        return f"{self.cfg.search_url}?{urlencode(params)}"

    def _already_stored(self, record_id: str) -> bool:
        """True if the record exists in Mongo with a hash and its file is in MinIO."""
        existing = self._landing.find_one({"_id": record_id}, {"file_hash": 1, "file_path": 1})
        return bool(
            existing
            and existing.get("file_hash")
            and existing.get("file_path")
            and object_exists(self._minio, self.cfg.landing_bucket, existing["file_path"])
        )

    @staticmethod
    def _parse_date(raw: str | None) -> str | None:
        try:
            return datetime.strptime((raw or "").strip(), "%d/%m/%Y").date().isoformat()
        except ValueError:
            return None

    @staticmethod
    def _decision_content(response):
        """The selector for the decision content column of a case page, or None."""
        for selector in DECISION_CONTENT_SELECTORS:
            column = response.css(selector)
            if column:
                return column
        return None

    def _attachment_url(self, response) -> str | None:
        """Absolute URL of the PDF/DOC linked from the decision content column, if any.

        Only that column is searched: every page also links the site's cookie
        policy and search guide PDFs from its header/footer. Query strings are
        dropped because the site links the same file twice, once as a
        thumbnail (`?type=pdfPreview&width=200`) and once as the download."""
        column = self._decision_content(response)
        if column is None:
            return None
        for href in column.css("a::attr(href)").getall():
            parts = urlparse(response.urljoin(href))
            if posixpath.splitext(parts.path)[1].lower() in ATTACHMENT_EXTENSIONS:
                return urlunparse(parts._replace(query="", fragment=""))
        return None

    def _content_chars(self, response) -> int:
        """Length of the visible text in the decision content column (0 if not found)."""
        column = self._decision_content(response)
        if column is None:
            return 0
        return len(" ".join(" ".join(column.xpath(".//text()").getall()).split()))

    def _file_extension(self, response) -> str | None:
        """Resolve the storage extension, or None if the document type is not one we handle."""
        ext = posixpath.splitext(urlparse(response.url).path)[1].lower()
        if ext in KNOWN_EXTENSIONS:
            return ".html" if ext == ".htm" else ext
        content_type = (response.headers.get("Content-Type") or b"").decode("latin-1").split(";")[0].strip()
        return CONTENT_TYPE_EXT.get(content_type.lower())

    @staticmethod
    def _content_problem(body: bytes, file_ext: str) -> str | None:
        """Return a description of why `body` is not a valid `file_ext` document, or None if it is."""
        if not body:
            return "empty response body"
        if file_ext == ".pdf":
            if not body.startswith(PDF_MAGIC):
                return "missing %PDF header"
            if b"%%EOF" not in body[-2048:]:
                return "missing %%EOF trailer (truncated download)"
        elif file_ext == ".doc":
            if not body.startswith(OLE2_MAGIC):
                return "missing OLE2 header"
        elif file_ext == ".docx":
            try:
                with zipfile.ZipFile(io.BytesIO(body)) as archive:
                    if archive.testzip() is not None:
                        return "zip archive fails CRC check"
                    if "word/document.xml" not in archive.namelist():
                        return "zip archive has no word/document.xml"
            except zipfile.BadZipFile as exc:
                return f"not a valid zip archive ({exc})"
        elif file_ext == ".html":
            if not HTML_TAG_RE.search(body[:8192]):
                return "no HTML document markup found"
        return None

    def _reject_document(self, response, record: dict, event: str, level: int, error: str, **fields):
        """Record a document we will not store: count it as failed and log why."""
        self.run_stats.slice(record["partition_label"], record["body"]).failed.append(
            {"url": response.url, "identifier": record["identifier"], "error": error}
        )
        log_event(logger, event, level=level,
                  partition=record["partition_label"], body=record["body"],
                  identifier=record["identifier"], url=response.url, error=error, **fields)

    @staticmethod
    def _describe_failure(failure) -> str:
        if failure.check(HttpError):
            return f"HTTP {failure.value.response.status}"
        return f"{failure.type.__name__}: {failure.getErrorMessage()}"


def _clean(text: str | None) -> str:
    """Collapse the site's embedded newlines/whitespace runs to single spaces."""
    return " ".join((text or "").split())


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()
