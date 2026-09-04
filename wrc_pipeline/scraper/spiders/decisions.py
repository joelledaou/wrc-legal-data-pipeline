"""Spider for the WRC "Decisions and Determinations" search.

The search page is an ASP.NET form, but its pagination links expose a plain
GET API:

    /en/search/?decisions=1&from=DD/MM/YYYY&to=DD/MM/YYYY&body=<id>&pageNumber=N

so there is no ViewState to post. For each (partition, body) slice we fetch
page 1, read the total count, and fan out the remaining pages in parallel. The
listing is not stable between requests (ties in the date ordering shift page
boundaries), so a record can appear twice and another not at all. Duplicate rows
are counted and skipped, and a slice whose distinct records fall short of the
reported total is flagged in the summary so it can be re-run.

Every result links to an HTML case page. Usually that page is the decision and
is stored as .html. Older records (Equality Tribunal up to 2002, Employment
Appeals Tribunal up to 2012) are stub pages linking the decision as a PDF or
DOC attachment, which is fetched and stored instead. If the attachment cannot
be fetched (robots.txt disallows the Equality Tribunal import folder), the stub
page is stored and the record is flagged with `attachment_error`.

Records are keyed by document URL path. Anything we already hold is skipped
unless force_refetch is set, in which case the file hash decides whether the
content changed.
"""

from __future__ import annotations

import io
import logging
import math
import posixpath
import re
import zipfile
from datetime import UTC, date, datetime
from urllib.parse import urlencode, urlparse, urlunparse

import scrapy
from scrapy.spidermiddlewares.httperror import HttpError

from wrc_pipeline.config import DECISION_CONTENT_SELECTORS, Settings, parse_bodies
from wrc_pipeline.logging_utils import log_event
from wrc_pipeline.partitions import Partition, build_partitions
from wrc_pipeline.scraper.items import DecisionItem
from wrc_pipeline.scraper.run_stats import RunStats
from wrc_pipeline.storage import get_minio_client, get_mongo_collection, object_exists

logger = logging.getLogger("wrc.ingest")

RESULT_COUNT_RE = re.compile(r"of\s+([\d,]+)\s+results?", re.S)
KNOWN_EXTENSIONS = {".pdf", ".doc", ".docx", ".html", ".htm"}
ATTACHMENT_EXTENSIONS = {".pdf", ".doc", ".docx"}
CONTENT_TYPE_EXT = {
    "application/pdf": ".pdf",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "text/html": ".html",
    "application/xhtml+xml": ".html",
}
# An HTML page with less text than this and no attachment is probably a layout
# we do not understand. It is stored anyway, but flagged.
MIN_CONTENT_CHARS = 100

PDF_MAGIC = b"%PDF-"
OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
HTML_TAG_RE = re.compile(rb"<\s*(!doctype\s+html|html|head|body)\b", re.I)


class DecisionsSpider(scrapy.Spider):
    name = "wrc_decisions"

    def __init__(
        self,
        start_date: str,
        end_date: str,
        bodies: str | None = None,
        force_refetch: str | None = None,
        partition_size: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cfg = Settings()
        # Spider arguments (strings, e.g. from `scrapy crawl -a`) override env config.
        self.partitions = build_partitions(
            date.fromisoformat(start_date), date.fromisoformat(end_date), partition_size or self.cfg.partition_size
        )
        self.bodies = parse_bodies(bodies or self.cfg.bodies)
        self.force_refetch = (
            force_refetch.strip().lower() in ("1", "true", "yes") if force_refetch else self.cfg.force_refetch
        )
        self.run_stats = RunStats()
        self._seen_ids: set[str] = set()
        # Only for the skip check and last_seen touch. The item pipeline owns document writes.
        self._landing = get_mongo_collection(self.cfg, self.cfg.landing_collection)
        self._minio = get_minio_client(self.cfg)

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
                yield self._search_request(partition, body_name, body_id, page=1)

    def parse_search_page(self, response, partition: Partition, body_name: str, body_id: int, page: int):
        if page == 1:
            match = RESULT_COUNT_RE.search(response.text)
            total = int(match.group(1).replace(",", "")) if match else 0
            self.run_stats.slice(partition.label, body_name).found = total
            log_event(
                logger,
                "partition_search",
                partition=partition.label,
                body=body_name,
                date_from=partition.start.isoformat(),
                date_to=partition.end.isoformat(),
                results_found=total,
            )
            last_page = math.ceil(total / self.cfg.results_per_page)
            for extra_page in range(2, last_page + 1):
                yield self._search_request(partition, body_name, body_id, extra_page)

        for row in response.css("#searchResult .each-item"):
            yield from self._handle_result_row(row, response, partition, body_name)

    def _handle_result_row(self, row, response, partition: Partition, body_name: str):
        href = row.css("h2.title a::attr(href)").get() or row.css(".link a::attr(href)").get()
        if not href:
            self.run_stats.add_failure(partition.label, body_name, response.url, "result row without document link")
            log_event(
                logger,
                "record_parse_failed",
                level=logging.WARNING,
                partition=partition.label,
                body=body_name,
                url=response.url,
            )
            return

        doc_url = response.urljoin(href)
        identifier = row.css("span.refNO::text").get() or row.css("h2.title a::text").get() or ""
        title = row.css("h2.title a::text").get() or ""
        description = row.css("p.description::text").get() or row.css("p.description::attr(title)").get() or ""
        record = {
            "record_id": urlparse(doc_url).path,
            "identifier": " ".join(identifier.split()),
            "title": " ".join(title.split()),
            "description": " ".join(description.split()),
            "published_date": self._parse_date(row.css("span.date::text").get()),
            "body": body_name,
            "doc_url": doc_url,
            "partition_date": partition.start.isoformat(),
            "partition_label": partition.label,
        }

        stats = self.run_stats.slice(partition.label, body_name)
        if record["record_id"] in self._seen_ids:
            stats.duplicate_rows += 1
            return
        self._seen_ids.add(record["record_id"])
        stats.listed += 1

        if not self.force_refetch and self._already_stored(record["record_id"]):
            stats.skipped += 1
            self._landing.update_one(
                {"_id": record["record_id"]},
                {"$set": {"last_seen_at": datetime.now(UTC).isoformat()}},
            )
            return

        yield scrapy.Request(
            doc_url, callback=self.parse_document, errback=self.on_document_error, cb_kwargs={"record": record}
        )

    def parse_document(self, response, record: dict, page_response=None):
        """Store the fetched document, or follow its PDF/DOC attachment first.

        `page_response` is set when `response` is an attachment fetched from that
        page. It also stops an HTML error page served for an attachment URL from
        being searched for attachments again.
        """
        content_type = (response.headers.get("Content-Type") or b"").decode("latin-1")
        file_ext = self._file_extension(response)
        if file_ext is None:
            self._reject_document(
                response,
                record,
                "unsupported_document_type",
                logging.WARNING,
                error=f"unsupported document type (Content-Type: {content_type or 'missing'})",
                content_type=content_type,
            )
            return

        if file_ext == ".html" and page_response is None:
            attachment_url = self._attachment_url(response)
            if attachment_url:
                # The page is a stub. It travels along so it can be stored if the attachment fails.
                log_event(
                    logger,
                    "attachment_found",
                    level=logging.DEBUG,
                    partition=record["partition_label"],
                    body=record["body"],
                    identifier=record["identifier"],
                    page_url=response.url,
                    url=attachment_url,
                )
                yield scrapy.Request(
                    attachment_url,
                    callback=self.parse_document,
                    errback=self.on_attachment_error,
                    cb_kwargs={"record": {**record, "attachment_url": attachment_url}, "page_response": response},
                    dont_filter=True,  # two case pages may link the same file; each record needs it
                )
                return
            content_chars = self._content_chars(response)
            if content_chars < MIN_CONTENT_CHARS:
                log_event(
                    logger,
                    "page_without_decision_content",
                    level=logging.WARNING,
                    partition=record["partition_label"],
                    body=record["body"],
                    identifier=record["identifier"],
                    url=response.url,
                    content_chars=content_chars,
                )

        yield from self._document_item(response, record, file_ext, content_type)

    def _document_item(self, response, record: dict, file_ext: str, content_type: str):
        problem = self._content_problem(response.body, file_ext)
        if problem:
            self._reject_document(
                response,
                record,
                "corrupt_document",
                logging.ERROR,
                error=f"corrupt {file_ext} document: {problem}",
                content_type=content_type,
                file_ext=file_ext,
                size=len(response.body),
            )
            return
        yield DecisionItem(
            **record, file_url=response.url, content=response.body, content_type=content_type, file_ext=file_ext
        )

    def on_search_error(self, failure):
        kwargs = failure.request.cb_kwargs
        partition, body_name = kwargs["partition"], kwargs["body_name"]
        error = self._describe_failure(failure)
        self.run_stats.add_failure(partition.label, body_name, failure.request.url, f"search page failed: {error}")
        log_event(
            logger,
            "search_page_failed",
            level=logging.ERROR,
            partition=partition.label,
            body=body_name,
            url=failure.request.url,
            error=error,
        )

    def on_document_error(self, failure):
        record = failure.request.cb_kwargs["record"]
        error = self._describe_failure(failure)
        self.run_stats.add_failure(
            record["partition_label"], record["body"], failure.request.url, error, record["identifier"]
        )
        log_event(
            logger,
            "download_failed",
            level=logging.WARNING,
            partition=record["partition_label"],
            body=record["body"],
            identifier=record["identifier"],
            url=failure.request.url,
            error=error,
        )

    def on_attachment_error(self, failure):
        """Keep the record by storing the stub page, flagged with the reason."""
        record = failure.request.cb_kwargs["record"]
        page = failure.request.cb_kwargs["page_response"]
        error = self._describe_failure(failure)
        self.run_stats.slice(record["partition_label"], record["body"]).attachment_unavailable += 1
        log_event(
            logger,
            "attachment_unavailable",
            level=logging.WARNING,
            partition=record["partition_label"],
            body=record["body"],
            identifier=record["identifier"],
            url=failure.request.url,
            page_url=page.url,
            error=error,
        )
        page_content_type = (page.headers.get("Content-Type") or b"").decode("latin-1")
        yield from self._document_item(page, {**record, "attachment_error": error}, ".html", page_content_type)

    def closed(self, reason: str):
        summary = self.run_stats.summary()
        for stats in summary["slices"]:
            if stats["missing_from_listing"]:
                log_event(
                    logger,
                    "listing_incomplete",
                    level=logging.WARNING,
                    partition=stats["partition"],
                    body=stats["body"],
                    found=stats["found"],
                    listed=stats["listed"],
                    error="site listed fewer distinct records than its result count; re-run this partition",
                )
        log_event(logger, "run_summary", reason=reason, **summary)

    def _search_request(self, partition: Partition, body_name: str, body_id: int, page: int) -> scrapy.Request:
        params = {
            "decisions": "1",
            "from": partition.start.strftime("%d/%m/%Y"),
            "to": partition.end.strftime("%d/%m/%Y"),
            "body": str(body_id),
            "pageNumber": str(page),
        }
        return scrapy.Request(
            f"{self.cfg.base_url.rstrip('/')}{self.cfg.search_path}?{urlencode(params)}",
            callback=self.parse_search_page,
            errback=self.on_search_error,
            cb_kwargs={"partition": partition, "body_name": body_name, "body_id": body_id, "page": page},
        )

    def _already_stored(self, record_id: str) -> bool:
        """True if the record is in Mongo with a hash and its file is in MinIO."""
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

    def _file_extension(self, response) -> str | None:
        """Storage extension from the URL suffix or Content-Type, or None if unsupported."""
        ext = posixpath.splitext(urlparse(response.url).path)[1].lower()
        if ext in KNOWN_EXTENSIONS:
            return ".html" if ext == ".htm" else ext
        content_type = (response.headers.get("Content-Type") or b"").decode("latin-1").split(";")[0].strip()
        return CONTENT_TYPE_EXT.get(content_type.lower())

    def _reject_document(self, response, record: dict, event: str, level: int, error: str, **fields):
        self.run_stats.add_failure(record["partition_label"], record["body"], response.url, error, record["identifier"])
        log_event(
            logger,
            event,
            level=level,
            partition=record["partition_label"],
            body=record["body"],
            identifier=record["identifier"],
            url=response.url,
            error=error,
            **fields,
        )

    def _attachment_url(self, response) -> str | None:
        """PDF/DOC linked from the decision content column, if any.

        Only that column is searched, since every page also links the cookie
        policy and search guide PDFs from its header and footer. The query string
        is dropped because the site links each file twice, once as a thumbnail
        (`?type=pdfPreview&width=200`) and once as the download.
        """
        column = self._decision_content(response)
        if column is None:
            return None
        for href in column.css("a::attr(href)").getall():
            parts = urlparse(response.urljoin(href))
            if posixpath.splitext(parts.path)[1].lower() in ATTACHMENT_EXTENSIONS:
                return urlunparse(parts._replace(query="", fragment=""))
        return None

    def _content_chars(self, response) -> int:
        column = self._decision_content(response)
        if column is None:
            return 0
        return len(" ".join(" ".join(column.xpath(".//text()").getall()).split()))

    @staticmethod
    def _decision_content(response):
        for selector in DECISION_CONTENT_SELECTORS:
            column = response.css(selector)
            if column:
                return column
        return None

    @staticmethod
    def _content_problem(body: bytes, file_ext: str) -> str | None:
        """Why `body` is not a valid document of type `file_ext`, or None if it is."""
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

    @staticmethod
    def _describe_failure(failure) -> str:
        if failure.check(HttpError):
            return f"HTTP {failure.value.response.status}"
        return f"{failure.type.__name__}: {failure.getErrorMessage()}"
