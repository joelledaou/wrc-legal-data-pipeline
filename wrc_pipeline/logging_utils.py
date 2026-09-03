"""Structured JSON logging.

Every log line the pipeline emits is a single JSON object on stdout, e.g.:

    {"ts": "2026-09-01T10:00:00Z", "level": "INFO", "logger": "wrc.ingest",
     "event": "partition_start", "partition": "2024-01", "body": "labour-court"}

Use `log_event(logger, "event_name", key=value, ...)` for pipeline events.
Third-party log records (Scrapy warnings etc.) pass through the same formatter
so the whole stream stays machine-parseable.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

_FIELDS_KEY = "_wrc_fields"


def setup_json_logging(level: str = "INFO") -> None:
    """Install a JSON formatter on the root logger (idempotent)."""
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Scrapy/pymongo internals are useful but chatty; keep them at WARNING so
    # the JSON stream stays focused on pipeline events.
    for noisy in ("scrapy", "twisted", "pymongo", "urllib3", "protego", "filelock"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields) -> None:
    """Emit one structured event with arbitrary extra fields."""
    logger.log(level, event, extra={_FIELDS_KEY: fields})


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry: dict = {
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
        }
        entry.update(getattr(record, _FIELDS_KEY, {}))
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)
