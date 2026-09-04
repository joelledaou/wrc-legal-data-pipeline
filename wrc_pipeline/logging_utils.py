"""Structured JSON logging: one JSON object per line on stdout.

Pipeline code emits events with `log_event(logger, "event_name", key=value)`.
Third-party records go through the same formatter so the stream stays parseable.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

_FIELDS_KEY = "_wrc_fields"


def setup_json_logging(level: str = "INFO") -> None:
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root.addHandler(handler)
    root.setLevel(level.upper())
    for noisy in ("scrapy", "twisted", "pymongo", "urllib3", "protego", "filelock"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def log_event(logger: logging.Logger, event: str, level: int = logging.INFO, **fields) -> None:
    logger.log(level, event, extra={_FIELDS_KEY: fields})


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            **getattr(record, _FIELDS_KEY, {}),
        }
        if record.exc_info and record.exc_info[0] is not None:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)
