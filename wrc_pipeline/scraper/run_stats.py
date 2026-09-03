"""Per-run counters, sliced by (partition, body), for the end-of-run summary log."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SliceStats:
    """Counters for one (partition, body) search slice."""

    found: int = 0        # results the website reports for this slice
    downloaded: int = 0   # documents fetched and stored (new or changed)
    unchanged: int = 0    # documents fetched but identical to what we already hold
    skipped: int = 0      # known records not re-fetched (idempotent fast path)
    attachment_unavailable: int = 0  # stored the stub page because its PDF/DOC attachment failed
    failed: list[dict] = field(default_factory=list)  # {url, error, ...} per failure
    # "scraped" in the summary = downloaded + unchanged (documents successfully fetched).


class RunStats:
    def __init__(self) -> None:
        self._slices: dict[tuple[str, str], SliceStats] = {}

    def slice(self, partition: str, body: str) -> SliceStats:
        return self._slices.setdefault((partition, body), SliceStats())

    def summary(self) -> dict:
        slices = [
            {
                "partition": partition,
                "body": body,
                "found": s.found,
                "scraped": s.downloaded + s.unchanged,
                "skipped_existing": s.skipped,
                "attachment_unavailable": s.attachment_unavailable,
                "failed": len(s.failed),
            }
            for (partition, body), s in sorted(self._slices.items())
        ]
        failures = [
            {"partition": partition, "body": body, **f}
            for (partition, body), s in sorted(self._slices.items())
            for f in s.failed
        ]
        return {
            "slices": slices,
            "failures": failures,
            "totals": {
                "found": sum(s.found for s in self._slices.values()),
                "scraped": sum(s.downloaded + s.unchanged for s in self._slices.values()),
                "downloaded": sum(s.downloaded for s in self._slices.values()),
                "unchanged": sum(s.unchanged for s in self._slices.values()),
                "skipped_existing": sum(s.skipped for s in self._slices.values()),
                "attachment_unavailable": sum(s.attachment_unavailable for s in self._slices.values()),
                "failed": len(failures),
            },
        }
