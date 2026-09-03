"""Per-run counters, sliced by (partition, body), for the end-of-run summary."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class SliceStats:
    found: int = 0  # results the site reports for this slice
    downloaded: int = 0  # fetched and stored (new or changed)
    unchanged: int = 0  # fetched but identical to what we already hold
    skipped: int = 0  # known records not re-fetched
    attachment_unavailable: int = 0  # stub page stored because its attachment failed
    failed: list[dict] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "found": self.found,
            "scraped": self.downloaded + self.unchanged,
            "downloaded": self.downloaded,
            "unchanged": self.unchanged,
            "skipped_existing": self.skipped,
            "attachment_unavailable": self.attachment_unavailable,
            "failed": len(self.failed),
        }


class RunStats:
    def __init__(self) -> None:
        self._slices: dict[tuple[str, str], SliceStats] = {}

    def slice(self, partition: str, body: str) -> SliceStats:
        return self._slices.setdefault((partition, body), SliceStats())

    def add_failure(self, partition: str, body: str, url: str, error: str, identifier: str | None = None) -> None:
        failure = {"url": url, "error": error}
        if identifier is not None:
            failure["identifier"] = identifier
        self.slice(partition, body).failed.append(failure)

    def summary(self) -> dict:
        slices, failures = [], []
        for (partition, body), stats in sorted(self._slices.items()):
            slices.append({"partition": partition, "body": body, **stats.as_dict()})
            failures += [{"partition": partition, "body": body, **f} for f in stats.failed]
        totals = {key: sum(s[key] for s in slices) for key in SliceStats().as_dict()}
        return {"slices": slices, "failures": failures, "totals": totals}
