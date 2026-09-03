"""Runtime configuration, read from environment variables.

Defaults match the docker-compose setup in this repository.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# The four "Body" filters on the WRC search page and the `body` query id each one uses.
WRC_BODIES = {
    "equality-tribunal": 1,
    "employment-appeals-tribunal": 2,
    "labour-court": 3,
    "workplace-relations-commission": 15376,
}

# Where the decision lives on a case page: the right-hand column of the main
# container. Everything outside it is site chrome. Shared by the spider (to find
# attachments) and the transformation (to extract content). First match wins.
DECISION_CONTENT_SELECTORS = ("div.container.mb-4 div.col-sm-9", "div.container.mb-4")


# Defined before Settings because dataclass field defaults are evaluated at class creation.
def _env(name: str, default):
    """Dataclass field read from the environment, cast to the default's type."""
    if isinstance(default, bool):
        return field(default_factory=lambda: os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on"))
    return field(default_factory=lambda: type(default)(os.getenv(name, str(default))))


@dataclass(frozen=True)
class Settings:
    # source website
    base_url: str = _env("WRC_BASE_URL", "https://www.workplacerelations.ie")
    search_path: str = _env("WRC_SEARCH_PATH", "/en/search/")
    results_per_page: int = _env("WRC_RESULTS_PER_PAGE", 10)
    bodies: str = _env("WRC_BODIES", "")

    # monthly | weekly | daily
    partition_size: str = _env("PARTITION_SIZE", "monthly")

    # scraping behaviour
    user_agent: str = _env(
        "SCRAPER_USER_AGENT", "wrc-legal-data-pipeline (research/assessment; contact: joelle.daou03@gmail.com)"
    )
    concurrent_requests: int = _env("SCRAPER_CONCURRENT_REQUESTS", 8)
    concurrency_per_domain: int = _env("SCRAPER_CONCURRENCY_PER_DOMAIN", 4)
    download_delay: float = _env("SCRAPER_DOWNLOAD_DELAY", 0.25)
    autothrottle_max_delay: float = _env("SCRAPER_AUTOTHROTTLE_MAX_DELAY", 15.0)
    retry_times: int = _env("SCRAPER_RETRY_TIMES", 3)
    download_timeout: int = _env("SCRAPER_DOWNLOAD_TIMEOUT", 60)
    # Re-fetch documents we already hold so content changes show up via the file hash.
    force_refetch: bool = _env("SCRAPER_FORCE_REFETCH", False)

    # MongoDB
    mongo_uri: str = _env("MONGO_URI", "mongodb://localhost:27017")
    mongo_db: str = _env("MONGO_DB", "wrc")
    landing_collection: str = _env("MONGO_LANDING_COLLECTION", "decisions_landing")
    processed_collection: str = _env("MONGO_PROCESSED_COLLECTION", "decisions_processed")

    # MinIO
    minio_endpoint: str = _env("MINIO_ENDPOINT", "localhost:9000")
    minio_access_key: str = _env("MINIO_ACCESS_KEY", "minioadmin")
    minio_secret_key: str = _env("MINIO_SECRET_KEY", "minioadmin")
    minio_secure: bool = _env("MINIO_SECURE", False)
    landing_bucket: str = _env("MINIO_LANDING_BUCKET", "wrc-landing")
    processed_bucket: str = _env("MINIO_PROCESSED_BUCKET", "wrc-processed")

    log_level: str = _env("LOG_LEVEL", "INFO")


def parse_bodies(spec: str) -> dict[str, int]:
    """Comma-separated subset of WRC_BODIES keys; empty means all four."""
    if not spec.strip():
        return dict(WRC_BODIES)
    selected = {}
    for name in spec.split(","):
        key = name.strip().lower()
        if key not in WRC_BODIES:
            raise ValueError(f"Unknown body '{key}'. Valid bodies: {', '.join(WRC_BODIES)}")
        selected[key] = WRC_BODIES[key]
    return selected
