"""Central configuration, read from environment variables.

Every connection string, storage path, partition size, and scraping parameter
lives here so that nothing is hardcoded in the pipeline code. Defaults target
the docker-compose setup shipped with this repository.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


# The four "Body" filters on the left side of the WRC decisions search page,
# mapped to the `body` query-string id the site uses for each of them.
WRC_BODIES: dict[str, int] = {
    "equality-tribunal": 1,
    "employment-appeals-tribunal": 2,
    "labour-court": 3,
    "workplace-relations-commission": 15376,
}


@dataclass(frozen=True)
class Settings:
    """All runtime configuration, resolved once from the environment."""

    # --- Source website ---
    base_url: str = field(
        default_factory=lambda: os.getenv("WRC_BASE_URL", "https://www.workplacerelations.ie")
    )
    search_path: str = field(default_factory=lambda: os.getenv("WRC_SEARCH_PATH", "/en/search/"))
    results_per_page: int = field(default_factory=lambda: int(os.getenv("WRC_RESULTS_PER_PAGE", "10")))
    # Comma-separated subset of WRC_BODIES keys; empty means "all four bodies".
    bodies: str = field(default_factory=lambda: os.getenv("WRC_BODIES", ""))

    # --- Partitioning ---
    # monthly | weekly | daily — how the [start_date, end_date) range is sliced.
    partition_size: str = field(default_factory=lambda: os.getenv("PARTITION_SIZE", "monthly"))

    # --- Scraping behaviour ---
    user_agent: str = field(
        default_factory=lambda: os.getenv(
            "SCRAPER_USER_AGENT",
            "wrc-legal-data-pipeline (research/assessment; contact: joelle.daou03@gmail.com)",
        )
    )
    concurrent_requests: int = field(default_factory=lambda: int(os.getenv("SCRAPER_CONCURRENT_REQUESTS", "8")))
    concurrency_per_domain: int = field(default_factory=lambda: int(os.getenv("SCRAPER_CONCURRENCY_PER_DOMAIN", "4")))
    download_delay: float = field(default_factory=lambda: float(os.getenv("SCRAPER_DOWNLOAD_DELAY", "0.25")))
    autothrottle_max_delay: float = field(default_factory=lambda: float(os.getenv("SCRAPER_AUTOTHROTTLE_MAX_DELAY", "15")))
    retry_times: int = field(default_factory=lambda: int(os.getenv("SCRAPER_RETRY_TIMES", "3")))
    download_timeout: int = field(default_factory=lambda: int(os.getenv("SCRAPER_DOWNLOAD_TIMEOUT", "60")))
    # When true, re-fetch documents even if we already hold them (used to detect
    # content changes via file hash). Default false = fast idempotent re-runs.
    force_refetch: bool = field(default_factory=lambda: _env_bool("SCRAPER_FORCE_REFETCH", False))

    # --- MongoDB (metadata store) ---
    mongo_uri: str = field(default_factory=lambda: os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    mongo_db: str = field(default_factory=lambda: os.getenv("MONGO_DB", "wrc"))
    landing_collection: str = field(default_factory=lambda: os.getenv("MONGO_LANDING_COLLECTION", "decisions_landing"))
    processed_collection: str = field(default_factory=lambda: os.getenv("MONGO_PROCESSED_COLLECTION", "decisions_processed"))

    # --- MinIO (object storage) ---
    minio_endpoint: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    minio_access_key: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    minio_secret_key: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    minio_secure: bool = field(default_factory=lambda: _env_bool("MINIO_SECURE", False))
    landing_bucket: str = field(default_factory=lambda: os.getenv("MINIO_LANDING_BUCKET", "wrc-landing"))
    processed_bucket: str = field(default_factory=lambda: os.getenv("MINIO_PROCESSED_BUCKET", "wrc-processed"))

    # --- Logging ---
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    @property
    def search_url(self) -> str:
        return self.base_url.rstrip("/") + self.search_path

    def selected_bodies(self) -> dict[str, int]:
        """Bodies to scrape: the WRC_BODIES env subset, or all four by default."""
        if not self.bodies.strip():
            return dict(WRC_BODIES)
        selected = {}
        for name in self.bodies.split(","):
            key = name.strip().lower()
            if key not in WRC_BODIES:
                raise ValueError(f"Unknown body '{key}'. Valid bodies: {', '.join(WRC_BODIES)}")
            selected[key] = WRC_BODIES[key]
        return selected


def get_settings() -> Settings:
    return Settings()


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")
