"""Scrapy settings, driven by the environment (see wrc_pipeline.config)."""

from wrc_pipeline.config import Settings

_cfg = Settings()

BOT_NAME = "wrc_pipeline"
SPIDER_MODULES = ["wrc_pipeline.scraper.spiders"]
NEWSPIDER_MODULE = "wrc_pipeline.scraper.spiders"

# The search and lowercase /en/cases/ pages are allowed by robots.txt, as are the
# /en/eat_import/ attachments. /en/Equality_Tribunal_Import/ is disallowed, so those
# attachments are skipped and the stub page is stored instead.
ROBOTSTXT_OBEY = True
USER_AGENT = _cfg.user_agent

# Moderate fixed concurrency plus AutoThrottle, which adapts the delay to server
# latency and backs off on 429/5xx.
CONCURRENT_REQUESTS = _cfg.concurrent_requests
CONCURRENT_REQUESTS_PER_DOMAIN = _cfg.concurrency_per_domain
DOWNLOAD_DELAY = _cfg.download_delay
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = _cfg.download_delay
AUTOTHROTTLE_MAX_DELAY = _cfg.autothrottle_max_delay
AUTOTHROTTLE_TARGET_CONCURRENCY = float(_cfg.concurrency_per_domain)

RETRY_ENABLED = True
RETRY_TIMES = _cfg.retry_times
RETRY_HTTP_CODES = [408, 429, 500, 502, 503, 504, 522, 524]
DOWNLOAD_TIMEOUT = _cfg.download_timeout

ITEM_PIPELINES = {"wrc_pipeline.scraper.pipelines.MongoMinioStorePipeline": 300}

TELNETCONSOLE_ENABLED = False
HTTPCACHE_ENABLED = False
LOG_LEVEL = _cfg.log_level
FEED_EXPORT_ENCODING = "utf-8"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
