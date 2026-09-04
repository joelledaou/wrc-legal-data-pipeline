# WRC Legal Data Pipeline

A Scrapy-based pipeline that scrapes decisions and determinations from
[Workplace Relations](https://www.workplacerelations.ie/en/search/), stores the
documents in object storage (MinIO), the metadata in a NoSQL database
(MongoDB), and transforms the landed data into a clean processed zone,
orchestrated with Dagster. Everything runs in Docker.

## How it works

```
                 ┌────────────── Dagster job ──────────────┐
                 │  ingest_decisions  ──►  transform_decisions
                 └─────────┬─────────────────────┬─────────┘
                           ▼                     ▼
 workplacerelations.ie ─► Scrapy ─► Landing zone          Processed zone
   (4 bodies × monthly            MinIO: raw files   ─►   MinIO: identifier.ext, cleaned HTML
    date partitions)              Mongo: metadata         Mongo: metadata + new path/hash
```

- **Ingestion** scrapes each of the four bodies (Labour Court, WRC, Employment
  Appeals Tribunal, Equality Tribunal) using the site's start/finish date
  filters, one search per monthly partition. Every record gets a
  `partition_date`, its document is downloaded, hashed (SHA-256), stored in
  the `wrc-landing` bucket, and its metadata is upserted into the
  `decisions_landing` collection. Every search result links to an HTML case
  page; when that page's decision content links a PDF/DOC attachment (older
  Equality Tribunal and Employment Appeals Tribunal records), the attachment
  is downloaded and stored as the record's document instead of the page. If
  the attachment cannot be fetched (robots.txt disallows the Equality
  Tribunal import folder), the page is stored, the record is flagged with
  `attachment_error`, and the run summary counts it as `attachment_unavailable`.
- **Transformation** reads a date range back from Mongo, strips WRC page
  chrome from HTML files with BeautifulSoup (PDF/DOC pass through unchanged),
  renames every file to `<identifier>.<ext>` (with the page name as a suffix
  when two records share an identifier), writes it to the `wrc-processed`
  bucket under the same `<body>/<partition_date>/` folder as its landing
  object, and stores the enriched metadata (new path, new hash) in
  `decisions_processed`. The landing zone is never modified.
- **Idempotency**: records are keyed by their document URL; re-running a range
  creates no duplicates and skips files already downloaded. File hashes detect
  content changes (`--force-refetch` re-fetches and re-compares).
- **Logs** are structured JSON on stdout: partition/body being processed,
  found vs. listed vs. scraped counts, every failed download with URL and
  error, a warning for any partition the site listed incompletely, and a run
  summary.

## Prerequisites

- Docker + Docker Compose. Python is not needed on the host.

## Run it

**1. Configure** (defaults work out of the box):

```bash
cp .env.example .env
```

**2. Start everything:**

```bash
docker compose up -d --build
```

This starts MongoDB, MinIO, and the Dagster UI at <http://localhost:3000>.
Published ports are configurable in `.env` (`MONGO_PORT`, `MINIO_API_PORT`,
`MINIO_CONSOLE_PORT`, `DAGSTER_PORT`); the URLs below assume the defaults.

**3. Run the pipeline**, either through Dagster (recommended):

1. Open <http://localhost:3000>
2. Jobs → `wrc_decisions_job` → **Launchpad**
3. Set the date range (end date is exclusive), e.g.:
   ```yaml
   ops:
     ingest_decisions:
       config:
         start_date: "2024-01-01"
         end_date: "2024-02-01"
   ```
   Optional keys: `bodies` (comma-separated subset), `partition_size`
   (`monthly`/`weekly`/`daily`, overriding `PARTITION_SIZE`), `force_refetch`.
4. **Launch Run**. Ingestion runs first, transformation starts when it
   succeeds. Logs stream live in the UI.

or from the command line:

```bash
docker compose run --rm pipeline python -m wrc_pipeline.ingest --start-date 2024-01-01 --end-date 2024-02-01
docker compose run --rm pipeline python -m wrc_pipeline.transform --start-date 2024-01-01 --end-date 2024-02-01
```

> January 2024 contains ~270 decisions across all bodies. Expect the first
> ingestion to take a few minutes: the scraper is deliberately polite
> (AutoThrottle, retries, identified User-Agent, robots.txt respected).

> All 2024 decisions are HTML pages. To exercise the PDF path, use a range
> where the case pages carry attachments, e.g. the Equality Tribunal in 2000
> (42 records) or the Employment Appeals Tribunal in December 2009:
>
> ```bash
> docker compose run --rm pipeline python -m wrc_pipeline.ingest --start-date 2000-01-01 --end-date 2001-01-01 --bodies equality-tribunal
> ```

## Verify the results

Metadata (MongoDB):

```bash
docker compose exec mongodb mongosh -u root -p wrc-secret --quiet --eval '
  db = db.getSiblingDB("wrc");
  print("landing:  ", db.decisions_landing.countDocuments());
  print("processed:", db.decisions_processed.countDocuments());
  printjson(db.decisions_landing.findOne());
'
```

Documents (MinIO): open the console at <http://localhost:9001>
(login `minioadmin` / `minioadmin`) and browse the `wrc-landing` and
`wrc-processed` buckets.

Idempotency: run the same ingestion command again. The `run_summary` log line
shows every already-stored record under `skipped_existing`, with no duplicate
records and no re-downloads.

## Run the tests

Unit tests cover partitioning, hash stability, document validation, HTML
extraction and file naming. They need no running services:

```bash
docker compose run --rm pipeline pytest
docker compose run --rm pipeline ruff check .
```

## Configuration

Everything is configurable via `.env` (see [.env.example](.env.example)):
connection strings, bucket/collection names, partition size
(`monthly`/`weekly`/`daily`), body subset, concurrency, delays, retries,
User-Agent, and log level. No values are hardcoded.

## Project layout

```
wrc_pipeline/
├── config.py                  # all settings, read from environment variables
├── partitions.py              # date range -> monthly/weekly/daily partitions
├── storage.py                 # MongoDB + MinIO clients
├── logging_utils.py           # structured JSON logging
├── ingest.py                  # CLI: run the scraper for a date range
├── transform.py               # CLI: landing zone -> processed zone
├── orchestration.py           # Dagster job (ingest -> transform)
└── scraper/
    ├── settings.py            # Scrapy settings (env-driven)
    ├── items.py               # DecisionItem
    ├── pipelines.py           # hash -> MinIO upload -> Mongo upsert
    └── spiders/decisions.py   # search + pagination + document spider
tests/                         # pytest unit tests (no services needed)
```


See [ARCHITECTURE.md](ARCHITECTURE.md) for design decisions.
