# Architecture Notes

## Date partition size: monthly (configurable)

The scraper slices the requested range into calendar months and runs one
search per (month × body). The busiest body (WRC) publishes a few hundred
decisions per month, so every slice stays well under the search UI's
pagination depth while keeping the number of search requests small (12 × 4
per year, vs. ~370 × 4 for daily). A failed month can be re-run alone,
idempotently. `PARTITION_SIZE` switches to `weekly`/`daily` if a source ever
publishes too densely for months.

The search's pagination links expose a plain GET API, so we read the total
count from page 1 and fan out the remaining pages concurrently. Every result
links to an HTML case page; older records are stubs linking a PDF/DOC, which
is stored instead.

## Retries and rate limiting

Scrapy's RetryMiddleware retries transient failures (408/429/5xx, timeouts,
connection errors) up to `SCRAPER_RETRY_TIMES`. AutoThrottle targets a modest
concurrency (default 4 per domain) and grows the delay when latency rises or
errors appear, which acts as automatic backoff. The scraper identifies itself
with a custom User-Agent and obeys robots.txt. Anything still failing is
counted per record and logged as JSON with URL and error code, so
`listed = scraped + skipped + failed` always reconciles. `listed` is the
number of distinct records the listing pages actually contained: the site's
pagination is not stable between requests, so duplicate rows are skipped and
any slice whose `listed` falls short of the site's `found` count is flagged
with a warning for a re-run.

## Deduplication strategy

1. **Record identity.** A record's Mongo `_id` is its document URL path,
   which is stable, unique, and independent of which slice found it. All
   writes are upserts, so re-running any range never creates duplicates.
2. **Content identity.** Every stored file carries its SHA-256, computed
   after stripping HTML comments, where the server puts per-request
   diagnostics that would otherwise make pages hash as "changed". Known
   records are skipped without re-downloading; with `--force-refetch` the new
   hash decides whether to re-upload. The transformation uses the same trick
   (`source_file_hash`) to skip unchanged inputs. Processed files are named
   `<identifier>.<ext>`; when the site reuses an identifier, the second record
   keeps its page name as a suffix instead of overwriting.

## Scaling to 50+ sources

- **Source as plugin:** one spider plus a small descriptor per source; the
  metadata schema, storage layout, hashing and logging stay shared.
- **Orchestration:** Dagster partitioned assets per (source × month) give
  independent schedules, backfills and retries without new code.
- **Decouple discovery from download:** publish discovered records to a queue
  and let a horizontally scaled worker pool download, with per-source rate
  budgets and batched existence checks.
- **Operations:** JSON logs feed centralized alerting (e.g. on `failed/found`);
  MinIO and Mongo become S3/GCS and a managed cluster with the same client
  code; secrets move to a vault.
