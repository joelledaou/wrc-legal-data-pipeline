# Architecture Notes

## Date partition size: monthly (configurable)

The scraper slices the requested range into calendar months and runs one
search per (month × body). Monthly fits this source: the busiest body
(WRC) publishes a few hundred decisions per month, so every slice stays well
under the search UI's pagination depth while keeping the number of search
requests small (12 × 4 per year, vs. ~370 × 4 for daily). A failed month can
be re-run alone, idempotently. `PARTITION_SIZE` switches to
`weekly`/`daily` if a source ever publishes too densely for months.

Although the search page is an ASP.NET ViewState form, its pagination links
expose a plain GET API (`?decisions=1&from=…&to=…&body=…&pageNumber=N`). We
read the total count from page 1 and fan out all remaining pages concurrently,
which enumerates results without simulating form posts. Every result links to an
HTML case page; older Equality Tribunal (≤2002) and Employment Appeals
Tribunal (≤2012) pages are stubs whose content column links the decision as a
PDF, which the spider follows and stores instead of the page (falling back to
the stub, flagged, where robots.txt disallows the attachment).

## Retries and rate limiting

Scrapy's RetryMiddleware retries transient failures (408/429/5xx, timeouts,
connection errors) up to `SCRAPER_RETRY_TIMES` (default 3). Rate limiting is
adaptive: AutoThrottle targets a modest concurrency (default 4 per domain) and
grows the delay when server latency rises or errors appear, which acts as
automatic backoff on 429s. The scraper identifies itself with a custom
User-Agent and obeys robots.txt. Anything still failing after retries is
counted per record and logged as JSON with URL and error code, so
`listed = scraped + skipped + failed` always reconciles in the run summary.
`listed` is the number of distinct records the listing pages actually
contained: the site's pagination is not stable between requests (a record can
appear on two pages while another appears on none), so duplicate rows are
skipped and any slice whose `listed` falls short of the site's `found` count
is flagged with a warning for a re-run.

## Deduplication strategy

Two layers:

1. **Record identity.** A record's Mongo `_id` is its document URL path
   (e.g. `/en/cases/2024/january/adj-00047352.html`), which is stable, unique,
   and independent of which search slice found it. All writes are upserts, so
   re-running any range can never create duplicates.
2. **Content identity.** Every stored file carries its SHA-256 (computed
   after stripping HTML comments, where the server puts per-request
   diagnostics such as render time and a cache marker that would otherwise
   make pages hash as "changed"). On a re-run,
   known records (hash in Mongo + object present in MinIO) are skipped without
   re-downloading. With `--force-refetch`, documents are re-fetched and the new hash
   is compared: identical content is not re-uploaded; changed content
   overwrites the object and updates the hash. The transformation uses the
   same trick (`source_file_hash`) to skip records whose input hasn't changed.
   Processed files are named `<identifier>.<ext>`; the site occasionally
   publishes a case twice or mislabels one, so a record whose identifier is
   already taken keeps its page name as a suffix instead of overwriting.

## Scaling to 50+ sources

- **Source as plugin:** keep the per-source code down to a spider plus a
  small descriptor (base URL, partition hints, parser). The contracts
  (metadata schema, landing/processed layout, hashing, logging) stay shared,
  so pipelines and transformations are written once.
- **Orchestration:** move from one job to Dagster partitioned assets per
  source (source × month), giving independent schedules, backfills, retries,
  and SLA monitoring per source without new code.
- **Decouple discovery from download:** at 1000× volume, publish discovered
  records to a queue and let a horizontally scaled worker pool download.
  Per-source rate budgets replace per-process throttling, and batched lookups
  replace the per-record existence check.
- **Operations:** JSON logs feed centralized alerting (e.g. on `failed/found`);
  MinIO and Mongo become S3/GCS and a managed cluster with the same client
  code; secrets move to a vault.
