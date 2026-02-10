# Architecture

## Objective
- Keep abstract fetching stable, legal, and low-maintenance.
- Depend only on official RSS signals and open APIs.
- Avoid any HTML scraping or publisher-specific parsers.

## Components
1. RSS trigger layer
- Extract `doi`, `title`, `link`, `source_feed`.
- Push each DOI event into queue storage.

2. Fetch layer (dual tower)
- Tower A: OpenAlex by DOI.
- Tower B: Semantic Scholar fallback by DOI.

3. Queue layer (SQLite)
- Track `status`, `retry_count`, `max_retries`, `next_retry_at`, and fetched abstract fields.
- Preserve traceability with canonical DOI.

## OpenAlex Abstract Reconstruction
- OpenAlex may expose abstract as `abstract_inverted_index` instead of plain text.
- Rebuild by:
1. Expand `{token: [position...]}` into `(position, token)` pairs.
2. Sort pairs by position.
3. Join tokens into one string.

## Retry Strategy
- First attempt immediately after RSS ingest.
- If OpenAlex and S2 both miss, keep the task as `new`.
- Increment `retry_count` and set `next_retry_at` with backoff.
- Default backoff: `24h, 24h, 48h`.
- Mark `failed` once `retry_count >= max_retries`.

## Recommended Scheduling
- RSS poll: near real-time or every 5-30 minutes.
- Queue runner: at least once per day.
- Optional fast lane: run queue once right after ingest, then daily retry sweep.
- Reserve `queue-run --force` for manual backfill/debug, not cron.

## Environment Variables
- `OPENALEX_EMAIL`: used as `User-Agent` contact for OpenAlex.
- `S2_API_KEY`: optional Semantic Scholar key when higher request allowance is needed.

## Failure Semantics
- `openalex_not_found` / `openalex_no_abstract`: metadata absent or no abstract in OpenAlex.
- `s2_not_found` / `s2_no_abstract`: missing in Semantic Scholar.
- `s2_rate_limited`, `*_network_error`, `*_server_error`: transient failures.
- Transient failures do not consume `retry_count`; they are rescheduled with a short delay.
- Other HTTP errors are recorded into `error_message` for debugging.
