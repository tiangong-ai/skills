# Schema and State Model

## Required Upstream Table

This skill expects the `entries` table from `sustainability-rss-fetch` to already exist in the same SQLite database.

Required columns used by this skill:
- `id`
- `canonical_url`
- `url`
- `title`

## Companion Table: `entry_content`

One row per upstream entry (`entry_id` is unique).

- `entry_id`: FK to `entries.id`.
- `source_url`: URL used for fetch (canonical first, raw fallback).
- `final_url`: response URL after redirects.
- `http_status`: response status when available.
- `extractor`: `trafilatura`, `html-parser`, or `none`.
- `content_text`: extracted plain text body.
- `content_hash`: SHA-256 of `content_text`.
- `content_length`: text length.
- `fetched_at`: latest fetch attempt timestamp.
- `last_error`: latest failure reason.
- `retry_count`: failure counter, reset to `0` on success.
- `next_retry_at`: next eligible retry time for failed rows (UTC ISO-8601).
- `status`: `ready` or `failed`.
- `created_at`, `updated_at`: row timestamps.

## Update Semantics

- Upsert key: `entry_id`.
- Success:
  - Set `status=ready`.
  - Replace text/hash/length.
  - Reset `retry_count=0`.
  - Clear `next_retry_at`.
- Failure with existing ready content:
  - Preserve existing text/hash/length.
  - Keep `status=ready`.
  - Increment `retry_count`, write `last_error`.
  - Keep `next_retry_at=NULL` (no failed queue state).
- Failure without ready content:
  - Set `status=failed`.
  - Increment `retry_count`.
  - Compute `next_retry_at` with exponential backoff.
  - When `retry_count` reaches `max_retries` (default `3`), this row stops entering default retry queue.
