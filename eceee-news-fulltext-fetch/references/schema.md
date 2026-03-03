# Schema and State Model

## Discovery Table: `entries`

One row per discovered eceee news URL.

- `id`: integer primary key.
- `url`: canonical article URL (unique).
- `title`: title from all-news index anchor.
- `published_at`: index `data-pubdate` (YYYY-MM-DD) when available.
- `discovered_at`: first discovery timestamp (UTC ISO-8601).
- `last_seen_at`: latest discovery timestamp.
- `created_at`, `updated_at`: row lifecycle timestamps.

## Content Table: `entry_content`

One row per entry (`entry_id` unique, FK to `entries.id`).

- `source_url`: URL used for fetch.
- `final_url`: response URL after redirects.
- `http_status`: HTTP status when available.
- `extractor`: `trafilatura`, `html-parser`, or `none`.
- `content_text`: extracted article text.
- `content_hash`: SHA-256 hash of `content_text`.
- `content_length`: text length.
- `fetched_at`: latest fetch attempt timestamp.
- `last_error`: latest failure reason.
- `retry_count`: cumulative failure counter (reset on success).
- `next_retry_at`: next eligible retry time for failed rows.
- `status`: `ready` or `failed`.
- `created_at`, `updated_at`: row lifecycle timestamps.

## Update Semantics

- Upsert key: `entry_id`.
- Success:
  - set `status=ready`
  - replace content/hash/length
  - reset `retry_count=0`
  - clear `next_retry_at`
- Failure with existing ready content:
  - preserve existing content/hash/length
  - keep `status=ready`
  - update retry metadata and `last_error`
- Failure without ready content:
  - set `status=failed`
  - increment `retry_count`
  - compute `next_retry_at` via exponential backoff
