# Schema and State Model

## Required Upstream Table

This skill expects the DOI-keyed `entries` table from `sustainability-rss-fetch` in the same SQLite database.

Required columns:
- `doi`
- `doi_is_surrogate`
- `is_relevant`
- `canonical_url`
- `url`
- `title`

## Companion Table: `entry_content`

One row per DOI (`doi` is primary key).

- `doi`: FK to `entries.doi`.
- `source_url`: URL used for fetch (`https://doi.org/<doi>` for API mode, canonical/raw URL for web mode).
- `final_url`: final URL after redirects (web mode).
- `http_status`: response status when available.
- `extractor`: `openalex`, `semanticscholar`, `trafilatura`, `html-parser`, or `none`.
- `content_kind`: `abstract` or `fulltext`.
- `content_text`: extracted content.
- `content_hash`: SHA-256 of `content_text`.
- `content_length`: text length.
- `fetched_at`: latest fetch attempt timestamp.
- `last_error`: latest failure reason.
- `retry_count`: failure counter, reset to `0` on success.
- `next_retry_at`: next retry time for failed rows.
- `status`: `ready` or `failed`.
- `created_at`, `updated_at`: row timestamps.

## Update Semantics

- Upsert key: `doi`.
- Success:
  - Set `status=ready`.
  - Replace `content_text/hash/length`.
  - Reset `retry_count=0`.
  - Clear `next_retry_at`.
- Failure with existing ready content:
  - Preserve existing content.
  - Keep `status=ready`.
  - Update `last_error` and retry counter.
- Failure without ready content:
  - Set `status=failed`.
  - Increment `retry_count`.
  - Compute `next_retry_at` with exponential backoff.
