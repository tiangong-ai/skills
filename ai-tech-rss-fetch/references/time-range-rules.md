# Time Range Rules

## Sync Frequency
- The workflow is frequency-agnostic. Run manually or on any scheduler cadence.
- Recommended baseline for active feeds: every 15-60 minutes.

## Timestamp Normalization
- Normalize parsed `published` / `updated` timestamps to UTC ISO-8601.
- Persist original ordering with feed publish time when available.
- Keep `first_seen_at` and `last_seen_at` in UTC.

## Incremental Sync Rules
- Use feed caching state (`etag`, `last_modified`) to reduce bandwidth.
- If response status is `304`, do not parse entries for that feed.
- If `etag`/`last_modified` are missing, perform normal fetch and rely on dedupe key.

## Windowing and Retention
- `max_items_per_feed` controls per-run ingestion cap from each feed.
- `seen_ttl_days` controls cleanup of stale metadata records.
- Cleanup should remove records not seen for more than `seen_ttl_days`.

## Missing Publish Time
- If feed item has no publish time, persist record with `published_at=NULL`.
- Use `first_seen_at` as operational fallback for recency ordering.
