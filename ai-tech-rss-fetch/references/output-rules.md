# Output Rules

## URL Canonicalization and Dedupe Key
Canonicalize URL before dedupe:
- Lowercase scheme and host.
- Remove fragment (`#...`).
- Remove common tracking params (`utm_*`, `ref`, `source`, `fbclid`, `gclid`).
- Keep path and semantic query params.

Build `dedupe_key` priority:
- `guid`/`id` from feed item.
- `canonical_url(link)`.
- `sha256(feed_url + normalized_title + published_at + summary_head_200)`.

## Persistent Dedupe (Frequency-Agnostic)
1. Keep per-feed cache state:
- `etag`
- `last_modified`
- `last_checked_at`

2. Use conditional requests when state exists:
- Send `If-None-Match: <etag>`.
- Send `If-Modified-Since: <last_modified>`.

3. If response is `304 Not Modified`:
- Mark feed as `no_change`.
- Skip item parsing for this feed.

4. Build `dedupe_key` for every parsed item using this priority:
- Use the "URL Canonicalization and Dedupe Key" section above.

5. Persist into seen store (SQLite/kv) with unique key:
- `dedupe_key`
- `content_hash`
- `first_seen_at`
- `last_seen_at`

6. De-dup behavior:
- Existing `dedupe_key` + same `content_hash`: skip as duplicate.
- Existing `dedupe_key` + changed `content_hash`: update metadata row.
- New `dedupe_key`: insert a new metadata row.

7. Retention:
- Delete seen records older than `seen_ttl_days` (default `30`).
- This workflow applies to any run cadence (manual, cron, webhook, or ad hoc).

## Error Handling
- Invalid feed/input format: report failed source and continue with valid sources.
- Missing `guid` and `link`: use hash fallback dedupe key.
- Seen store unavailable: use in-memory dedupe for current run and emit warning.

## Quality Control Checklist
- URL canonicalization was applied before dedupe.
- Conditional request state was applied when available.
- Every output item has a stable `dedupe_key`.
- Feed and entry metadata were persisted successfully.
- No full-text extraction and no summarization were performed.
