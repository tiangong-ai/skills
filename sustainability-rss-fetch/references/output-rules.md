# Output Rules

## URL Canonicalization and Dedupe Key
Canonicalize URL before dedupe:
- Lowercase scheme and host.
- Remove fragment (`#...`).
- Remove common tracking params (`utm_*`, `ref`, `source`, `fbclid`, `gclid`).
- Keep path and semantic query params.

Build `dedupe_key` priority:
- Feed-scoped `guid`/`id` from feed item (`guid:<feed_url>:<guid>`), to avoid cross-feed collisions.
- `canonical_url(link)`.
- `sha256(feed_url + normalized_title + published_at + summary_head_200)`.

## Two-Phase Ingestion Rule (Required)
1. Collect full candidate window from source feeds using `collect-window`.
2. Screen candidates semantically in agent context using prompt topics.
3. Show metadata (`candidate_id`, `title`, `published_at`, `feed_title`, `url`) and get explicit user confirmation.
4. Persist only confirmed candidates via `insert-selected`.

## Semantic Screening Rule
- Use prompt understanding over title/summary/categories/feed context together.
- Regex-only filtering is not allowed as the final decision mechanism.
- Default topics include:
  - 生命周期评价 (LCA)
  - 物质流分析 (MFA)
  - 绿色供应链
  - 绿电
  - 绿色设计
  - 减污降碳
- Accept user-defined custom sustainability topics and override/extend defaults.

## Persistent Dedupe (Frequency-Agnostic)
1. Keep per-feed cache state:
- `etag`
- `last_modified`
- `last_checked_at`

2. Use conditional requests when state exists (for `sync` mode):
- Send `If-None-Match: <etag>`.
- Send `If-Modified-Since: <last_modified>`.

3. If response is `304 Not Modified`:
- Mark feed as `no_change`.
- Skip item parsing for this feed.

4. Build `dedupe_key` for every parsed item using priority above.

5. Persist into SQLite with unique key:
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
- Candidate file missing selected IDs: fail before write.
- Seen store unavailable: use in-memory dedupe for current run and emit warning.

## Quality Control Checklist
- Candidate window was collected before DB insertion.
- Semantic screening (not regex-only) was applied using prompt topics.
- User confirmation was completed for selected candidates.
- URL canonicalization was applied before dedupe.
- Every output item has a stable `dedupe_key`.
- Feed and entry metadata were persisted successfully.
- No full-text extraction and no summarization were performed.
