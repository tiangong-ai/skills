# Output Rules

## Full-Text-First Fallback
Use this strict selection order for each item:

1. Try extracting full article text from `link`.
2. If full text is missing, blocked, or shorter than `min_fulltext_chars`, try RSS content fields:
  - `content:encoded`
  - `content`
3. If still empty, try RSS summary fields:
  - `description`
  - `summary`
4. If all are empty, set `text_source=none` and `text=[no text available]`.

Record chosen source in `text_source`.

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
- `guid`/`id` from feed item.
- Canonicalized article URL (`canonical_url(link)`).
- `sha256(source_name + normalized_title + published_at + text_head_200)`.

5. Persist into seen store (SQLite/kv) with unique key:
- `dedupe_key`
- `content_hash`
- `first_seen_at`
- `last_seen_at`

6. De-dup behavior:
- Existing `dedupe_key` + same `content_hash`: skip as duplicate.
- Existing `dedupe_key` + changed `content_hash`: output as updated item once.
- New `dedupe_key`: output as new item.

7. Retention:
- Delete seen records older than `seen_ttl_days` (default `30`).
- This workflow applies to any run cadence (manual, cron, webhook, or ad hoc).

## Optional Incremental Window
- Use `lookback_hours` (default `24`) to absorb late or edited feed items.
- For full backfills, set a larger range and rely on persistent `dedupe_key` to prevent repeats.
- Use `dedupe_key` to avoid duplicate output inside replay windows.

## Content Filtering
1. Remove duplicates.
- Duplicate rule in one run: same `dedupe_key`.
- If `dedupe_key` cannot be built, fallback to normalized `title + publish_date`.

2. Apply keyword filters.
- Include filter: keep items containing any `include_keywords`.
- Exclude filter: remove items containing any `exclude_keywords`.
- Apply include first, then exclude.

3. Limit by count.
- Keep up to `max_items` after filtering.
- Default order: `published_at` descending.

## Text Cleanup Rules
- Remove HTML tags, scripts, style blocks, cookie banners, and navigation boilerplate.
- Normalize whitespace and keep paragraph boundaries.
- Preserve source meaning and wording.
- Do not summarize, paraphrase, or rewrite unless user explicitly asks.

## Output Format Guidance
- Output format is `text`.
- Do not generate markdown report sections or weekly/daily summaries.
- Return one item block at a time with `title/source/published_at/url/text_source/text`.
- Default output should include only `new` and `updated` items.
- If no new/updated items exist, return `no new items`.

## Error Handling
- Invalid feed/input format: report failed source and continue with valid sources.
- Unreachable article URL: keep item and try RSS content/summary fallback.
- Missing `link` and missing RSS text fields: output `[no text available]`.
- Date/range conflicts (`start > end`): return parameter error and request corrected range.
- Seen store unavailable: use in-memory dedupe for current run and emit warning.

## Quality Control Checklist
- Full-text-first fallback was applied for every item.
- Conditional request state was applied when available.
- Every output item has a stable `dedupe_key`.
- `text_source` is present and accurate for every output item.
- Output contains plain text only (no generated summaries).
- Duplicate items are removed according to dedupe rules.
