# Fetch and Retry Rules

## Discovery Rules

- Fetch `https://www.eceee.org/all-news/` (or `--index-url`).
- Parse anchors with `class="newslink"` under `/all-news/news/`.
- Canonicalize discovered URLs (lowercase host, strip query/fragment, normalize trailing slash).
- Deduplicate by canonical URL before upsert.

## Candidate Selection (`sync`)

Default selection:
- include entries without `entry_content` row
- include failed rows when:
  - retries are not exhausted (`retry_count < max_retries`, unless `max_retries=0`)
  - and cooldown reached (`next_retry_at` is null or <= now)

Optional filters:
- `--force`: include ready rows as well
- `--only-failed`: process only retry-eligible failed rows
- `--since-date YYYY-MM-DD`: limit queue to recent `published_at`
- `--refetch-days N`: include ready rows older than `N` days
- `--oldest-first`: switch queue order from freshness-priority to historical order

## Extraction Rules

1. Download article HTML from `entries.url`.
2. Restrict extraction to article main block (`mainContentColumn`) and strip share/related sections.
3. Attempt `trafilatura` unless `--disable-trafilatura`.
4. Fallback to built-in HTML parser.
5. If extracted chars < `--min-chars`, mark as failure.

## Failure and Retry Rules

- Record fetch/parser/content-type failures in `entry_content.last_error`.
- One failed entry does not stop other entries in the same sync batch.
- Retry schedule uses exponential backoff from `--retry-backoff-minutes`.
- Failed rows stop re-entering default queue at `max_retries` (unless `max_retries=0`).
- `--fail-on-errors` returns non-zero if the run creates new failed states.
