# Fetch and Retry Rules

## Candidate Selection

Default `sync` selection:
- Include entries with URL (`canonical_url` or `url`) present.
- Include rows with missing content state (`entry_content` absent).
- Include rows with `status=failed` only when:
  - retry is not exhausted (`retry_count < max_retries`, default `3`);
  - and retry cooldown is reached (`next_retry_at` is NULL or <= now).
- Default queue order prioritizes fresher rows to maximize success under bounded `--limit`.

Optional selection controls:
- `--force`: include all rows (including ready).
- `--only-failed`: include only failed rows.
- `--refetch-days N`: include ready rows older than `N` days.
- `--oldest-first`: revert to historical oldest-first processing order for backfills.
- `--max-retries`: cap failed retries per entry (`3` by default, `0` means unlimited).
- `--retry-backoff-minutes`: base minutes for exponential cooldown between failed retries.

## URL and Extraction Priority

1. Pick URL:
- `canonical_url` first.
- fallback `url`.

2. Extract body text:
- Prefer `trafilatura` if installed and not disabled.
- Fallback to built-in HTML parser.

3. Quality threshold:
- Enforce `--min-chars`.
- Treat too-short extraction as failure.

## Failure Handling

- HTTP/network/parsing/content-type errors are recorded per entry.
- One failed entry does not stop the rest of the sync batch.
- Failed retries use exponential backoff and stop entering default queue after `max_retries`.
- `--fail-on-errors` optionally returns non-zero if new failed states are produced.

## Operational Guidance

- Run `sustainability-rss-fetch insert-selected` first to keep `entries` focused and confirmed.
- Run this skill on a separate schedule:
  - high-velocity feeds: every 10-30 minutes
  - normal feeds: every 30-120 minutes
- Keep `--limit` bounded for predictable runtime per job.
