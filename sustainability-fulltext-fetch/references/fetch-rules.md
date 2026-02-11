# Fetch and Retry Rules

## Candidate Selection

Default `sync` selection:
- Include only `entries.is_relevant = 1`.
- Include rows that are API-eligible (real DOI) or webpage-eligible (`canonical_url`/`url` exists).
- Include rows with missing content state (`entry_content` absent).
- Include rows with `status=failed` only when:
  - retry is not exhausted (`retry_count < max_retries`, default `3`);
  - retry cooldown is reached (`next_retry_at` is NULL or <= now).

Optional selection controls:
- `--force`: include all rows (including ready).
- `--only-failed`: include only failed rows.
- `--refetch-days N`: include ready rows older than `N` days.
- `--oldest-first`: use historical oldest-first ordering.
- `--max-retries`: cap failed retries (`0` means unlimited).
- `--retry-backoff-minutes`: base minutes for exponential cooldown.

## Extraction Priority

1. API metadata by DOI (default):
- OpenAlex first.
- Semantic Scholar fallback.
- Accept text when `content_length >= --api-min-chars`.

2. Webpage fallback:
- `canonical_url` first, fallback `url`.
- Prefer `trafilatura` if installed.
- Fallback to built-in HTML parser.
- Accept text when `content_length >= --min-chars`.

## Failure Handling

- API/network/parsing/content-type errors are recorded per DOI.
- One failed DOI does not stop the batch.
- Failed retries use exponential backoff and stop entering default queue after `max_retries`.
- `--fail-on-errors` optionally returns non-zero if new failed states are produced.

## Operational Guidance

- Run `sustainability-rss-fetch insert-selected` first to keep relevant scope strict.
- Configure `OPENALEX_EMAIL` (and optional `S2_API_KEY`) for better API reliability.
- Keep `--limit` bounded for predictable runtime per job.
