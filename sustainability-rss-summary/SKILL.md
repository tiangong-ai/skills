---
name: sustainability-rss-summary
description: Run a zero-scraper sustainability intelligence pipeline that uses official RSS DOI events plus open APIs (OpenAlex primary, Semantic Scholar fallback) to fetch paper abstracts with delayed retries. Use when building, operating, or troubleshooting DOI-based abstract fetching and queue orchestration before LLM summarization.
---

# Sustainability RSS Summary

## Core Goal
- Fetch abstracts without webpage crawling.
- Use DOI as the only lookup key.
- Handle indexing lag with delayed retries.
- Output ready abstracts for downstream LLM summarization.

## Triggering Conditions
- Receive a request to build or run an RSS-to-summary pipeline for sustainability/science papers.
- Receive RSS items with DOI/title/link and need legal, low-maintenance abstract retrieval.
- Need OpenAlex-first, Semantic Scholar-fallback orchestration.
- Need retry handling for papers that are too new to be indexed immediately.

## Workflow
1. Ingest DOI events from official RSS feeds.
- Keep at least `doi`; include `title`, `link`, and `source_feed` when available.
- Use `assets/rss-events.example.jsonl` as input format reference.

2. Add events into the pending queue.
- Run `scripts/abstract_pipeline.py queue-add` to upsert DOI tasks into SQLite.
- Preserve existing `ready` records; reset non-ready records to retryable `new`.

3. Fetch abstracts via dual-tower APIs.
- Primary: OpenAlex (`abstract_inverted_index` reconstruction).
- Fallback: Semantic Scholar (`abstract` text field).
- Use `fetch` for immediate single DOI checks and debugging.

4. Retry delayed-index papers.
- Use `queue-run` on a schedule (for example daily).
- If both APIs miss, increment `retry_count`, set `next_retry_at`, and keep status as `new`.
- Mark record `failed` when `retry_count >= max_retries`.

5. Hand off ready abstracts to LLM.
- Pull rows with `status=ready` via `queue-list`.
- Send `title + abstract_text + metadata` to summarization workflow.

## Commands

### Single DOI fetch
```bash
python3 scripts/abstract_pipeline.py fetch \
  --doi "10.1177/014920639101700108" \
  --openalex-email "you@example.com" \
  --pretty
```

### Queue ingest from JSONL
```bash
python3 scripts/abstract_pipeline.py queue-add \
  --db sustainability-rss-summary.db \
  --jsonl assets/rss-events.example.jsonl \
  --max-retries 3
```

### Queue run (with retries)
```bash
python3 scripts/abstract_pipeline.py queue-run \
  --db sustainability-rss-summary.db \
  --backoff-hours "24,24,48"
```

Set credentials via environment variables (recommended):

```bash
export OPENALEX_EMAIL="you@example.com"
export S2_API_KEY="optional-semantic-scholar-key"
```

Manual emergency rerun (ignore `next_retry_at`):

```bash
python3 scripts/abstract_pipeline.py queue-run \
  --db sustainability-rss-summary.db \
  --force
```

### Inspect queue state
```bash
python3 scripts/abstract_pipeline.py queue-list \
  --db sustainability-rss-summary.db \
  --pretty
```

## Queue Status Model
- `new`: waiting for first attempt or delayed retry.
- `ready`: abstract available (`abstract_source`, `abstract_text` set).
- `failed`: exhausted retry budget; manual follow-up needed.

## Operational Rules
- Prefer OpenAlex first for cost and openness; use Semantic Scholar only as fallback.
- Never scrape publisher webpages for abstract extraction in this skill.
- Persist every miss into queue; do not drop DOI tasks silently.
- Always carry exact DOI in output metadata for traceability.
- Use `--openalex-email` (or `OPENALEX_EMAIL`) for polite and faster OpenAlex routing.
- Treat API throttling/network errors as transient retries, not permanent failures.
- Use `--force` only for manual backfill or debugging; keep scheduled jobs in normal due-mode.

## References
- `references/architecture.md`
- `references/testing.md`

## Assets
- `assets/rss-events.example.jsonl`
