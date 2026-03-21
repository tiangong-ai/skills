---
name: youtube-video-search
description: Search YouTube videos for topical/domain discovery using explicit query strings, channel filters, publish-time windows, retries, throttling, enrichment, and validation. Use when tasks need candidate YouTube videos for a domain such as environment/public-opinion research before downstream comment collection or when OpenClaw must decide search keywords itself from a broad task objective.
---

# YouTube Video Search

## Core Goal
- Search public YouTube videos with `search.list`.
- Support domain discovery from flexible query strings and optional channel/time filters.
- Enrich discovered videos with `videos.list` metadata and statistics.
- Return machine-readable JSON and optionally save JSONL candidate artifacts.
- Keep execution observable with structured logs and optional log file.

## Required Environment
- Configure runtime via environment variables (see `references/env.md`).
- Start from `assets/config.example.env` and keep real secrets in `assets/config.env`.
- Load env values before running commands:

```bash
set -a
source assets/config.env
set +a
```

## Workflow
1. Validate effective configuration.

```bash
python3 scripts/youtube_video_search.py check-config --pretty
```

2. Dry-run the search plan first.

```bash
python3 scripts/youtube_video_search.py search \
  --query "climate change pollution" \
  --published-after 2026-03-01 \
  --published-before 2026-03-08 \
  --order date \
  --max-pages 2 \
  --max-results 50 \
  --dry-run \
  --pretty
```

3. Run a domain search and save candidate videos for downstream comment fetch.

```bash
python3 scripts/youtube_video_search.py search \
  --query "climate change pollution" \
  --published-after 2026-03-01 \
  --published-before 2026-03-08 \
  --order date \
  --max-pages 4 \
  --max-results 120 \
  --comment-count-min 20 \
  --output-dir ./data/youtube-videos \
  --log-level INFO \
  --log-file ./logs/youtube-video-search.log \
  --pretty
```

## Built-in Robustness
- Retry transient failures with exponential backoff.
- Respect `Retry-After` and fail fast when it exceeds configured cap.
- Throttle request rate with a minimum request interval.
- Enforce run safety caps:
  - max pages
  - max results
  - max enriched videos
- Validate transport:
  - JSON content-type
  - UTF-8 decode
  - JSON object parse
- Validate structure:
  - search response shape
  - `videoId` presence and datetime fields
  - detail batch completeness and duplicate IDs
- Emit JSON results while writing operational logs to stderr and optional log file.

## Scope Decision
- Keep one atomic operation: public video discovery plus optional detail enrichment.
- Let OpenClaw decide query strategy and iterate by repeated invocations.
- Do not embed scheduler/polling loops or comment fetching in this skill.

## References
- `references/env.md`
- `references/youtube-search-api-notes.md`
- `references/youtube-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/youtube_video_search.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke with `$youtube-video-search`.
- Keep the skill atomic: one query execution per invocation.
- Use script parameters for retrieval conditions (`--query`, publish window, channel filters, caps).
- Feed its JSONL output directly into `$youtube-comments-fetch` via `--video-ids-file`.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (query plan check)

```text
Use $youtube-video-search.
Run:
python3 scripts/youtube_video_search.py search \
  --query "[QUERY_STRING]" \
  --published-after [YYYY-MM-DDTHH:MM:SSZ] \
  --published-before [YYYY-MM-DDTHH:MM:SSZ] \
  --order date \
  --max-pages [N] \
  --max-results [M] \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Search (candidate videos)

```text
Use $youtube-video-search.
Run:
python3 scripts/youtube_video_search.py search \
  --query "[QUERY_STRING]" \
  --published-after [YYYY-MM-DDTHH:MM:SSZ] \
  --published-before [YYYY-MM-DDTHH:MM:SSZ] \
  --order date \
  --max-pages [N] \
  --max-results [M] \
  --comment-count-min [K] \
  --output-dir [OUTPUT_DIR] \
  --pretty
Return only the JSON result.
```

3. Chain (prepare downstream comment fetch)

```text
Use $youtube-video-search.
Run:
python3 scripts/youtube_video_search.py search \
  --query "[QUERY_STRING]" \
  --published-after [YYYY-MM-DDTHH:MM:SSZ] \
  --published-before [YYYY-MM-DDTHH:MM:SSZ] \
  --order date \
  --max-pages [N] \
  --max-results [M] \
  --output-dir [OUTPUT_DIR] \
  --pretty
Take artifacts.output_jsonl and pass it to $youtube-comments-fetch as --video-ids-file.
Return JSON plus the artifact path.
```
