---
name: youtube-comments-fetch
description: Fetch YouTube public comment threads and replies for discovered or provided video IDs in configurable UTC time windows with retries, throttling, transport checks, and structure validation. Use when tasks need YouTube public-language datasets after domain video discovery, especially as a complement to official-language sources such as GDELT or Regulations.gov.
---

# YouTube Comments Fetch

## Core Goal
- Fetch public YouTube comment threads with `commentThreads.list`.
- Optionally expand replies with `comments.list`.
- Filter comments by configurable UTC time windows using `published` or `updated` timestamps.
- Return machine-readable JSON and optionally save JSONL artifacts.
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
python3 scripts/youtube_comments_fetch.py check-config --pretty
```

2. Dry-run a fetch plan against candidate video IDs first.

```bash
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file ./data/youtube-videos/candidates.jsonl \
  --start-datetime 2026-03-01T00:00:00Z \
  --end-datetime 2026-03-08T00:00:00Z \
  --max-videos 5 \
  --max-thread-pages 3 \
  --dry-run \
  --pretty
```

3. Fetch comments and save JSONL artifacts.

```bash
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file ./data/youtube-videos/candidates.jsonl \
  --start-datetime 2026-03-01T00:00:00Z \
  --end-datetime 2026-03-08T00:00:00Z \
  --time-field published \
  --include-replies \
  --order time \
  --max-videos 10 \
  --max-thread-pages 10 \
  --max-reply-pages 20 \
  --max-comments 2000 \
  --output-dir ./data/youtube-comments \
  --log-level INFO \
  --log-file ./logs/youtube-comments-fetch.log \
  --pretty
```

## Built-in Robustness
- Retry transient failures with exponential backoff.
- Respect `Retry-After` and fail fast when it exceeds configured cap.
- Throttle request rate with a minimum request interval.
- Enforce safety caps:
  - max videos
  - max thread pages
  - max reply pages
  - max threads
  - max comments
- Validate transport:
  - JSON content-type
  - UTF-8 decode
  - JSON object parse
- Validate structure:
  - thread/comment response shape
  - comment IDs and parent linkage
  - datetime fields
  - duplicate comment suppression
- Preserve item-level failures and validation issues in JSON output and optional quarantine files.

## Scope Decision
- Keep one atomic operation: comment fetch for known/discovered video IDs.
- Accept upstream video IDs from CLI or from `$youtube-video-search` JSON/JSONL outputs.
- Do not perform video discovery in this skill.
- Do not embed scheduler/polling loops.

## References
- `references/env.md`
- `references/youtube-comments-api-notes.md`
- `references/youtube-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/youtube_comments_fetch.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke with `$youtube-comments-fetch`.
- Keep the skill atomic: each invocation consumes one configured ID set and time window.
- Prefer chaining from `$youtube-video-search` output via `--video-ids-file`.
- Surface `reply_window_completeness` to downstream agents when replies are requested.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (dry-run)

```text
Use $youtube-comments-fetch.
Run:
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file [VIDEO_IDS_FILE] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --max-videos [N] \
  --max-thread-pages [M] \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (windowed comments)

```text
Use $youtube-comments-fetch.
Run:
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file [VIDEO_IDS_FILE] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --time-field published \
  --include-replies \
  --order time \
  --max-videos [N] \
  --max-thread-pages [M] \
  --max-reply-pages [R] \
  --max-comments [K] \
  --output-dir [OUTPUT_DIR] \
  --pretty
Return only the JSON result.
```

3. Validate (quality gate)

```text
Use $youtube-comments-fetch.
Run:
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file [VIDEO_IDS_FILE] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --max-videos 1 \
  --max-thread-pages 1 \
  --max-reply-pages 1 \
  --max-comments 50 \
  --pretty
Check validation_summary.total_issue_count, failures, and fetch_summary.record_count.
Return JSON plus one-line pass/fail verdict.
```
