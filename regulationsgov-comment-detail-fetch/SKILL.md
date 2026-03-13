---
name: regulationsgov-comment-detail-fetch
description: Fetch Regulations.gov v4 comment detail resources by comment IDs with authenticated requests, retries, throttling, transport checks, and structure validation. Use when tasks need enriched comment payloads (full comment text, docket/document linkage, and optional attachment relationships) after a comments list fetch.
---

# Regulations.gov Comment Detail Fetch

## Core Goal
- Fetch detailed comment resources from `GET /comments/{commentId}`.
- Support ID inputs from CLI and files (txt/json/jsonl).
- Optionally request `include=attachments`.
- Return machine-readable JSON and optionally save JSONL artifacts.
- Keep execution observable with structured logs and optional log file.

## Required Environment
- Configure runtime with environment variables (see `references/env.md`).
- Start from `assets/config.example.env`.
- Load env values before running commands:

```bash
set -a
source assets/config.example.env
set +a
```

## Workflow
1. Validate effective configuration.

```bash
python3 scripts/regulationsgov_comment_detail_fetch.py check-config --pretty
```

2. Dry-run input parsing and request plan.

```bash
python3 scripts/regulationsgov_comment_detail_fetch.py fetch \
  --comment-id FS-2026-0001-1963 \
  --comment-id FS-2026-0001-1964 \
  --include attachments \
  --dry-run \
  --pretty
```

3. Fetch details from a prior comments JSONL output.

```bash
python3 scripts/regulationsgov_comment_detail_fetch.py fetch \
  --comment-ids-file ./data/regulationsgov-comments/comments-window.jsonl \
  --max-comments 100 \
  --include attachments \
  --output-dir ./data/regulationsgov-comment-details \
  --quarantine-dir ./data/regulationsgov-comment-details-quarantine \
  --no-fail-on-item-error \
  --log-level INFO \
  --log-file ./logs/reggov-comment-detail-fetch.log \
  --pretty
```

## Built-in Robustness
- Retry transient failures (`429/500/502/503/504`) with exponential backoff.
- Respect `Retry-After` and fail fast if value exceeds configured cap.
- Throttle request interval between detail requests.
- Enforce hard safety cap on IDs per run (`REGGOV_MAX_COMMENT_IDS_PER_RUN`).
- Validate transport and structure:
  - status/content-type/UTF-8/JSON checks
  - detail resource shape checks (`data.id`, `data.type`, `attributes`)
  - datetime format checks for key timestamp fields
- Optional issue quarantine (`--quarantine-dir`).
- Optional partial-failure mode (`--no-fail-on-item-error`) for batch resilience.

## Scope Decision
- Keep one atomic operation: detail fetch by comment ID list.
- Do not include comments list discovery logic in this skill.
- Do not include internal scheduling/polling loops.

## References
- `references/env.md`
- `references/regulationsgov-detail-api-notes.md`
- `references/regulationsgov-detail-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/regulationsgov_comment_detail_fetch.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke with `$regulationsgov-comment-detail-fetch`.
- Keep skill atomic: one invocation consumes one ID set.
- Upstream orchestration can provide IDs from `$regulationsgov-comments-fetch` output.

## OpenClaw Prompt Templates

1. Recon (dry-run)

```text
Use $regulationsgov-comment-detail-fetch.
Run:
python3 scripts/regulationsgov_comment_detail_fetch.py fetch \
  --comment-ids-file [COMMENTS_JSONL] \
  --max-comments [N] \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (detail enrichment)

```text
Use $regulationsgov-comment-detail-fetch.
Run:
python3 scripts/regulationsgov_comment_detail_fetch.py fetch \
  --comment-ids-file [COMMENTS_JSONL] \
  --max-comments [N] \
  --include attachments \
  --output-dir [OUTPUT_DIR] \
  --quarantine-dir [QUARANTINE_DIR] \
  --no-fail-on-item-error \
  --pretty
Return only the JSON result.
```

3. Validate (strict mode)

```text
Use $regulationsgov-comment-detail-fetch.
Run:
python3 scripts/regulationsgov_comment_detail_fetch.py fetch \
  --comment-id [COMMENT_ID] \
  --fail-on-item-error \
  --fail-on-validation-error \
  --pretty
Check failure_count and validation_issue_count.
Return JSON plus one-line pass/fail verdict.
```
