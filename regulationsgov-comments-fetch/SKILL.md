---
name: regulationsgov-comments-fetch
description: Fetch Regulations.gov v4 comments in configurable time windows with authenticated requests, pagination, retries, throttling, transport checks, and response-structure validation. Use when tasks need policy/public-opinion comment datasets for downstream analysis (including sentiment workflows) with deterministic JSON/JSONL outputs.
---

# Regulations.gov Comments Fetch

## Core Goal
- Fetch `comments` data from `https://api.regulations.gov/v4/comments`.
- Filter by configurable time windows (`lastModifiedDate` or `postedDate`).
- Apply optional filters (`agencyId`, `commentOnId`, `searchTerm`).
- Return machine-readable JSON and optionally save JSONL records locally.
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
python3 scripts/regulationsgov_comments_fetch.py check-config --pretty
```

2. Dry-run to verify query parameters without making remote calls.

```bash
python3 scripts/regulationsgov_comments_fetch.py fetch \
  --filter-mode last-modified \
  --start-datetime 2026-03-10T00:00:00Z \
  --end-datetime 2026-03-10T23:59:59Z \
  --max-pages 2 \
  --dry-run \
  --pretty
```

3. Fetch comments with structure validation and logs.

```bash
python3 scripts/regulationsgov_comments_fetch.py fetch \
  --filter-mode last-modified \
  --start-datetime 2026-03-10T00:00:00Z \
  --end-datetime 2026-03-10T23:59:59Z \
  --agency-id EPA \
  --max-pages 3 \
  --max-records 300 \
  --output-dir ./data/regulationsgov-comments \
  --quarantine-dir ./data/regulationsgov-comments-quarantine \
  --log-level INFO \
  --log-file ./logs/regulationsgov-comments-fetch.log \
  --pretty
```

## Built-in Robustness
- Retry transient failures (`429/500/502/503/504`) with exponential backoff.
- Respect `Retry-After` on rate-limit responses and stop if it exceeds configured cap.
- Throttle request frequency with a minimum interval between requests.
- Enforce safety caps:
  - `--max-pages` limited by `REGGOV_MAX_PAGES_PER_RUN`
  - `--max-records` limited by `REGGOV_MAX_RECORDS_PER_RUN`
- Validate transport and payload:
  - HTTP/content-type checks
  - UTF-8 + JSON parse checks
  - JSON:API-style `data/meta` structure checks
  - comment item field checks (`id`, `type`, datetime field formats)
  - optional issue quarantine (`--quarantine-dir`)

## Scope Decision
- Keep one atomic endpoint implementation: `comments` list fetch.
- Keep operations request-driven; do not add built-in scheduler/polling loops.
- Preserve simple downstream integration by returning JSON and optional JSONL artifact.

## References
- `references/env.md`
- `references/regulationsgov-api-notes.md`
- `references/regulationsgov-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/regulationsgov_comments_fetch.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke in prompts with `$regulationsgov-comments-fetch`.
- Keep the skill atomic: each invocation handles one configured window.
- Use script parameters for fetch conditions (`--filter-mode`, time window, page caps).
- If periodic polling is needed, let OpenClaw orchestrate repeated invocations externally.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (query plan check)

```text
Use $regulationsgov-comments-fetch.
Run:
python3 scripts/regulationsgov_comments_fetch.py fetch \
  --filter-mode last-modified \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --max-pages [N] \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (time-window comments)

```text
Use $regulationsgov-comments-fetch.
Run:
python3 scripts/regulationsgov_comments_fetch.py fetch \
  --filter-mode last-modified \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --agency-id [OPTIONAL_AGENCY] \
  --max-pages [N] \
  --max-records [M] \
  --output-dir [OUTPUT_DIR] \
  --quarantine-dir [QUARANTINE_DIR] \
  --pretty
Return only the JSON result.
```

3. Validate (quality gate)

```text
Use $regulationsgov-comments-fetch.
Run:
python3 scripts/regulationsgov_comments_fetch.py fetch \
  --filter-mode posted \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --max-pages 1 \
  --max-records 50 \
  --pretty
Check validation_summary.total_issue_count and stop_reason.
Return JSON plus one-line pass/fail verdict.
```
