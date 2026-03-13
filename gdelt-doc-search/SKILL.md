---
name: gdelt-doc-search
description: Query GDELT DOC 2.0 API using explicit query/mode/format/time parameters with retry, throttling, and detailed logs. Use when tasks need topical/domain retrieval, article lists, or timeline aggregates without pulling raw events files.
---

# GDELT DOC Search

## Core Goal
- Execute atomic DOC API searches against `https://api.gdeltproject.org/api/v2/doc/doc`.
- Support topic/domain retrieval via explicit query syntax.
- Support both relative windows (`timespan`) and absolute UTC windows (`STARTDATETIME`, `ENDDATETIME`).
- Return structured JSON envelopes and optionally write raw response bytes.
- Keep runtime observable with structured logs and optional log file.

## Required Environment
- Configure runtime by environment variables (see `references/env.md`).
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
python3 scripts/gdelt_doc_search.py check-config --pretty
```

2. Run a relative-window DOC search.

```bash
python3 scripts/gdelt_doc_search.py search \
  --query '("climate change" OR pollution)' \
  --mode artlist \
  --format json \
  --timespan 1day \
  --max-records 50 \
  --pretty
```

3. Run an absolute-window timeline query.

```bash
python3 scripts/gdelt_doc_search.py search \
  --query '("climate change" OR pollution) sourcecountry:us' \
  --mode timelinevolraw \
  --format json \
  --start-datetime 20260301000000 \
  --end-datetime 20260308000000 \
  --timeline-smooth 5 \
  --pretty
```

4. Persist raw API payload to a file for downstream tools.

```bash
python3 scripts/gdelt_doc_search.py search \
  --query '(wildfire OR drought)' \
  --mode artlist \
  --format json \
  --timespan 1week \
  --output ./data/gdelt-doc/wildfire.json \
  --pretty
```

## Built-in Robustness
- Apply retry with exponential backoff on transient HTTP/network failures.
- Respect `Retry-After` when present on retriable responses.
- Throttle request frequency with a minimum interval between requests.
- Validate query/time parameter combinations before remote calls.
- Validate DOC constraints (`MAXRECORDS<=250`, `TIMELINESMOOTH<=30`).
- Emit JSON results while writing operational logs to stderr and optional log file.

## Scope Decision
- Keep only DOC API retrieval in this skill.
- Keep atomic operations only; do not add internal scheduler/polling loops.

## References
- `references/env.md`
- `references/gdelt-data-sources.md`
- `references/gdelt-doc-search.md`
- `references/gdelt-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/gdelt_doc_search.py`

## OpenClaw Invocation Compatibility
- Keep skill trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke in prompts with `$gdelt-doc-search`.
- Keep the skill atomic: one query execution per invocation.
- Use script parameters for retrieval conditions (`--query`, `--mode`, `--format`, `--timespan` or `--start-datetime/--end-datetime`).
- If you need polling, let OpenClaw agent orchestrate repeated invocations externally (scheduler/loop), not inside this skill.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (config and endpoint check)

```text
Use $gdelt-doc-search.
Run:
python3 scripts/gdelt_doc_search.py check-config --pretty
Return only the JSON result.
```

2. Search (relative window)

```text
Use $gdelt-doc-search.
Run:
python3 scripts/gdelt_doc_search.py search \
  --query '[QUERY_EXPRESSION]' \
  --mode [MODE] \
  --format json \
  --timespan [TIMESPAN] \
  --max-records [N] \
  --pretty
Return only the JSON result.
```

3. Validate (absolute window and output persistence)

```text
Use $gdelt-doc-search.
Run:
python3 scripts/gdelt_doc_search.py search \
  --query '[QUERY_EXPRESSION]' \
  --mode [MODE] \
  --format json \
  --start-datetime [YYYYMMDDHHMMSS] \
  --end-datetime [YYYYMMDDHHMMSS] \
  --output [OUTPUT_FILE] \
  --pretty
Check command exit code and bytes_written > 0.
Return JSON plus one-line pass/fail verdict.
```
