---
name: gdelt-mentions-fetch
description: Fetch GDELT 2.0 Global Mentions export snapshots from lastupdate/masterfilelist with retry, throttling, transport validation, and structure validation. Use when tasks need latest or time-range mentions files (*.mentions.CSV.zip) for deterministic ingestion and machine-readable manifests.
---

# GDELT Mentions Fetch

## Core Goal
- Fetch GDELT 2.0 `Global Mentions` table exports (`*.mentions.CSV.zip`) from official public endpoints.
- Resolve latest available snapshot via `lastupdate.txt`.
- Resolve historical snapshots in a UTC range via `masterfilelist.txt`.
- Persist downloaded files and return machine-readable JSON manifest.
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
python3 scripts/gdelt_mentions_fetch.py check-config --pretty
```

2. Inspect the latest available mentions snapshot.

```bash
python3 scripts/gdelt_mentions_fetch.py resolve-latest --pretty
```

3. Dry-run a historical range selection before downloading.

```bash
python3 scripts/gdelt_mentions_fetch.py fetch \
  --mode range \
  --start-datetime 20260301000000 \
  --end-datetime 20260301120000 \
  --max-files 3 \
  --dry-run \
  --pretty
```

4. Fetch files with transport and structure validation.

```bash
python3 scripts/gdelt_mentions_fetch.py fetch \
  --mode latest \
  --max-files 1 \
  --output-dir ./data/gdelt-mentions \
  --preview-lines 2 \
  --validate-structure \
  --expected-columns 16 \
  --quarantine-dir ./data/gdelt-mentions-quarantine \
  --log-level INFO \
  --log-file ./logs/gdelt-mentions-fetch.log \
  --pretty
```

## Built-in Robustness
- Apply retry with exponential backoff on transient HTTP/network failures.
- Respect `Retry-After` when present on retriable responses.
- Throttle request frequency with a minimum interval between requests.
- Enforce `--max-files` safety cap (`GDELT_MAX_FILES_PER_RUN`) to prevent accidental bulk pulls.
- Validate datetime format and range boundaries before remote calls.
- Validate transport and structure after download:
  - ZIP CRC/integrity check
  - UTF-8 strict decoding check
  - Tab column-count check (default 16)
  - Optional bad-line issue quarantine (`--quarantine-dir`)
- Emit JSON results while writing operational logs to stderr and optional log file.

## Scope Decision
- Keep one concrete file-table fetch implementation: `Global Mentions` (`*.mentions.CSV.zip`).
- Keep atomic operations only; do not add internal scheduler/polling loops.

## References
- `references/gdelt-data-sources.md`
- `references/gdelt-limitations.md`
- `references/gdelt-schema.md`
- `references/env.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/gdelt_mentions_fetch.py`

## OpenClaw Invocation Compatibility
- Keep skill trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke in prompts with `$gdelt-mentions-fetch`.
- Keep the skill atomic: only resolve/fetch on demand.
- Use script parameters for fetch conditions (`--mode range --start-datetime --end-datetime`).
- If you need polling, let OpenClaw orchestrate repeated invocations externally, not inside this skill.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (latest availability)

```text
Use $gdelt-mentions-fetch.
Run:
python3 scripts/gdelt_mentions_fetch.py resolve-latest --pretty
Return only the JSON result.
```

2. Fetch (historical window, dry-run first)

```text
Use $gdelt-mentions-fetch.
Run:
python3 scripts/gdelt_mentions_fetch.py fetch \
  --mode range \
  --start-datetime [YYYYMMDDHHMMSS] \
  --end-datetime [YYYYMMDDHHMMSS] \
  --max-files [N] \
  --dry-run \
  --pretty

Then run without --dry-run using:
  --output-dir [OUTPUT_DIR]
  --validate-structure
  --expected-columns 16
  --quarantine-dir [QUARANTINE_DIR]
Return only the JSON result.
```

3. Validate (download quality gate)

```text
Use $gdelt-mentions-fetch.
Run:
python3 scripts/gdelt_mentions_fetch.py fetch \
  --mode latest \
  --max-files 1 \
  --output-dir [OUTPUT_DIR] \
  --validate-structure \
  --expected-columns 16 \
  --quarantine-dir [QUARANTINE_DIR] \
  --pretty
Check validation.issue_count, decode_error_count, column_mismatch_count.
Return JSON plus one-line pass/fail verdict.
```
