---
name: open-meteo-flood-fetch
description: Fetch Open-Meteo Flood API daily river-discharge data for specified date windows and coordinates with retries, throttling, detailed logs, and transport/structure validation. Use when tasks need deterministic flood or runoff verification for ecology and environmental monitoring, such as checking river discharge background, flood risk windows, or ensemble-style discharge signals for one or more coordinates.
---

# Open-Meteo Flood Fetch

## Core Goal
- Fetch Open-Meteo flood data for one or more coordinates in one invocation.
- Support inclusive `start_date` and `end_date` windows.
- Return machine-readable JSON with request metadata, transport info, validation summary, and normalized records.
- Keep runtime observable with structured logs and optional log file output.

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
python3 scripts/open_meteo_flood_fetch.py check-config --pretty
```

2. Dry-run the request plan first.

```bash
python3 scripts/open_meteo_flood_fetch.py fetch \
  --location 52.52,13.41 \
  --start-date 2026-03-01 \
  --end-date 2026-03-05 \
  --daily-var river_discharge \
  --timezone GMT \
  --dry-run \
  --pretty
```

3. Run the fetch with validation and operational logs.

```bash
python3 scripts/open_meteo_flood_fetch.py fetch \
  --location 52.52,13.41 \
  --location 48.85,2.35 \
  --start-date 2026-03-01 \
  --end-date 2026-03-05 \
  --daily-var river_discharge \
  --daily-var river_discharge_p75 \
  --ensemble \
  --cell-selection nearest \
  --timezone GMT \
  --output ./data/open-meteo/open-meteo-flood-fetch.json \
  --log-level INFO \
  --log-file ./logs/open-meteo-flood-fetch.log \
  --pretty
```

## Built-in Robustness
- Retry transient HTTP and network failures with exponential backoff.
- Respect `Retry-After` when present and fail fast when it exceeds configured cap.
- Throttle request rate with minimum request interval.
- Enforce safety caps before remote calls:
  - maximum locations
  - maximum day range
  - maximum daily variables
- Validate transport:
  - HTTP status handling
  - JSON content-type
  - UTF-8 strict decode
  - JSON parse
- Validate structure:
  - response object/list shape
  - `daily` and `daily_units` presence
  - time axis parseability and range checks
  - requested variable presence and aligned array lengths
  - optional ensemble member field alignment when `--ensemble` is enabled

## Scope Decision
- Keep one atomic fetch implementation for the Open-Meteo flood endpoint only.
- Do not embed alert thresholds, geocoding, polling loops, or flood interpretation logic.
- If recurring execution is needed, let OpenClaw orchestrate repeated calls externally.

## References
- `references/env.md`
- `references/open-meteo-flood-api-notes.md`
- `references/open-meteo-flood-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/open_meteo_flood_fetch.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke with `$open-meteo-flood-fetch`.
- Keep calls atomic and parameterized by:
  - `--location`
  - `--start-date`
  - `--end-date`
  - `--daily-var`
  - `--ensemble`
  - `--cell-selection`
- Use OpenClaw orchestration, not this script, for recurring jobs.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (dry-run)

```text
Use $open-meteo-flood-fetch.
Run:
python3 scripts/open_meteo_flood_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --daily-var river_discharge \
  --timezone GMT \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (flood verification window)

```text
Use $open-meteo-flood-fetch.
Run:
python3 scripts/open_meteo_flood_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --daily-var river_discharge \
  --daily-var river_discharge_p75 \
  --ensemble \
  --cell-selection nearest \
  --timezone GMT \
  --pretty
Return only the JSON result.
```

3. Validate (quality gate)

```text
Use $open-meteo-flood-fetch.
Run:
python3 scripts/open_meteo_flood_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --daily-var river_discharge \
  --timezone GMT \
  --pretty
Check validation_summary.total_issue_count and validation_summary.ok.
Return JSON plus one-line pass/fail verdict.
```
