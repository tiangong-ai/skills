---
name: open-meteo-air-quality-fetch
description: Fetch Open-Meteo Air Quality API hourly background fields for specified date windows and coordinates with retries, throttling, detailed logs, and transport/structure validation. Use when tasks need deterministic modeled air-quality context for ecology or environmental verification, such as checking PM, gases, dust, UV, pollen, or AQI background conditions beyond station coverage.
---

# Open-Meteo Air Quality Fetch

## Core Goal
- Fetch Open-Meteo hourly air-quality background data for one or more coordinates in one invocation.
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
python3 scripts/open_meteo_air_quality_fetch.py check-config --pretty
```

2. Dry-run the request plan first.

```bash
python3 scripts/open_meteo_air_quality_fetch.py fetch \
  --location 52.52,13.41 \
  --start-date 2026-03-17 \
  --end-date 2026-03-18 \
  --hourly-var pm2_5 \
  --hourly-var pm10 \
  --timezone GMT \
  --dry-run \
  --pretty
```

3. Run the fetch with validation and operational logs.

```bash
python3 scripts/open_meteo_air_quality_fetch.py fetch \
  --location 52.52,13.41 \
  --location 48.85,2.35 \
  --start-date 2026-03-17 \
  --end-date 2026-03-18 \
  --hourly-var pm2_5 \
  --hourly-var pm10 \
  --hourly-var nitrogen_dioxide \
  --hourly-var ozone \
  --hourly-var us_aqi \
  --domain auto \
  --cell-selection nearest \
  --timezone GMT \
  --output ./data/open-meteo/open-meteo-air-quality-fetch.json \
  --log-level INFO \
  --log-file ./logs/open-meteo-air-quality-fetch.log \
  --pretty
```

## Built-in Robustness
- Retry transient HTTP and network failures with exponential backoff.
- Respect `Retry-After` when present and fail fast when it exceeds configured cap.
- Throttle request rate with minimum request interval.
- Enforce safety caps before remote calls:
  - maximum locations
  - maximum day range
  - maximum hourly variables
- Validate transport:
  - HTTP status handling
  - JSON content-type
  - UTF-8 strict decode
  - JSON parse
- Validate structure:
  - response object/list shape
  - requested `hourly` section presence
  - requested variable presence and aligned array lengths
  - time axis parseability and range checks

## Scope Decision
- Keep one atomic fetch implementation for the Open-Meteo air-quality endpoint only.
- Do not embed geocoding, station merging, alert thresholds, or AQI interpretation logic.
- Use OpenClaw orchestration, not this script, for recurring jobs or multi-step fusion with OpenAQ.

## References
- `references/env.md`
- `references/open-meteo-air-quality-api-notes.md`
- `references/open-meteo-air-quality-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/open_meteo_air_quality_fetch.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke with `$open-meteo-air-quality-fetch`.
- Keep calls atomic and parameterized by:
  - `--location`
  - `--start-date`
  - `--end-date`
  - `--hourly-var`
  - `--domain`
  - `--cell-selection`
- Use OpenClaw orchestration, not this script, for recurring jobs.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (dry-run)

```text
Use $open-meteo-air-quality-fetch.
Run:
python3 scripts/open_meteo_air_quality_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var [HOURLY_VARIABLE] \
  --timezone GMT \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (air-quality background window)

```text
Use $open-meteo-air-quality-fetch.
Run:
python3 scripts/open_meteo_air_quality_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var pm2_5 \
  --hourly-var pm10 \
  --hourly-var nitrogen_dioxide \
  --hourly-var ozone \
  --hourly-var us_aqi \
  --domain auto \
  --cell-selection nearest \
  --timezone GMT \
  --pretty
Return only the JSON result.
```

3. Validate (quality gate)

```text
Use $open-meteo-air-quality-fetch.
Run:
python3 scripts/open_meteo_air_quality_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var pm2_5 \
  --hourly-var pm10 \
  --hourly-var us_aqi \
  --timezone GMT \
  --pretty
Check validation_summary.total_issue_count and validation_summary.ok.
Return JSON plus one-line pass/fail verdict.
```
