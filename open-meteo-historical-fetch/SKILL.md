---
name: open-meteo-historical-fetch
description: Fetch Open-Meteo Historical Weather API time-window data for weather and shallow-soil variables with retries, throttling, detailed logs, and transport/structure validation. Use when tasks need deterministic physical-world context for ecology or environmental verification, such as checking historical temperature, precipitation, wind, humidity, evapotranspiration, soil temperature, or soil moisture for one or more coordinates in a specified date range.
---

# Open-Meteo Historical Fetch

## Core Goal
- Fetch historical Open-Meteo archive data for one or more coordinates in one invocation.
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
python3 scripts/open_meteo_historical_fetch.py check-config --pretty
```

2. Dry-run the request plan first.

```bash
python3 scripts/open_meteo_historical_fetch.py fetch \
  --location 52.52,13.41 \
  --start-date 2026-03-01 \
  --end-date 2026-03-02 \
  --hourly-var temperature_2m \
  --hourly-var soil_moisture_0_to_7cm \
  --timezone GMT \
  --dry-run \
  --pretty
```

3. Run the fetch with validation and operational logs.

```bash
python3 scripts/open_meteo_historical_fetch.py fetch \
  --location 52.52,13.41 \
  --location 48.85,2.35 \
  --start-date 2026-03-01 \
  --end-date 2026-03-02 \
  --hourly-var temperature_2m \
  --hourly-var relative_humidity_2m \
  --hourly-var wind_speed_10m \
  --hourly-var soil_temperature_0cm \
  --hourly-var soil_moisture_0_to_7cm \
  --daily-var precipitation_sum \
  --daily-var evapotranspiration \
  --model era5 \
  --timezone GMT \
  --output ./data/open-meteo/open-meteo-fetch.json \
  --log-level INFO \
  --log-file ./logs/open-meteo-historical-fetch.log \
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
  - maximum daily variables
- Validate transport:
  - HTTP status handling
  - JSON content-type
  - UTF-8 strict decode
  - JSON parse
- Validate structure:
  - response object/list shape
  - requested section presence (`hourly`, `daily`)
  - time axis parseability and range checks
  - requested variable presence and aligned array lengths

## Scope Decision
- Keep one atomic fetch implementation for the Open-Meteo historical archive endpoint only.
- Do not embed geocoding, polling loops, scheduler logic, or flood API handling.
- If recurring execution is needed, let OpenClaw orchestrate repeated calls externally.

## References
- `references/env.md`
- `references/open-meteo-api-notes.md`
- `references/open-meteo-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/open_meteo_historical_fetch.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke with `$open-meteo-historical-fetch`.
- Keep calls atomic and parameterized by:
  - `--location`
  - `--start-date`
  - `--end-date`
  - `--hourly-var`
  - `--daily-var`
  - `--model`
- Use OpenClaw orchestration, not this script, for recurring jobs.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (dry-run)

```text
Use $open-meteo-historical-fetch.
Run:
python3 scripts/open_meteo_historical_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var [HOURLY_VARIABLE] \
  --timezone GMT \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (ecology verification window)

```text
Use $open-meteo-historical-fetch.
Run:
python3 scripts/open_meteo_historical_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var temperature_2m \
  --hourly-var relative_humidity_2m \
  --hourly-var precipitation \
  --hourly-var wind_speed_10m \
  --hourly-var soil_moisture_0_to_7cm \
  --daily-var precipitation_sum \
  --daily-var evapotranspiration \
  --timezone GMT \
  --pretty
Return only the JSON result.
```

3. Validate (quality gate)

```text
Use $open-meteo-historical-fetch.
Run:
python3 scripts/open_meteo_historical_fetch.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var temperature_2m \
  --hourly-var soil_moisture_0_to_7cm \
  --daily-var precipitation_sum \
  --timezone GMT \
  --pretty
Check validation_summary.total_issue_count and validation_summary.ok.
Return JSON plus one-line pass/fail verdict.
```
