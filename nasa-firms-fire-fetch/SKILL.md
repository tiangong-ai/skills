---
name: nasa-firms-fire-fetch
description: Fetch NASA FIRMS active fire detections from the area/csv endpoint for specified date windows, sources, and bounding boxes with retries, throttling, detailed logs, and transport/structure validation. Use when tasks need deterministic wildfire, open-burning, or smoke-source verification for ecology and environmental monitoring within a known coordinate window.
---

# NASA FIRMS Fire Fetch

## Core Goal
- Fetch NASA FIRMS active fire detections for one bounding box in one invocation.
- Support inclusive `--start-date` and `--end-date` even though FIRMS only accepts up to 5 days per request.
- Return machine-readable JSON with request metadata, chunk transport details, validation summary, and normalized fire records.
- Keep runtime observable with structured stderr logs and optional file logs.

## Required Environment
- `NASA_FIRMS_MAP_KEY` is required for remote requests.
- Start from `assets/config.example.env` and keep the real key in `assets/config.env`.
- Load env values before running commands:

```bash
set -a
source assets/config.env
set +a
```

## Workflow
1. Validate effective configuration. Probe the key when needed.

```bash
python3 scripts/nasa_firms_fire_fetch.py check-config \
  --probe-map-key \
  --pretty
```

2. Dry-run the request plan first.

```bash
python3 scripts/nasa_firms_fire_fetch.py fetch \
  --source VIIRS_NOAA20_NRT \
  --bbox 115.8,-8.9,116.3,-8.3 \
  --start-date 2026-03-01 \
  --end-date 2026-03-08 \
  --dry-run \
  --pretty
```

3. Run the fetch with validation and operational logs.

```bash
python3 scripts/nasa_firms_fire_fetch.py fetch \
  --source VIIRS_NOAA20_NRT \
  --bbox 115.8,-8.9,116.3,-8.3 \
  --start-date 2026-03-01 \
  --end-date 2026-03-08 \
  --check-availability \
  --output ./data/firms/nasa-firms-fire-fetch.json \
  --log-level INFO \
  --log-file ./logs/nasa-firms-fire-fetch.log \
  --pretty
```

## Built-in Robustness
- Retry transient HTTP and network failures with exponential backoff.
- Respect `Retry-After` when present and fail fast when it exceeds the configured cap.
- Throttle request rate with a minimum request interval.
- Enforce safety caps before remote calls:
  - maximum inclusive day range per run
  - maximum per-chunk day range
  - maximum estimated transaction weight per run
- Validate transport:
  - HTTP status handling
  - content-type checks
  - UTF-8 strict decode
  - CSV and JSON parse checks
- Validate structure:
  - required fire columns
  - coordinate range and bbox inclusion
  - acquisition date/time parseability
  - returned dates remaining inside the requested window
  - consistent chunk headers

## Scope Decision
- Keep one atomic fetch implementation for the FIRMS `area/csv` endpoint only.
- Do not embed geocoding, world-scale scans, country queries, alert thresholds, or fire classification logic.
- Use OpenClaw orchestration, not this script, for recurring jobs or multi-area fan-out.

## References
- `references/env.md`
- `references/nasa-firms-api-notes.md`
- `references/nasa-firms-limitations.md`
- `references/openclaw-chaining-templates.md`

## Script
- `scripts/nasa_firms_fire_fetch.py`

## OpenClaw Invocation Compatibility
- Keep trigger metadata in `name`, `description`, and `agents/openai.yaml`.
- Invoke with `$nasa-firms-fire-fetch`.
- Keep calls atomic and parameterized by:
  - `--source`
  - `--bbox`
  - `--start-date`
  - `--end-date`
- Use OpenClaw orchestration, not this script, for repeated areas or repeated sources.

## OpenClaw Prompt Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (dry-run)

```text
Use $nasa-firms-fire-fetch.
Run:
python3 scripts/nasa_firms_fire_fetch.py fetch \
  --source [FIRMS_SOURCE] \
  --bbox [WEST,SOUTH,EAST,NORTH] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (fire verification window)

```text
Use $nasa-firms-fire-fetch.
Run:
python3 scripts/nasa_firms_fire_fetch.py fetch \
  --source [FIRMS_SOURCE] \
  --bbox [WEST,SOUTH,EAST,NORTH] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --check-availability \
  --pretty
Return only the JSON result.
```

3. Validate (quality gate)

```text
Use $nasa-firms-fire-fetch.
Run:
python3 scripts/nasa_firms_fire_fetch.py fetch \
  --source [FIRMS_SOURCE] \
  --bbox [WEST,SOUTH,EAST,NORTH] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --pretty
Check validation_summary.total_issue_count and validation_summary.ok.
Return JSON plus one-line pass/fail verdict.
```
