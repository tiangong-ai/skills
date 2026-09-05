---
name: open-meteo-historical-fetch
description: Retrieve bounded Open-Meteo historical weather reanalysis through the Tiangong CLI for up to ten known coordinates, one controlled model, and explicit hourly or daily variables. Use for gap-filled historical temperature, precipitation, humidity, wind, radiation, or shallow-soil context; do not use for station observations, forecasts, climate projections or attribution, geocoding, commercial public-endpoint use, or unreviewed cross-model trend conclusions.
---

# Open-Meteo Historical Weather

Use the CLI-owned `open-meteo.historical-weather` capability. This Skill
supplies intent routing and result-use boundaries only; the CLI owns source
discovery, input/output schemas, HTTP behavior, limits, validation, and
receipts.

## Before running

1. Read `references/tiangong-data-requirement.json`.
2. Use the caller- or workspace-resolved stable CLI. The requirement declares
   compatible capability and operation contract majors; it does not select a
   package build.
3. Run `data describe` with that same CLI. Continue only when the capability
   ID, required contract majors, and the
   `open-meteo.series-all-null` operation feature match; then copy the exact current
   capability/operation versions from that response into the run request.
4. Run the default static doctor. Do not add `--live` unless the user explicitly
   asks for a provider probe.

```bash
tiangong-ai data describe open-meteo.historical-weather --json
tiangong-ai data doctor open-meteo.historical-weather --json
```

Use the returned Discovery Metadata to confirm current model coverage,
resolution, update delay, variable availability, public-endpoint terms,
attribution, `provides`, and `doesNotProvide`. Do not substitute facts remembered
from an older Skill revision.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope and replace the two version
placeholders with the exact versions from the same `data describe` response. Coordinates are WGS84
decimal degrees. Both variable arrays are required; use an empty array for an
unneeded granularity. At least one array must be non-empty. The CLI fixes
timezone and units, preserves coordinate order, and normalizes variable order:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "open-meteo.historical-weather",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "fetch",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "locations": [
      { "latitude": 52.52, "longitude": 13.41 },
      { "latitude": 48.85, "longitude": 2.35 }
    ],
    "startDate": "2024-01-01",
    "endDate": "2024-01-07",
    "hourlyVariables": [
      "temperature_2m",
      "relative_humidity_2m",
      "precipitation",
      "soil_moisture_0_to_7cm"
    ],
    "dailyVariables": [
      "temperature_2m_max",
      "temperature_2m_min",
      "precipitation_sum",
      "et0_fao_evapotranspiration"
    ],
    "model": "era5",
    "cellSelection": "land"
  }
}
```

Use the operation input schema returned by the same `data describe` response for current variable
codes, model meanings, and limits. Select `era5` or `era5_land` for a
multi-decade analysis where one consistent model family matters. The default
`best_match` favors available local detail but can change model families over
time. Do not add an API key: this capability uses the public non-commercial
endpoint only. Commercial customer-endpoint access requires a separately
reviewed capability. Do not silently geocode names, adjust coordinates, widen
the date range, switch models, or add variables beyond the user's intent.

## Run

```bash
tiangong-ai data run open-meteo.historical-weather fetch \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat values as gap-filled reanalysis or model-grid estimates, not raw
  observations from a named station. Preserve requested and returned grid
  coordinates plus elevation.
- Retain nulls as unavailable model values. Never convert them to zero or
  interpolate them without an explicit downstream method.
- Distinguish `series-missing` from `series-all-null`: missing means the
  provider did not return a requested series, while all-null means it returned
  a structurally valid, aligned series with no usable numeric values. Preserve
  an all-null series and report the machine-readable issue code.
- Treat `partial` as incomplete coordinate, section, timestamp, variable, or
  unit coverage and report the affected paths with the usable series.
- In GMT mode, require exactly 24 strictly ascending hourly timestamps and one
  strictly ascending daily date per inclusive request date, with a zero
  provider UTC offset. Preserve count, ordering, timezone, unit, alignment, and
  non-numeric-value issues; a short axis is not complete coverage.
- Treat `blocked` as no usable business result and surface the structured
  errors instead of bypassing limits or switching endpoints.
- Report record-limit truncation. The CLI keeps every retained variable aligned
  to its returned time axis, but a truncated result is not exhaustive.
- Preserve the selected model in analysis and citations. Do not combine models
  or interpret a Best Match discontinuity as a weather or climate trend.
- Choose a station source when instrument provenance, local measurement
  quality, or regulatory-grade observations are required.
- Do not use this capability for forecasts, future climate scenarios, causal
  attribution, significance testing, or safety-critical weather decisions.
- The fixed public contract intentionally omits the source script's multi-model
  request, arbitrary timezone and units, optional API-key/customer access,
  endpoint overrides, and raw JSON/log artifacts. Expand those only through a
  separately reviewed CLI contract.
- Attribute Open-Meteo and the underlying data providers. The public endpoint
  is non-commercial; do not imply commercial-use permission.
- Cross-source comparison, statistical inference, and research evidence
  admission belong to the caller or Auto Research, not this atomic Skill.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
