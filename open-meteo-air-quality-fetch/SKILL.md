---
name: open-meteo-air-quality-fetch
description: Retrieve hourly modeled Open-Meteo air-quality, aerosol, pollen, UV, or AQI fields through the Tiangong CLI for up to ten known coordinates and one bounded date window. Use for spatially continuous CAMS model context beyond station coverage; do not use for monitoring-station observations, regulatory records, health or exposure advice, geocoding, commercial public-endpoint use, or causal interpretation.
---

# Open-Meteo Air Quality

Use the CLI-owned `open-meteo.air-quality` capability. This Skill supplies
intent routing and result-use boundaries only; the CLI owns source discovery,
input/output schemas, HTTP behavior, limits, validation, and receipts.

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
tiangong-ai data describe open-meteo.air-quality --json
tiangong-ai data doctor open-meteo.air-quality --json
```

Use the returned Discovery Metadata to confirm current model coverage,
variable availability, freshness, public-endpoint terms, attribution,
`provides`, and `doesNotProvide`. Do not substitute facts remembered from an
older Skill revision.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope and replace the two version
placeholders with the exact versions from the same `data describe` response. Coordinates are WGS84
decimal degrees. The CLI fixes timestamps to GMT, preserves coordinate order,
and normalizes the variable order:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "open-meteo.air-quality",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "fetch-hourly",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "locations": [
      { "latitude": 52.52, "longitude": 13.41 },
      { "latitude": 48.85, "longitude": 2.35 }
    ],
    "startDate": "2026-03-17",
    "endDate": "2026-03-18",
    "hourlyVariables": ["pm2_5", "pm10", "nitrogen_dioxide", "ozone", "us_aqi"],
    "domain": "auto",
    "cellSelection": "nearest"
  }
}
```

Use the operation input schema returned by the same `data describe` response for current variable
codes, enum meanings, and limits. Do not add an API key: this capability uses
the public non-commercial endpoint only. Commercial customer-endpoint access
requires a separately reviewed capability. Do not silently geocode names,
widen the date range, add locations or variables, or change model-domain
selection beyond the user's intent.

## Run

```bash
tiangong-ai data run open-meteo.air-quality fetch-hourly \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat every value as a CAMS-derived model-grid estimate, not a measurement at
  the requested coordinate. Preserve requested and returned grid coordinates.
- Retain nulls as unavailable model values. Never convert them to zero.
- Distinguish `series-missing` from `series-all-null`: missing means the
  provider did not return a requested series, while all-null means it returned
  a structurally valid, aligned series with no usable numeric values. Preserve
  an all-null series and report the machine-readable issue code.
- Treat `partial` as incomplete coordinate, timestamp, or variable coverage and
  report the affected paths with the usable columns.
- In GMT mode, require exactly 24 strictly ascending hourly timestamps per
  inclusive date and a zero provider UTC offset. Preserve time-count, ordering,
  timezone, unit, array-alignment, and non-numeric-value issues instead of
  treating a structurally short response as complete.
- Treat `blocked` as no usable business result and surface the structured
  errors instead of bypassing limits or switching endpoints.
- Report record-limit truncation; all variable arrays are aligned to the
  returned GMT time axis, but a truncated result is not an exhaustive window.
- Attribute Open-Meteo and the underlying CAMS data provider. The public
  endpoint is non-commercial; do not imply commercial-use permission.
- Do not infer station conditions, regulatory compliance, health effects,
  personal exposure, alerts, or causes from these modeled fields.
- Cross-source comparison and research evidence admission belong to the caller
  or Auto Research, not this atomic Skill.
- The fixed public execution contract intentionally omits the source script's
  arbitrary timezone, optional API-key/customer access, endpoint overrides, and
  raw JSON/log artifacts. Those require separately reviewed contracts rather
  than Skill-local parameters.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
