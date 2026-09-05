---
name: open-meteo-flood-fetch
description: Retrieve daily Open-Meteo GloFAS simulated river-discharge fields through the Tiangong CLI for up to ten known coordinates and one bounded date window, optionally including forecast ensemble members. Use for broad modeled river-flow context or separately governed uncertainty analysis; do not use for gauge observations, named-river lookup, flood alerts or severity, emergency advice, geocoding, commercial public-endpoint use, or causal interpretation.
---

# Open-Meteo Flood

Use the CLI-owned `open-meteo.flood` capability. This Skill supplies intent
routing and result-use boundaries only; the CLI owns source discovery,
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
tiangong-ai data describe open-meteo.flood --json
tiangong-ai data doctor open-meteo.flood --json
```

Use the returned Discovery Metadata to confirm current GloFAS coverage,
forecast and historical availability, model limitations, public-endpoint terms,
attribution, `provides`, and `doesNotProvide`. Do not substitute facts remembered
from an older Skill revision.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope and replace the two version
placeholders with the exact versions from the same `data describe` response. Coordinates are WGS84
decimal degrees. The CLI fixes dates to GMT, preserves coordinate order, and
normalizes the variable order:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "open-meteo.flood",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "fetch-daily",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "locations": [
      { "latitude": 52.52, "longitude": 13.41 },
      { "latitude": 48.85, "longitude": 2.35 }
    ],
    "startDate": "2026-03-01",
    "endDate": "2026-03-05",
    "dailyVariables": ["river_discharge", "river_discharge_p75"],
    "includeEnsembleMembers": true,
    "cellSelection": "nearest"
  }
}
```

Use the operation input schema returned by the same `data describe` response for current variable
codes, enum meanings, and limits. Ensemble members require `river_discharge`
and can materially increase response size. Forecast statistics may be absent
for consolidated historical dates. Do not add an API key: this capability uses
the public non-commercial endpoint only. Commercial customer-endpoint access
requires a separately reviewed capability. Do not silently geocode names,
adjust coordinates, widen the date range, or change variables beyond the
user's intent.

## Run

```bash
tiangong-ai data run open-meteo.flood fetch-daily \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Treat every value as simulated GloFAS river-grid discharge, not an in-situ
  gauge observation. Preserve requested and returned river-grid coordinates.
- The provider selects the largest represented river near each coordinate.
  Verify that selection independently before associating results with a named
  river or local channel.
- Retain nulls as unavailable model values. Never convert them to zero.
- Distinguish `series-missing` from `series-all-null`: missing means the
  provider did not return a requested discharge series, while all-null means
  it returned a structurally valid, aligned series with no usable numeric
  values. Preserve an all-null series and report the machine-readable issue
  code.
- Treat `partial` as incomplete coordinate, date, variable, or ensemble-member
  coverage and report the affected paths with the usable columns.
- Require one real, strictly ascending GMT date per inclusive request date and
  a zero provider UTC offset. Preserve count, ordering, timezone, unit,
  alignment, and non-numeric-value issues rather than treating a structurally
  incomplete response as complete.
- Treat `blocked` as no usable business result and surface the structured
  errors instead of bypassing limits or switching endpoints.
- Report record-limit truncation; all variables and ensemble arrays remain
  aligned to the returned GMT dates, but a truncated result is not exhaustive.
- Ensemble members are model realizations. Do not translate them directly into
  flood probability, severity, alerts, return periods, or emergency guidance.
- Preserve each provider `river_discharge_memberN` field name and its safe
  integer member ID; member suffixes are not assumed to have a fixed width.
- Attribute Open-Meteo and GloFAS. The public endpoint is non-commercial; do
  not imply commercial-use permission.
- Cross-source comparison and research evidence admission belong to the caller
  or Auto Research, not this atomic Skill.
- The fixed public execution contract intentionally omits the source script's
  arbitrary timezone, optional API-key/customer access, endpoint overrides,
  and raw JSON/log artifacts. Those require separately reviewed contracts.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
