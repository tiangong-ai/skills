---
name: usbr-rise-fetch
description: Discover U.S. Bureau of Reclamation RISE catalog item IDs and retrieve bounded operational or water-environment time-series rows through the Tiangong CLI. Use when a task needs official RISE metadata or values for a known place, parameter, reservoir, dam, or project; do not use to infer shortage severity, compliance, causality, governance responsibility, or complete USBR coverage.
---

# USBR RISE Fetch

Use the CLI-owned `usbr.rise` capability. This Skill supplies intent routing,
item-selection discipline, and result-use boundaries only; the CLI TypeScript 7
runtime owns RISE endpoints, schemas, pagination, validation, limits, partial
results, and receipts.

## Before running

1. Read `references/tiangong-data-requirement.json`.
2. Use the caller- or workspace-resolved stable CLI. The requirement declares
   compatible capability and operation contract majors; it does not select a
   package build.
3. Run `data describe` with that same CLI. Continue only when the capability
   ID and required contract majors match, and copy the exact current
   capability/operation versions from that response into the run request.
4. Immediately inspect `manifest.availability` and `discovery.availability`.
   If either does not report `available`, stop before constructing a request
   and report its exact `reasonCode`, description, and resume criteria. Do not
   bypass suspension with a direct legacy API, EDR beta, page fetch, or proxy.

```bash
tiangong-ai data describe usbr.rise --json
```

Use the returned Discovery Metadata to confirm current source coverage,
granularity, limitations, selection hints, `provides`, and `doesNotProvide`.
Do not rely on source facts remembered from an older Skill revision.

## Choose the operation

- Use `discover-items` when the evidence need names a place, facility,
  parameter, or source but no official RISE item ID is grounded. Candidate
  order is provider page-scan order after client-side filtering, not relevance
  ranking or evidence weight.
- Use `fetch-results` only with explicit item IDs supported by a reviewed RISE
  catalog record or another official source. Add a bounded UTC window whenever
  the question has a time scope, and request item metadata when unit, timestep,
  transformation, location, or disclaimer context is needed.
- A bounded discovery scan can stop before later catalog pages. Refine terms or
  deliberately raise the run-request page limit within the manifest ceiling;
  never treat zero candidates as proof that the real-world record is absent.

## Discover item IDs

Build a `tiangong.data.run-request.v1` envelope. Replace both version
placeholders with the exact versions from the same `data describe` response and validate `input` against the
current operation schema returned by `data describe`.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "usbr.rise",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "discover-items",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "queryTerms": ["Lake Powell", "release"],
    "locationNameContains": "Glen Canyon",
    "pageSize": 100
  }
}
```

```bash
tiangong-ai data run usbr.rise discover-items \
  --input /absolute/path/to/discovery-request.json --json
```

Review candidate title, location, parameter, unit, timestep, transformation,
source code, temporal coverage, landing page, and spatial metadata before
selecting IDs. Preserve the discovery result when handing selected IDs to a
downstream fetch.

## Fetch result rows

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "usbr.rise",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "fetch-results",
  "operationVersion": "<describe.manifest.operations[1].operationVersion>",
  "input": {
    "itemIds": ["10835"],
    "afterUtc": "2025-01-01T00:00:00Z",
    "beforeUtc": "2025-01-31T23:59:59Z",
    "orderDateTime": "asc",
    "includeItemMetadata": true,
    "pageSize": 100
  }
}
```

```bash
tiangong-ai data run usbr.rise fetch-results \
  --input /absolute/path/to/results-request.json --json
```

Preserve the complete `tiangong.data.run-result.v1` envelope, including
`contract`, `warnings`, `errors`, `observations`, and `receipt`, with `data`.

## Result boundaries

- Treat `partial` as incomplete item, page, record, or metadata coverage.
  Retain successful rows and report each gap; do not replace missing values
  with zero. Treat `blocked` as no usable business result.
- Interpret a value only with its item ID, location, parameter, unit, timestep,
  transformation, source code, timestamp, status, and source disclaimer when
  available. Do not merge unlike items merely because their titles are close.
- Missing or sparse rows can reflect item choice, date filters, page caps,
  provider latency, or metadata availability. They are not proof of physical
  absence or non-operation.
- The capability does not determine drought or shortage severity, operating or
  legal compliance, causality, policy responsibility, evidence weight, or
  report readiness. Those judgments belong to a separately governed caller.
- Project-document retrieval, cross-source joins, durable evidence admission,
  normalization, and analysis remain outside this atomic Skill.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
