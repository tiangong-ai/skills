---
name: usbr-project-records-fetch
description: Inventory official U.S. Bureau of Reclamation project or program pages and their bounded same-origin record links through the Tiangong CLI. Use when a task starts from one or more known www.usbr.gov URLs and needs page metadata or candidate reports, notices, spreadsheets, or related records; do not use for USBR-wide search, RISE time series, linked-file retrieval, or legal, policy, operating, environmental-effects, or governance conclusions.
---

# USBR Project Records Fetch

Use the CLI-owned `usbr.project-records/fetch` operation. This Skill supplies
intent routing, URL-selection discipline, and result-use boundaries only; the
CLI TypeScript 7 runtime owns exact-origin enforcement, HTML parsing, schemas,
limits, retries, link normalization, partial results, and receipts.

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
   bypass suspension with a direct page fetch or another USBR endpoint.

```bash
tiangong-ai data describe usbr.project-records --json
```

Use the returned Discovery Metadata to confirm current coverage, granularity,
limits, selection hints, `provides`, and `doesNotProvide`. Do not rely on
objective source facts copied from an older Skill revision.

## Choose source pages

- Supply exact official `https://www.usbr.gov` project or program page URLs
  already grounded by the user or another reviewed source. This operation is
  not site search, ranking, discovery across USBR, or recursive crawling.
- Use `usbr-rise-fetch` instead when the task needs RISE catalog item IDs or
  operational and water-environment time-series values.
- Select only pages relevant to the evidence need. The CLI preserves caller
  order, allows no other USBR subdomain, and inventories only links present on
  each fetched page.
- Set `maxLinkedRecordsPerPage` deliberately when a smaller bounded inventory
  is sufficient. Reaching it means later page links were not returned.

## Prepare the request

Build one `tiangong.data.run-request.v1` envelope. Replace both version
placeholders with the exact versions from the same `data describe` response and validate `input` against the current
operation schema returned by `data describe`.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "usbr.project-records",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "fetch",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "urls": [
      "https://www.usbr.gov/uc/progact/amp/index.html",
      "https://www.usbr.gov/lc/region/programs/crbstudy.html"
    ],
    "maxLinkedRecordsPerPage": 25
  }
}
```

Use run-request `limits` only when intentionally lowering the manifest's page,
record, response-byte, or timeout ceiling. Do not place output paths, retry
settings, a custom base URL, external-link instructions, or credentials in
`input`.

## Run

```bash
tiangong-ai data run usbr.project-records fetch \
  --input /absolute/path/to/request.json --json
```

Preserve the complete `tiangong.data.run-result.v1` envelope, including
`contract`, `warnings`, `errors`, `observations`, and `receipt`, with `data`.

## Result boundaries

- A `project-page` record represents bytes actually fetched by this operation;
  preserve its URL, title, description, digest, byte length, safe response
  metadata, and selected links together.
- A `linked-document` record is only an availability cue derived from one
  anchor. Its extension-derived type is not content validation, and null
  content metadata means the target was not downloaded or reviewed.
- Treat `partial` as a failed later source page: retain successful records and
  report the missing page. Treat `blocked` as no usable result.
- Treat `max-pages`, `max-records`, and `max-linked-records` as explicit bounded
  coverage. Do not infer that omitted or unlinked records do not exist.
- Same-origin filtering excludes external sources and other USBR subdomains;
  it is a security and scope boundary, not an assessment of relevance.
- Downloading linked files, OCR, document parsing, durable evidence admission,
  cross-source synthesis, and legal, policy, operating, environmental-effects,
  or governance judgments remain outside this atomic Skill.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
