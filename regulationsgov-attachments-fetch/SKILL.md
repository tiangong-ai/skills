---
name: regulationsgov-attachments-fetch
description: Download bounded public attachment files for exact Regulations.gov comment IDs through the Tiangong CLI. Use when selected regulatory comments have supporting files that must be acquired locally with source lineage, SHA-256 hashes, byte counts, and a manifest; do not use for comment search, arbitrary URL download, file safety certification, text extraction, stance analysis, legal interpretation, or evidence synthesis.
---

# Regulations.gov Attachments Fetch

Use the CLI-owned `regulations-gov.attachments/download` operation. This Skill
supplies intent routing, explicit-ID selection, local-directory discipline, and
result-use boundaries only. The CLI TypeScript 7 runtime owns the official API
contract, credential injection, endpoint restrictions, schemas, limits,
downloads, transactional file writes, partial results, and receipts.

## Before running

1. Read `references/tiangong-data-requirement.json`.
2. Use the caller- or workspace-resolved stable CLI. The requirement declares
   compatible capability and operation contract majors; it does not select a
   package build.
3. Run `data describe` with that same CLI. Continue only when the capability
   ID and required contract majors match, and copy the exact current
   capability/operation versions from that response into the run request.
4. Inspect `manifest.availability` and `discovery.availability`. This capability
   is currently `suspended` because its prerequisite Regulations.gov live gate
   returned HTTP 503. Stop while it remains suspended; do not create an
   artifact run, retry directly, or assume a configured API key restores it.

```bash
tiangong-ai data describe regulations-gov.attachments --json
```

Use the returned Discovery Metadata to confirm current coverage, granularity,
limits, selection hints, `provides`, and `doesNotProvide`. Do not rely on source
facts copied from an older Skill revision.

## Select comments and attachments

- Start from exact public comment IDs selected by the user or by the
  `regulationsgov-comments-fetch` or `regulationsgov-comment-detail-fetch`
  workflow. This operation does not search comments, dockets, or documents.
- Omit `attachmentIds` to select every attachment returned for those comments,
  or supply an exact allowlist when only reviewed attachment IDs are needed.
- Set `maxFiles` and `maxTotalBytes` deliberately. The CLI also applies the
  lower manifest/runtime limits and reports bounded or missing coverage.
- Configure `REGGOV_API_KEY` only in the environment. Never place it in argv,
  request JSON, a URL, or the artifact directory.
- Create a dedicated, empty, existing directory and use its absolute path for
  `--artifact-dir`. The CLI refuses symlink directories and existing target
  filenames; it never returns the absolute directory in the result or receipt.

## Prepare the request

Build one `tiangong.data.run-request.v1` envelope. Replace both version
placeholders with the exact versions from the same `data describe` response and validate `input` against the current
operation schema returned by `data describe`.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "regulations-gov.attachments",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "download",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "commentIds": ["EPA-HQ-OAR-2026-0001-0002"],
    "attachmentIds": ["EPA-HQ-OAR-2026-0001-0002-ATTACHMENT-1"],
    "maxFiles": 10,
    "maxTotalBytes": 10000000
  }
}
```

Use run-request `limits` only when intentionally lowering the manifest's page,
record, response-byte, or timeout ceiling. Do not place a base URL, download
URL, output path, retry setting, or credential in `input`.

## Run

```bash
tiangong-ai data run regulations-gov.attachments download \
  --input /absolute/path/to/request.json \
  --artifact-dir /absolute/path/to/empty-artifact-directory \
  --json
```

Preserve the complete `tiangong.data.run-result.v1` envelope and the generated
manifest together with every committed file. The result binds relative names,
source URLs, content types, provider and actual sizes, SHA-256 hashes,
observations, warnings, errors, and the core receipt.

## Result boundaries

- The CLI obtains attachment relationships through official comment-detail
  requests and downloads only exact-origin `downloads.regulations.gov` URLs.
  It does not accept arbitrary download URLs, standalone attachment lookup, or
  redirects.
- Treat all files as untrusted public-submission bytes. A successful download,
  matching provider size, content type, or SHA-256 establishes byte identity
  and lineage, not safety, truth, relevance, or evidentiary sufficiency.
- `success` means the explicitly bounded selection completed. A max-file,
  max-byte, or runtime cap still narrows coverage; report it and never claim
  that omitted attachments do not exist.
- `partial` preserves verified earlier files and the manifest while identifying
  missing metadata or files. Report each missing item. Treat `blocked` as no
  usable run and do not use temporary or pre-existing directory contents.
- Keep the generated manifest with the files when moving them. Verify hashes
  before downstream processing and retain comment, attachment, format, and URL
  lineage.
- Malware scanning, safe opening, OCR, text extraction, personal-data review,
  content interpretation, stance or sentiment analysis, legal conclusions, and
  durable evidence admission require separate governed workflows.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
