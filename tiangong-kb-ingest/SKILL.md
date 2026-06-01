---
name: tiangong-kb-ingest
description: Upload local files or folders into Tiangong KB through the Tiangong AI CLI. Use when a user asks an agent to ingest documents into a KB collection, list uploadable collections, or explain ingest duplicate/status results.
---

# Tiangong KB Ingest

## Boundary

Use the Tiangong AI CLI for execution through `npx @tiangong-ai/cli@latest` by default, so users do not need a preinstalled CLI. The agent may orchestrate CLI schema, scan, metadata dry-run, and bulk commands, but it must not call backend databases or storage systems directly.

CLI-owned behavior:

- `tiangong-ai kb ingest bulk`
- `tiangong-ai kb ingest bulk scan`
- `tiangong-ai kb ingest metadata dry-run`
- `tiangong-ai kb ingest status`
- `tiangong-ai kb collections list`
- `tiangong-ai kb collections schema`
- SQLite checkpoint and resume
- concurrency/retry
- JSON output
- API key and env handling

Skill-owned behavior:

- choose this workflow when the user wants to upload local files/folders into Tiangong KB
- choose the safest collection selector
- generate a conservative `rule_mode: layered` metadata map from collection schema plus folder scan when the user did not pass `--metadata-map`
- run CLI metadata dry-run before bulk run and pass the generated metadata map into bulk ingest
- explain returned `duplicate`, `existingDocumentId`, document status, job id, request id, and idempotency key
- keep backend secrets and storage/search systems out of the agent workflow

## Short Workflow

1. Confirm the local file or folder path exists.
2. Pick one collection selector:
   - prefer `--collection-name` or `TIANGONG_KB_DEFAULT_COLLECTION_NAME` when the user gives a unique display name
   - use `--collection-key`, `--collection-path`, or `--collection-id` only when the user provides an exact selector
3. Load dotenv defaults from the target path directory when present:
   - for a file, read `.env` from the file's parent directory
   - for a folder, read `.env` from that folder
   - loaded dotenv values fill only unset environment variables; explicit user
     inputs passed as CLI flags take precedence
4. For a first check, run `npx @tiangong-ai/cli@latest kb collections list --json`.
5. For bulk/upload runs without `--metadata-map`, generate `metadata-map.yaml`:
   - call CLI collection schema through the Tiangong KB ingest API
   - call CLI bulk scan for folder structure
   - write a layered metadata map with base filesystem fields plus conservative domain/detector rules
   - run CLI metadata dry-run against the generated map
6. Ingest with `npx @tiangong-ai/cli@latest kb ingest bulk <path> --json`; pass `--metadata-map metadata-map.yaml` unless a metadata map is already provided or the user explicitly asks to skip metadata-map generation.
7. For long runs, tune `--window-size`, `--top-up-max`, `--upload-concurrency`, `--retries`, and `--state`; do not add `--max-polls` unless the user explicitly wants a bounded monitoring run.
8. If the user asks to verify later state, use `npx @tiangong-ai/cli@latest kb ingest status <document-id-or-job-id> --json`.
9. Report only current CLI output and backend response fields. Do not infer success from direct database queries.

## Metadata Map Minimum

When generating `metadata-map.yaml`, start from this conservative layered map
and add only collection-specific fields that the schema requires:

```yaml
version: 1
rule_mode: layered
defaults:
  source: local_bulk_upload
layers:
  - name: base
    merge: all
    rules:
      - name: filesystem
        match:
          glob: "**/*"
        fields:
          relative_path:
            source: relative_path
          filename:
            source: filename
          filename_stem:
            source: filename_stem
          ext:
            source: ext
          path_depth:
            source: path_depth
          top_dir:
            source: top_dir
          parent_dir:
            source: parent_dir
```

For required schema fields not covered by path rules, add safe defaults under
`defaults`: prefer `other` when it is an enum value, otherwise the first enum
value, `0` for numbers, `false` for booleans, `1970-01-01` for dates, and
`unknown` for strings.

Validate before upload:

```bash
npx @tiangong-ai/cli@latest kb ingest bulk scan /path/to/folder --json
npx @tiangong-ai/cli@latest kb collections schema --collection-key course/thu_humanities --json
npx @tiangong-ai/cli@latest kb ingest metadata dry-run /path/to/folder --metadata-map metadata-map.yaml --json
```

For offline validation with a captured schema, pass
`--schema-file schema.json` to `metadata dry-run`.

## Examples

Upload one file:

```bash
npx @tiangong-ai/cli@latest kb ingest bulk /path/to/document.pdf --json
```

Upload a folder:

```bash
npx @tiangong-ai/cli@latest kb ingest bulk /path/to/folder --upload-concurrency 3 --retries 3 --json
```

Use an existing metadata map:

```bash
npx @tiangong-ai/cli@latest kb ingest bulk /path/to/folder --metadata-map metadata-map.yaml --json
```

Use a metadata map saved at a specific path:

```bash
npx @tiangong-ai/cli@latest kb ingest bulk /path/to/folder --metadata-map course-map.yaml --json
```

List uploadable collections:

```bash
npx @tiangong-ai/cli@latest kb collections list --capability upload --json
```

Read a collection schema through the API:

```bash
npx @tiangong-ai/cli@latest kb collections schema --collection-key course/thu_humanities --json
```

Check document status:

```bash
npx @tiangong-ai/cli@latest kb ingest status <document-id> --json
```

## Result Interpretation

- `duplicate: true`: the backend found an active existing document with the same dedupe identity. Tell the user the upload was treated as duplicate and show `existingDocumentId` when present.
- `duplicate: false` or missing: do not claim dedupe was checked unless the backend response says so.
- `documentId`: use this id for follow-up status checks.
- `status`: explain it as backend state. Terminal states are success/failure/deleted according to the API response; nonterminal states mean processing is still underway.
- `jobId`, `statePath`, `requestId`, `idempotencyKey`, `rawUri`: include them when present because they help support/debugging.
- `metadata-map.yaml`: explain it as a reusable layered rules file. It is not per-file metadata; CLI evaluates the same rule file for each file.

## Safety Boundary

Never ask for or use:

- Supabase URL, anon key, service-role key, email/password, or session cookies
- NAS credentials or raw storage paths
- AWS, Pinecone, or OpenSearch admin credentials
- direct SQL/database queries to verify the current upload

The backend owns authorization, collection permission checks, duplicate detection, raw canonical write, document reservation, parse queueing, and status transitions. Read `references/env.md` only when environment setup or security boundaries matter.
