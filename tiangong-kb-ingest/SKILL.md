---
name: tiangong-kb-ingest
description: Upload local files or folders into Tiangong KB through the Tiangong AI CLI. Use when a user asks an agent to ingest documents into a KB collection, list uploadable collections, or explain ingest duplicate/status results.
---

# Tiangong KB Ingest

## Boundary

Use the Tiangong AI CLI for execution. The skill only decides when to use the CLI, which collection selector to pass, how to explain duplicate/status output, and which secrets or backend systems must stay out of scope.

CLI-owned behavior:

- `tiangong-ai kb ingest upload`
- `tiangong-ai kb ingest status`
- `tiangong-ai kb collections list`
- manifest/checkpoint
- concurrency/retry
- JSON output
- API key and env handling

Skill-owned behavior:

- choose this workflow when the user wants to upload local files/folders into Tiangong KB
- choose the safest collection selector
- explain returned `duplicate`, `existingDocumentId`, document status, job id, request id, and idempotency key
- keep backend secrets and storage/search systems out of the agent workflow

## Short Workflow

1. Confirm the local file or folder path exists.
2. Pick one collection selector:
   - prefer `--collection-name` or `TIANGONG_KB_DEFAULT_COLLECTION_NAME` when the user gives a unique display name
   - use `--collection-key`, `--collection-path`, or `--collection-id` only when the user provides an exact selector
3. For a first check, run `node scripts/run_kb_ingest.mjs collections list --json`.
4. Upload with `node scripts/run_kb_ingest.mjs upload <path> --json`.
5. For folders or long runs, add `--recursive`, `--concurrency`, `--retries`, and optionally `--manifest`.
6. If the user asks to wait or verify later state, use `--wait` on upload or `node scripts/run_kb_ingest.mjs status <document-id> --json`.
7. Report only current CLI output and backend response fields. Do not infer success from direct database queries.

## Examples

Upload one file:

```bash
node scripts/run_kb_ingest.mjs upload /path/to/document.pdf --json
```

Upload a folder recursively:

```bash
node scripts/run_kb_ingest.mjs upload /path/to/folder --recursive --concurrency 3 --retries 3 --json
```

List uploadable collections:

```bash
node scripts/run_kb_ingest.mjs collections list --capability upload --json
```

Check document status:

```bash
node scripts/run_kb_ingest.mjs status <document-id> --json
```

## Result Interpretation

- `duplicate: true`: the backend found an active existing document with the same dedupe identity. Tell the user the upload was treated as duplicate and show `existingDocumentId` when present.
- `duplicate: false` or missing: do not claim dedupe was checked unless the backend response says so.
- `documentId`: use this id for follow-up status checks.
- `status`: explain it as backend state. Terminal states are success/failure/deleted according to the API response; nonterminal states mean processing is still underway.
- `jobId`, `requestId`, `idempotencyKey`, `rawUri`: include them when present because they help support/debugging.

## Safety Boundary

Never ask for or use:

- Supabase URL, anon key, service-role key, email/password, or session cookies
- NAS credentials or raw storage paths
- AWS, Pinecone, or OpenSearch admin credentials
- direct SQL/database queries to verify the current upload

The backend owns authorization, collection permission checks, duplicate detection, raw canonical write, document reservation, parse queueing, and status transitions. Read `references/env.md` only when environment setup or security boundaries matter.
