---
name: dify-knowledge-base-upload
description: Upload local files to an existing Dify dataset and optionally update document metadata using existing dataset metadata fields. Use when importing documents into a known Dify knowledge base, testing upload payloads, or assigning metadata after upload.
---

# Dify Knowledge Base Upload

## Prepare required inputs
- Provide a local file path to upload.
- Optionally provide upload `data` JSON for indexing and processing settings.
- Optionally provide metadata JSON:
  - a flat object mapping existing Dify metadata field names to values, or
  - a list of `{ "name": ..., "value": ... }` / `{ "id": ..., "value": ... }` items.
  - The skill does not hardcode your metadata schema. Pass the current field names or ids in the JSON you provide at runtime.
- Read these env vars:
  - `DIFY_API_BASE_URL`: base API URL and include `/v1` (example: `https://api.dify.ai/v1`).
  - `DIFY_DATASET_ID`: target dataset ID.
  - `DIFY_API_KEY`: send as `Authorization: Bearer <DIFY_API_KEY>`.

## Run workflow
- Use `scripts/upload_to_dataset.py` to:
  - upload the file to `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/document/create-by-file`
  - optionally resolve metadata field names through `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/metadata`
  - optionally update metadata through `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/documents/metadata`
- Upload without metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file /path/to/document.pdf \
  --data-json assets/example-upload-data.json
```

- Upload and then apply metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file /path/to/document.pdf \
  --data-json assets/example-upload-data.json \
  --metadata-json assets/example-metadata.json
```

## Interpret response
- Success prints JSON with `document.id`, `batch`, `upload_response`, and optional `metadata_response`.
- If `--metadata-json` is omitted, the script uploads the file only and skips metadata API calls.
- Metadata keys must already exist in the target dataset. Unknown names fail fast before the metadata write request.
- `assets/example-metadata.json` is only a template. Replace its keys with your own existing Dify metadata field names before live use.
- Use `--dry-run` to validate local files and payload shape without calling Dify.

## Troubleshoot quickly
- If auth fails, verify `DIFY_API_KEY` and `Authorization` header format.
- If upload fails, verify `DIFY_API_BASE_URL` includes `/v1` and `DIFY_DATASET_ID` points to an existing dataset.
- If metadata resolution fails, rename keys in the metadata JSON to the exact field names defined in Dify.
- If Dify rejects the upload `data` payload, start from `assets/example-upload-data.json` and adjust it to the target Dify version.

## References
- `references/env.md`
- `references/request-response.md`
- `references/testing.md`

## Assets
- `assets/example-upload-data.json`
- `assets/example-metadata.json`
