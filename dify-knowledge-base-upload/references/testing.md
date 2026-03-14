# Testing

## Dry run
Use dry run first to validate file paths and payload files locally:

Without metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file ./SKILL.md \
  --data-json assets/example-upload-data.json \
  --dry-run
```

With metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file ./SKILL.md \
  --data-json assets/example-upload-data.json \
  --metadata-json assets/example-metadata.json \
  --dry-run
```

## Live request
With env vars set:

Upload only:

```bash
python3 scripts/upload_to_dataset.py \
  --file /path/to/document.pdf \
  --data-json assets/example-upload-data.json
```

Upload and write metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file /path/to/document.pdf \
  --data-json assets/example-upload-data.json \
  --metadata-json assets/example-metadata.json
```

Replace the placeholder keys in `assets/example-metadata.json` with your actual existing Dify metadata field names before running a live request.

Checklist:
- Upload returns `upload_response.document.id`.
- `metadata_applied` is an empty array and `metadata_response` is `null` when `--metadata-json` is omitted.
- `metadata_applied` uses existing Dify field names or ids.
- `metadata_response` is non-null when metadata input is provided.
- If metadata names cannot be resolved, the script exits before calling the update endpoint.
