# Testing

## Dry run
Use dry run first to validate file paths and payload files locally:

Without metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file ./SKILL.md \
  --dry-run
```

With pipeline inputs and metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file ./SKILL.md \
  --inputs-json assets/example-pipeline-inputs.json \
  --metadata-json assets/example-metadata.json \
  --dry-run
```

## Live request
With caller env vars set:

Upload only:

```bash
python3 scripts/upload_to_dataset.py \
  --file /path/to/document.pdf
```

Upload and pass pipeline inputs:

```bash
python3 scripts/upload_to_dataset.py \
  --file /path/to/document.pdf \
  --inputs-json assets/example-pipeline-inputs.json
```

Upload and write metadata:

```bash
python3 scripts/upload_to_dataset.py \
  --file /path/to/document.pdf \
  --metadata-json assets/example-metadata.json
```

Pure text smoke test:

```bash
printf '%s\n' 'pipeline upload smoke test' > /tmp/dify_pipeline_test.txt
python3 scripts/upload_to_dataset.py \
  --file /tmp/dify_pipeline_test.txt
```

If `.txt` returns a Dify-side indexing `400` on this deployment, retry the same content as a `.md` file.

Replace the placeholder keys in `assets/example-metadata.json` with your actual existing Dify metadata field names before running a live request.

Checklist:
- `discovery_response` shows one published datasource plugin.
- `file_upload_response.id` is present.
- `pipeline_run_response.batch` is present.
- `document_id` is present.
- `validation.ok` is `true`.
- `validation.warnings` may mention missing `tokens` on pipeline deployments that do not populate that field.
- `metadata_applied` is an empty array and `metadata_response` is `null` when `--metadata-json` is omitted.
- `metadata_applied` uses existing Dify field names or ids.
- `metadata_response` is non-null when metadata input is provided.
- If metadata names cannot be resolved, the script exits before calling the update endpoint.
- If `user_input_variables` is non-empty in `discovery_response`, pass the missing keys in `--inputs-json`.
