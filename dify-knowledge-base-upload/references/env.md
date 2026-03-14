# Env (caller side)
- Base URL: `$DIFY_API_BASE_URL` (example: `https://api.dify.ai/v1`)
- Dataset ID: `$DIFY_DATASET_ID`
- API key: `$DIFY_API_KEY`

Endpoints used by this skill:
- Upload file: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/document/create-by-file`
- List metadata fields: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/metadata`
- Update document metadata: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/documents/metadata`

CLI overrides are also supported:
- `--api-base-url`
- `--dataset-id`
- `--api-key`
