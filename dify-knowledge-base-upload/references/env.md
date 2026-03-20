# Env (caller side)

Required caller env vars:
- `DIFY_API_BASE_URL` with `/v1` included
- `DIFY_DATASET_ID`
- `DIFY_API_KEY`

The script auto-loads `.env` from the current working directory, the skill directory, or the repo root if present. The skill does not bundle env files or fixed dataset values.

Optional debug overrides:
- `DIFY_PIPELINE_RESPONSE_MODE`
- `DIFY_POLL_INTERVAL_SECONDS`
- `DIFY_POLL_TIMEOUT_SECONDS`
- `DIFY_PIPELINE_START_NODE_ID`
- `DIFY_PIPELINE_DATASOURCE_TYPE`

Leave `DIFY_PIPELINE_START_NODE_ID` and `DIFY_PIPELINE_DATASOURCE_TYPE` unset in normal use so the script can discover the published datasource plugin at runtime.

Endpoints used by this skill:
- Discover published datasource: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/pipeline/datasource-plugins?is_published=true`
- Upload file: `${DIFY_API_BASE_URL}/datasets/pipeline/file-upload`
- Run pipeline: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/pipeline/run`
- Poll indexing status: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/documents/{batch}/indexing-status`
- Fetch document detail: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/documents/{document_id}?metadata=without`
- List metadata fields: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/metadata`
- Update document metadata: `${DIFY_API_BASE_URL}/datasets/${DIFY_DATASET_ID}/documents/metadata`

CLI overrides are also supported:
- `--api-base-url`
- `--dataset-id`
- `--api-key`
- `--response-mode`
- `--poll-interval-seconds`
- `--poll-timeout-seconds`
- `--pipeline-start-node-id`
- `--pipeline-datasource-type`
