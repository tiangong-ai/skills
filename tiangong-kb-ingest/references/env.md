# Environment

The skill invokes the Tiangong AI CLI. The CLI calls the Tiangong KB ingest API and authenticates with an API key sent as `Authorization: Bearer <token>`.

## Required

```text
TIANGONG_AI_API_KEY=<kb-api-token>
TIANGONG_KB_DEFAULT_COLLECTION_NAME=<unique-collection-display-name>
```

The API key should include:

- `kb:upload` for uploading files.
- `kb:read` for collection-name resolution, collection listing, and status polling.

`TIANGONG_KB_DEFAULT_COLLECTION_NAME` is the human-visible collection name. It must match exactly one uploadable collection for the API key actor. If names are duplicated, use a precise selector instead.

## Optional

```text
TIANGONG_KB_API_BASE_URL=https://thuenv.tiangong.world:7300
TIANGONG_KB_API_PATH_PREFIX=/api/v1/kb
TIANGONG_KB_DEFAULT_COLLECTION_KEY=course/thu_humanities
TIANGONG_KB_DEFAULT_COLLECTION_PATH=/course/thu_humanities
TIANGONG_KB_MANIFEST_PATH=.tiangong-kb-ingest-manifest.jsonl
TIANGONG_KB_UPLOAD_CONCURRENCY=3
TIANGONG_KB_UPLOAD_RETRIES=3
TIANGONG_KB_POLL_INTERVAL=2
TIANGONG_KB_TIMEOUT=300
TIANGONG_KB_WAIT_TIMEOUT=300
TIANGONG_AI_CLI_BIN=/absolute/path/to/tiangong
```

`TIANGONG_KB_API_BASE_URL` is optional because the CLI defaults to `https://thuenv.tiangong.world:7300`. Set it only for local, staging, or another compatible deployment.

`TIANGONG_KB_DEFAULT_COLLECTION_ID` is accepted only as a legacy alias for collection name. Do not put a UUID in it. For UUID-based uploads, pass `--collection-id` explicitly.

`TIANGONG_KB_API_KEY` is accepted as a fallback alias for `TIANGONG_AI_API_KEY`.

`TIANGONG_AI_CLI_BIN` is optional. Set it only when `tiangong` is not on PATH
and the skill is not running inside the workspace checkout that contains
`tiangong-ai-cli`.

For large uploads, the manifest is append-only JSONL. Rerunning the same command with the same manifest skips files whose latest checkpoint status is `succeeded`; use `--force` to upload them again. The current per-file checkpoint key includes `sha256` and file size.

## Do Not Configure

Do not put these in the skill environment:

```text
TIANGONG_KB_SUPABASE_URL
TIANGONG_KB_SUPABASE_PUBLISHABLE_KEY
TIANGONG_KB_EMAIL
TIANGONG_KB_PASSWORD
SUPABASE_SERVICE_ROLE_KEY
AWS_SECRET_ACCESS_KEY
SYNOLOGY_PASSWORD
PINECONE_API_KEY
OPENSEARCH_ADMIN_SECRET
```

The backend owns those credentials and enforces collection grants.
