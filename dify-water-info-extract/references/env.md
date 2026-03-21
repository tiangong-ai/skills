# Environment Variables

## Required
- `DIFY_WORKFLOW_API_BASE_URL`: Dify API base URL for this app, including `/v1`.
  - For the deployment the user referenced, the expected shape is `https://thuenv.tiangong.world:7001/v1`.
- `DIFY_WORKFLOW_API_KEY`: app-level API key for the published workflow.

## Optional
- `DIFY_WORKFLOW_USER`: caller identifier forwarded to Dify. If unset, the script uses `codex-dify-water-info-extract`.
- `--env-file /path/to/file`: load settings from a dotenv-style file for this run.

## Recommended local file
- Preferred location for app-specific secrets:
  - `/home/yangli/projects/tiangong/skills/dify-water-info-extract/.env.workflow.local`
- Starter template:
  - `/home/yangli/projects/tiangong/skills/dify-water-info-extract/.env.workflow.local.example`
- The script auto-loads `.env.workflow.local` when it exists, so this is the easiest place to write the workflow app URL and key without affecting the knowledge-base configuration.

## Supported fallbacks
- `DIFY_API_BASE_URL`: accepted as a fallback when `DIFY_WORKFLOW_API_BASE_URL` is not set.
- `DIFY_API_KEY`: accepted as a fallback when `DIFY_WORKFLOW_API_KEY` is not set.

## Notes
- Keep the base URL at the API root, not the web studio path. The `/app/<id>/develop` page is useful for humans, but the script calls `/v1/files/upload` and `/v1/workflows/run`.
- This skill assumes the target app has already exposed an API key.
