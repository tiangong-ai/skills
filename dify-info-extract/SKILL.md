---
name: dify-info-extract
description: "Call this Dify workflow by uploading local transcript documents and images, sending them as `raw_scripts`, `photos`, and optional `scene`, then returning the Dify response. The current exported workflow exposes its main result at `data.outputs.full_info_with_image_des`. Use when Codex needs to process local files with this specific Dify workflow or debug its API invocation and output shape."
---

# Dify Info Extract

## Quick start
- Treat this as a dedicated skill for one specific Dify workflow, not a generic Dify wrapper.
- Prefer `scripts/run_workflow.py` over handwritten multipart or workflow requests unless you are debugging low-level HTTP details.
- Read `references/env.md` and `references/api.md` when you need environment or API details.
- Read `references/workflow-summary.md` when you need the current input and output contract.
- Align changes to the latest workflow export stored in the repository before updating the skill.
- Consistent response contract:
  - By default the script prints the full Dify JSON response.
  - With `--print-outputs-only`, it prints only `data.outputs`.
  - In the current export, the main result is `data.outputs.full_info_with_image_des`.

## Run the workflow
- Set environment variables:
  - `DIFY_WORKFLOW_API_BASE_URL`, including `/v1`, for example `https://thuenv.tiangong.world:7001/v1`
  - `DIFY_WORKFLOW_API_KEY`
  - optional: `DIFY_WORKFLOW_USER`
- To avoid changing global environment variables, put app-specific settings in `.env.workflow.local` under this skill.
  - Start from `.env.workflow.local.example`
  - The script auto-loads `.env.workflow.local`
  - Or pass `--env-file /path/to/.env.workflow.local`
- Pass local transcript or document files with `--raw-script` and local images with `--photo`. Both flags are repeatable.
- If the caller already has a research context, pass it with `--scene` so the workflow receives it as `inputs.scene`.
- The script uses blocking mode and prints the full JSON response by default. Use `--print-outputs-only` when only the workflow outputs object is needed.
- Stability defaults:
  - `--upload-retries 3`
  - `--workflow-retries 0`
  - `--retry-delay-seconds 3`
  - `--connect-timeout-seconds 30`
  - `--request-timeout-seconds 1800`
- `--request-timeout-seconds` defaults to 30 minutes.
- Local files are validated before upload:
  - extension
  - MIME guess
  - file signature or content sniffing
  - file size, default `500 MB`
- Use `--skip-file-validation` only when you intentionally want to bypass those checks.
- For real runs, prefer both:
  - `--output-file /tmp/xxx.json`
  - `--state-file /tmp/xxx.state.json`
- This keeps the full response and progress snapshots on disk even if the surrounding session is interrupted.

```bash
python3 scripts/run_workflow.py \
  --raw-script /path/to/transcript.docx \
  --raw-script /path/to/notes.pdf \
  --scene water-quality-research \
  --photo /path/to/site-1.jpg \
  --photo /path/to/site-2.png
```

## Merge extra inputs carefully
- The latest workflow export shows these start-node inputs:
  - file inputs: `raw_scripts`, `photos`
  - text input: `scene`
- If the Dify workflow later adds more text or toggle fields, pass them through `--inputs-json`.
- If `--inputs-json` already contains `raw_scripts` or `photos`, uploaded file objects are appended rather than overwritten.
- If both `--scene` and `--inputs-json` are provided, the CLI `--scene` value is written to `inputs.scene`.
- If the workflow structure changes, update `references/workflow-summary.md` before changing the script.
- In the current export, the end node exposes only one main output key: `full_info_with_image_des`.

## Interpret the response
- Dify blocking workflow responses usually place workflow results under `data.outputs`.
- Return the full JSON response when run metadata, state, or debugging context matters.
- In the current export, the main result is `data.outputs.full_info_with_image_des`, typically an array of file objects.
- On authentication failures, verify `Authorization: Bearer <key>`, the `/v1` API root, and that the key belongs to the target workflow app.
- If file upload succeeds but workflow input validation fails, verify that `raw_scripts` contains document files and `photos` contains image files.
- If you see `Could not resolve host`, the issue is usually network access in the execution environment rather than the skill logic.
- If outputs are empty, first confirm the input files are real and valid; fake test files often lead to empty workflow outputs.
- If upload is rejected before the workflow starts, check for disguised extensions, corrupted files, or size-limit violations.
- If a run times out, inspect `--state-file` first:
  - if uploads finished, the bottleneck is likely workflow runtime or server-side processing
  - `--workflow-retries` defaults to `0` to avoid duplicate workflow executions after a client-side timeout

## Resources
- `scripts/run_workflow.py`: upload local files and invoke the workflow API
- `references/env.md`: environment variables and recommended defaults
- `references/api.md`: Dify file upload and workflow run notes
- `references/workflow-summary.md`: input and output contract derived from the workflow export
