# API Notes

## Endpoints used by the script
- `POST /files/upload`
  - Multipart upload for local files before workflow execution.
  - The script sends `user` and `file`.
- `POST /workflows/run`
  - Executes the published workflow in `blocking` mode.
  - The script sends JSON with `inputs`, `response_mode`, and `user`.

## Input mapping for this skill
- Uploaded transcript/document files are turned into file objects under `inputs.raw_scripts`.
- Uploaded image files are turned into file objects under `inputs.photos`.
- When provided, the user-facing result name is also sent as `inputs.filename`.
- Each uploaded file object is shaped like:

```json
{
  "transfer_method": "local_file",
  "upload_file_id": "uploaded-file-id",
  "type": "document"
}
```

- For photos, `type` becomes `"image"`.

## Response handling
- Keep the default blocking mode so the script can print one final JSON payload.
- By default the script prints the full Dify response JSON.
- When the caller only needs the workflow outputs object, use `--print-outputs-only` to print `data.outputs` when present.
- When debugging, inspect the full JSON response first because Dify often puts run metadata and error details alongside outputs.
- For longer runs, pass `--output-file /tmp/<name>.json` so the response is persisted even if the surrounding terminal session is interrupted.
- The latest exported workflow currently exposes one main output key: `data.outputs.full_info_with_image_des`.
- In practice this key is typically an array of downloadable file objects.

## Stability defaults in the script
- The script now applies:
  - connect timeout: 30 seconds
  - max request time: 1800 seconds
  - upload retries: 3
  - workflow retries: 0 by default
  - retry delay: 3 seconds
  - local file validation before upload
- Upload retries are safer than workflow retries because replaying `/workflows/run` can create duplicate executions.
- Upload and workflow stages print progress to stderr so long runs are easier to distinguish from a hang.
- Optional `--state-file` snapshots progress, uploaded file ids, workflow status, and timestamps.

## Local file validation
- `raw_scripts` currently accepts common document extensions used by this workflow wrapper: `.txt`, `.md`, `.pdf`, `.doc`, `.docx`, `.rtf`, `.csv`, `.tsv`, `.json`
- `photos` currently accepts `.jpg`, `.jpeg`, `.png`, `.gif`, `.webp`, `.svg`
- The script checks:
  - extension allowlist
  - guessed MIME
  - file signature/content sniffing
  - size threshold via `--max-file-size-mb`
- Use `--skip-file-validation` only for exceptional cases where the file is known-good but outside the wrapper's current heuristics.

## Troubleshooting
- `401` or `403`: verify the API key belongs to the workflow app, and the header is `Authorization: Bearer <key>`.
- `404`: verify the base URL points to the API root with `/v1`, not the web UI route.
- Upload succeeds but run fails on file inputs: confirm `raw_scripts` is used for document files and `photos` is used for image files.
- `curl: (6) Could not resolve host`: DNS/network restriction in the execution environment, not a workflow contract issue.
- Empty outputs on obviously fake files: use real documents/images before debugging the workflow itself.
- `curl: (28)`: the request exceeded the configured timeout threshold. The default threshold is now 30 minutes.
