# Request/response

## Workflow
1. Discover the published datasource plugin for the target dataset.
2. Upload one local file to the pipeline upload endpoint.
3. Run the published pipeline with `response_mode`.
4. Poll indexing status until the batch reaches a terminal state.
5. Fetch the resulting document detail and verify chunk/token counts.
6. If metadata JSON is provided, resolve dataset metadata definitions and write metadata for the uploaded `document_id`.

## Published datasource discovery
- Endpoint: `GET /datasets/{dataset_id}/pipeline/datasource-plugins?is_published=true`
- Auth header: `Authorization: Bearer <DIFY_API_KEY>`
- The script selects the published plugin and reads:
  - `datasource_type`
  - `node_id` as `start_node_id`
  - `user_input_variables`

## File upload request
- Endpoint: `POST /datasets/pipeline/file-upload`
- Auth header: `Authorization: Bearer <DIFY_API_KEY>`
- Form parts:
  - `file`: local file body

## Pipeline run request
- Endpoint: `POST /datasets/{dataset_id}/pipeline/run`
- Auth header: `Authorization: Bearer <DIFY_API_KEY>`
- JSON body written by the script:
```json
{
  "inputs": {},
  "datasource_type": "local_file",
  "datasource_info_list": [
    {
      "related_id": "uploaded-file-id",
      "name": "your-file-name.txt",
      "transfer_method": "local_file"
    }
  ],
  "start_node_id": "published-node-id",
  "is_published": true,
  "response_mode": "blocking"
}
```

Notes:
- The request field is `response_mode`, not `streaming`.
- Pure text must be saved to a local `.txt` or `.md` file first, then uploaded through the same flow.
- If the published pipeline later exposes required inputs, pass them through `--inputs-json`.
- In the current tested deployment, `.md` completed successfully while `.txt` returned a Dify-side indexing `400`.

## Indexing status request
- Endpoint: `GET /datasets/{dataset_id}/documents/{batch}/indexing-status`
- Success criteria for this skill:
  - `indexing_status = "completed"`
  - `total_segments > 0`

## Document detail request
- Endpoint: `GET /datasets/{dataset_id}/documents/{document_id}?metadata=without`
- Success criteria for this skill:
  - `segment_count > 0`
  - `tokens > 0` when the deployment populates it

Notes:
- In the current tested pipeline deployment, successful `.md` uploads can still return `"tokens": null`. The script treats missing `tokens` as a warning, not a fatal validation error.

## Metadata input formats supported by `upload_to_dataset.py`

These examples are templates only. Replace the field names or ids with the metadata definitions that already exist in your target dataset.

### Flat object by field name
```json
{
  "your_existing_field_name": "replace-me",
  "another_existing_field_name": 2026
}
```

### Explicit list by field name or id
```json
[
  {
    "name": "your_existing_field_name",
    "value": "replace-me"
  },
  {
    "id": "your-existing-metadata-field-id",
    "value": 2026
  }
]
```

## Metadata write request
- List definitions: `GET /datasets/{dataset_id}/metadata?page=1&limit=100`
- Update values: `POST /datasets/{dataset_id}/documents/metadata`

Metadata definition list responses vary by Dify deployment or version. This skill accepts either:
- `{ "data": [...] }`
- `{ "doc_metadata": [...] }`

Request body written by the script:
```json
{
  "operation_data": [
    {
      "document_id": "uploaded-document-id",
      "metadata_list": [
        {
          "id": "metadata-field-id",
          "name": "your_existing_field_name",
          "value": "replace-me"
        }
      ]
    }
  ]
}
```

## Output
Successful script output is JSON with:
- `dataset_id`
- `pipeline_config`
- `discovery_response`
- `file_upload_response`
- `pipeline_run_request`
- `pipeline_run_response`
- `batch`
- `document_id`
- `indexing_status_response`
- `document_response`
- `metadata_applied`
- `metadata_response` (null when `--metadata-json` is omitted)
- `validation`

The script exits non-zero when `validation.ok` is false. Non-fatal signals, such as missing `tokens`, are emitted through `validation.warnings`.

Use `--dry-run` to print the resolved endpoints, file metadata, pipeline input payload, and parsed metadata input without sending requests.
