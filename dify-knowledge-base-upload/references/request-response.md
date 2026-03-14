# Request/response

## Workflow
1. Upload one local file with `multipart/form-data`.
2. If metadata JSON is provided, list dataset metadata definitions.
3. Resolve metadata field names to Dify metadata ids.
4. If metadata JSON is provided, write metadata for the uploaded `document_id`.

## Upload request
- Endpoint: `POST /datasets/{dataset_id}/document/create-by-file`
- Auth header: `Authorization: Bearer <DIFY_API_KEY>`
- Form parts:
  - `file`: local file body
  - `data`: JSON string for indexing/process settings

Example `data` JSON:
```json
{
  "indexing_technique": "high_quality",
  "process_rule": {
    "mode": "automatic"
  }
}
```

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
- `document_id`
- `upload_response`
- `metadata_applied`
- `metadata_response` (null when `--metadata-json` is omitted)

Use `--dry-run` to print the resolved endpoints, file metadata, upload data, and parsed metadata input without sending requests.
