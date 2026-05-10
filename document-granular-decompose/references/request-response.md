# MinerU With Images Contract

Endpoint:

- `POST ${UNSTRUCTURED_API_BASE_URL}/mineru_with_images?return_txt=true`

Request:

- Header: `Authorization: Bearer $UNSTRUCTURED_AUTH_TOKEN`
- Multipart form fields:
  - `file` (required)
  - `provider` (optional, sent only when `UNSTRUCTURED_PROVIDER` is set)
  - `model` (optional, sent only when `UNSTRUCTURED_MODEL` is set)
- Supported file types only:
  - `.bmp, .doc, .docm, .docx, .dot, .dotx, .gif, .jp2, .jpeg, .jpg, .markdown, .md, .odp, .odt, .pdf, .png, .pot, .potx, .pps, .ppsx, .ppt, .pptm, .pptx, .tiff, .webp, .xls, .xlsm, .xlsx, .xlt, .xltx`
- Supported office formats:
  - `.doc, .docm, .docx, .dot, .dotx, .odp, .odt, .pot, .potx, .pps, .ppsx, .ppt, .pptm, .pptx, .xls, .xlsm, .xlsx, .xlt, .xltx`
- Other file extensions are rejected by the script before API call.

Response shape (relevant fields):

```json
{
  "result": [
    { "text": "chunk text", "page_number": 1 }
  ],
  "txt": "full plain text"
}
```

Fulltext extraction rule:

1. Use `txt` when non-empty.
2. Fallback to joining non-empty `result[].text` with blank lines.
3. Return fulltext only.
