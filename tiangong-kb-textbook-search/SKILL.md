---
name: tiangong-kb-textbook-search
description: "Search Tiangong knowledge-base textbook sources through the Tiangong AI CLI. Use for textbook-style explanations and reference material only. This skill searches only the textbook source, not course or edu."
---

# Tiangong KB Textbook Search

Use this skill for Tiangong textbook-source retrieval. It is intentionally
single-source: always search `textbook`, never `all`, `course`, or `edu`.

## Prerequisites

- `tiangong-ai` must be available on `PATH`, or set `TIANGONG_AI_CLI` to the CLI
  executable path.
- Set the authentication environment variables expected by `tiangong-ai`.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query:

```bash
./scripts/textbook_search.sh '{
  "query": "activated sludge process principles",
  "top_k": 5
}'
```

The script calls:

```bash
tiangong-ai education search --query <query> --sources textbook --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/textbook_search.sh '{
  "request_file": "./textbook-request.json",
  "dry_run": true
}'
```

## Raw Payload Filters

Wrapper JSON can include inline raw `textbook_search` fields; the wrapper will
forward them through the CLI `--input` path. The same payload can also be put in
`request_file` / `input_file`:

```json
{
  "query": "如何减排二氧化碳？",
  "filter": {
    "isbn_number": ["9787030641274"]
  },
  "datefilter": {
    "publication_date": { "gte": 1262304000 }
  },
  "topK": 5,
  "extK": 1
}
```

- `filter`: metadata term filters, shaped as `{ "field": ["value"] }`.
- `datefilter`: numeric range filters, shaped as
  `{ "field": { "gte"?: number, "lte"?: number } }`, for indexed numeric/date
  metadata fields.
- `topK`, `extK`: raw edge-function names for result count and adjacent chunk
  expansion.
- `getMeta` is not supported by `textbook_search`.

## Input Fields

- `query` or `input`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `filter`, `datefilter`, `topK`, `extK`: optional inline raw payload fields
  for textbook search.
- `sources`: optional compatibility field; only `textbook` or `default` is
  accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `textbook_api_key`.
- `textbook_url`, `region`, `timeout`.
- `top_k`, `ext_k`: only used in query mode.
