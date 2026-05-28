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

## Input Fields

- `query` or `input`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `sources`: optional compatibility field; only `textbook` or `default` is
  accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `textbook_api_key`.
- `textbook_url`, `region`, `timeout`.
- `top_k`, `ext_k`: only used in query mode.
