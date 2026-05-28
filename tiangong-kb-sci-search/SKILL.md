---
name: tiangong-kb-sci-search
description: "Search Tiangong knowledge-base SCI sources through the Tiangong AI CLI. Use for academic papers, scientific journal evidence, literature support, and research claims. This skill searches only the sci source, not report or patent."
---

# Tiangong KB SCI Search

Use this skill for Tiangong SCI-source retrieval. It is intentionally
single-source: always search `sci`, never `all`, `report`, or `patent`.

## Prerequisites

- `tiangong-ai` must be available on `PATH`, or set `TIANGONG_AI_CLI` to the CLI
  executable path.
- Set the authentication environment variables expected by `tiangong-ai`, or
  pass `api_key` / `sci_api_key` to the wrapper script.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query:

```bash
./scripts/sci_search.sh '{
  "query": "mechanical recycling reduces lifecycle emissions",
  "top_k": 5,
  "get_meta": true
}'
```

The script calls:

```bash
tiangong-ai research search --query <query> --sources sci --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/sci_search.sh '{
  "request_file": "./sci-request.json",
  "dry_run": true
}'
```

## Input Fields

- `query`, `input`, or `claim`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `sources`: optional compatibility field; only `sci` or `default` is accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `sci_api_key`.
- `sci_url`, `region`, `timeout`.
- `top_k`, `ext_k`, `get_meta`: only used in query mode.
