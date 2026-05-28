---
name: tiangong-kb-report-search
description: "Search Tiangong knowledge-base report sources through the Tiangong AI CLI. Use for industry reports, policy reports, whitepapers, institutional reports, and technical reports. This skill searches only the report source, not sci or patent."
---

# Tiangong KB Report Search

Use this skill for Tiangong report-source retrieval. It is intentionally
single-source: always search `report`, never `all`, `sci`, or `patent`.

## Prerequisites

- `tiangong-ai` must be available on `PATH`, or set `TIANGONG_AI_CLI` to the CLI
  executable path.
- Set the authentication environment variables expected by `tiangong-ai`, or
  pass `api_key` / `report_api_key` to the wrapper script.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query:

```bash
./scripts/report_search.sh '{
  "query": "mechanical recycling policy report",
  "top_k": 5
}'
```

The script calls:

```bash
tiangong-ai research search --query <query> --sources report --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/report_search.sh '{
  "request_file": "./report-request.json",
  "dry_run": true
}'
```

## Input Fields

- `query`, `input`, or `claim`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `sources`: optional compatibility field; only `report` or `default` is
  accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `report_api_key`.
- `report_url`, `region`, `timeout`.
- `top_k`, `ext_k`, `get_meta`: only used in query mode.
