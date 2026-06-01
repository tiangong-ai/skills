---
name: tiangong-kb-sci-search
description: "Search Tiangong knowledge-base SCI sources through the Tiangong AI CLI. Use for academic papers, scientific journal evidence, literature support, and research claims. This skill searches only the sci source, not report or patent."
---

# Tiangong KB SCI Search

Use this skill for Tiangong SCI-source retrieval. It is intentionally
single-source: always search `sci`, never `all`, `report`, or `patent`.

## Prerequisites

- The wrapper defaults to `npx @tiangong-ai/cli@latest`; users do not need a
  preinstalled CLI. Set `TIANGONG_AI_CLI` or `TIANGONG_AI_CLI_BIN` only to
  override the CLI entrypoint.
- Set the authentication environment variables expected by `tiangong-ai`, or
  pass `api_key` / `sci_api_key` to the wrapper script.
- When `request_file` / `input_file` is provided, the wrapper loads `.env` from
  that file's directory by default. `env_file` can point to a different dotenv
  file. Loaded dotenv values only fill unset environment variables; explicit
  JSON fields such as `api_key`, `sci_api_key`, and `api_base_url` are passed as
  CLI flags and take precedence.
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
npx @tiangong-ai/cli@latest research search --query <query> --sources sci --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/sci_search.sh '{
  "request_file": "./sci-request.json",
  "dry_run": true
}'
```

## Raw Payload Filters

Wrapper JSON can include inline raw `sci_search` fields; the wrapper will
forward them through the CLI `--input` path. The same payload can also be put in
`request_file` / `input_file`:

```json
{
  "query": "critical metal material flows",
  "filter": {
    "journal": ["JOURNAL OF INDUSTRIAL ECOLOGY"]
  },
  "datefilter": {
    "date": { "gte": 1262304000 }
  },
  "topK": 5,
  "extK": 2,
  "getMeta": true
}
```

- `filter`: metadata term filters, shaped as `{ "field": ["value"] }`.
- `datefilter`: numeric range filters, shaped as
  `{ "field": { "gte"?: number, "lte"?: number } }`, for indexed numeric/date
  metadata fields.
- `getMeta`: when true, returns paper metadata for matched DOI records.
- `topK`, `extK`: raw edge-function names for result count and adjacent chunk
  expansion.

## Input Fields

- `query`, `input`, or `claim`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `env_file`: optional dotenv file. Without it, `request_file` /
  `input_file` causes the wrapper to load `.env` from that file's directory.
- `filter`, `datefilter`, `topK`, `extK`, `getMeta`: optional inline raw
  payload fields for SCI search.
- `sources`: optional compatibility field; only `sci` or `default` is accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `sci_api_key`.
- `sci_url`, `region`, `timeout`.
- `top_k`, `ext_k`, `get_meta`: only used in query mode.
