---
name: tiangong-kb-patent-search
description: "Search Tiangong knowledge-base patent sources through the Tiangong AI CLI. Use for inventions, technical routes, claims, patent evidence, and prior art. This skill searches only the patent source, not sci or report."
---

# Tiangong KB Patent Search

Use this skill for Tiangong patent-source retrieval. It is intentionally
single-source: always search `patent`, never `all`, `sci`, or `report`.

## Prerequisites

- The wrapper defaults to `npx @tiangong-ai/cli@0.0.19`; users do not need a
  preinstalled CLI. Set `TIANGONG_AI_CLI` or `TIANGONG_AI_CLI_BIN` only to
  override the CLI entrypoint.
- Set the authentication environment variables expected by `tiangong-ai`, or
  pass `api_key` / `patent_api_key` to the wrapper script.
- When `request_file` / `input_file` is provided, the wrapper loads `.env` from
  that file's directory by default. `env_file` can point to a different dotenv
  file. Loaded dotenv values only fill unset environment variables; explicit
  JSON fields such as `api_key`, `patent_api_key`, and `api_base_url` are passed
  as CLI flags and take precedence.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query:

```bash
./scripts/patent_search.sh '{
  "query": "mechanical recycling polymer purification patent",
  "top_k": 5
}'
```

The script calls:

```bash
npx @tiangong-ai/cli@0.0.19 research search --query <query> --sources patent --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/patent_search.sh '{
  "request_file": "./patent-request.json",
  "dry_run": true
}'
```

## Raw Payload Filters

Wrapper JSON can include inline raw `patent_search` fields; the wrapper will
forward them through the CLI `--input` path. The same payload can also be put in
`request_file` / `input_file`:

```json
{
  "query": "battery recycling equipment",
  "filter": {
    "assignee": ["example company"]
  },
  "datefilter": {
    "publication_date": { "gte": 1262304000 }
  },
  "topK": 5
}
```

- `filter`: metadata term filters, shaped as `{ "field": ["value"] }`.
- `datefilter`: numeric range filters, shaped as
  `{ "field": { "gte"?: number, "lte"?: number } }`, for indexed numeric/date
  metadata fields.
- `topK`: raw edge-function name for result count.
- `extK` and `getMeta` are not supported by `patent_search`.

## Input Fields

- `query`, `input`, or `claim`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `env_file`: optional dotenv file. Without it, `request_file` /
  `input_file` causes the wrapper to load `.env` from that file's directory.
- `filter`, `datefilter`, `topK`: optional inline raw payload fields for patent
  search.
- `sources`: optional compatibility field; only `patent` or `default` is
  accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `patent_api_key`.
- `patent_url`, `region`, `timeout`.
- `top_k`: only used in query mode.
