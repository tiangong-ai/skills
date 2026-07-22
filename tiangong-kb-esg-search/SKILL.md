---
name: tiangong-kb-esg-search
description: "Search Tiangong knowledge-base ESG disclosures through the Tiangong AI CLI. Use for environmental, social, and governance reports and disclosure evidence only. This skill searches only the ESG source."
---

# Tiangong KB ESG Search

Use this skill for Tiangong ESG disclosure retrieval. It is intentionally
single-source: always search the `esg_search` endpoint, never a broad or
multi-source preset.

## Prerequisites

- The wrapper defaults to `npx @tiangong-ai/cli@latest`; users do not need a
  preinstalled CLI. Set `TIANGONG_AI_CLI` or `TIANGONG_AI_CLI_BIN` only to
  override the CLI entrypoint. Native ESG search requires
  `@tiangong-ai/cli@0.0.19` or later.
- Set `TIANGONG_ESG_APIKEY` or `TIANGONG_AI_APIKEY`. The explicit JSON field
  `esg_api_key` takes precedence over `api_key`, and both take precedence over
  environment credentials.
- When `request_file` / `input_file` is provided, the wrapper loads `.env` from
  that file's directory by default. `env_file` can point to a different dotenv
  file. Loaded dotenv values only fill unset environment variables.
- Optionally set `TIANGONG_ESG_SEARCH_URL`. The CLI otherwise derives the
  `esg_search` endpoint from `TIANGONG_RESEARCH_API_BASE_URL`,
  `TIANGONG_AI_SEARCH_API_BASE_URL`, or `TIANGONG_AI_API_BASE_URL`. The wrapper
  also maps `TIANGONG_ESG_API_BASE_URL` to the CLI `--api-base-url` option.

## Search

For normal searches, pass a query:

```bash
./scripts/esg_search.sh '{
  "query": "scope 3 emissions reduction targets",
  "top_k": 5
}'
```

The script calls:

```bash
npx @tiangong-ai/cli@latest research search --sources esg --query <query> --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/esg_search.sh '{
  "request_file": "./esg-request.json",
  "dry_run": true
}'
```

## Raw Payload Filters

Wrapper JSON can include inline raw `esg_search` fields; the wrapper forwards
them through the CLI `--input` path:

```json
{
  "query": "greenhouse gas emissions",
  "filter": {
    "country": ["China"]
  },
  "datefilter": {
    "publication_date": {
      "gte": 1672531200
    }
  },
  "meta_contains": "annual sustainability report",
  "topK": 5,
  "extK": 1
}
```

- `filter.<field>`: accept a string array for an indexed ESG metadata field.
  The current public contract explicitly documents `rec_id` and `country`.
- `datefilter.<field>`: accept `gte` and/or `lte` numeric bounds. The current
  ESG date field is `publication_date`, expressed as a UNIX timestamp.
- `meta_contains`: fuzzy-match ESG metadata. Use it only when the user
  explicitly requests metadata-based narrowing.
- `topK`, `extK`: raw edge-function names for result count and adjacent chunk
  expansion.
- Inline `metaContains` and `dateFilter` are accepted as convenience aliases
  and normalized to the edge-function field names.
- Inline payloads enforce the edge function's dynamic filter shapes: term
  filters must contain string arrays and range filters must contain numeric
  `gte` / `lte` bounds. Exact payload files are forwarded unchanged, so keep
  their field names in the edge-function form shown above.

## Input Fields

- `query` or `input`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `env_file`: optional dotenv file. Without it, `request_file` /
  `input_file` causes the wrapper to load `.env` from that file's directory.
- `filter`, `datefilter`, `dateFilter`, `meta_contains`, `metaContains`,
  `topK`, `extK`: optional inline raw payload fields for `esg_search`.
- `sources`: optional compatibility field; only `esg` or `default` is accepted.
- `dry_run`: return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `esg_api_key`, `esg_url`, `region`, `timeout`.
- `top_k`, `ext_k`: only used in query mode.
