---
name: tiangong-kb-esg-search
description: "Search Tiangong knowledge-base ESG disclosures through the Tiangong AI CLI. Use for environmental, social, and governance reports and disclosure evidence only. This skill searches only the ESG source."
---

# Tiangong KB ESG Search

Use this skill for Tiangong ESG disclosure retrieval. It is intentionally
single-source: always search the `esg_search` endpoint, never a broad or
multi-source preset.

## Prerequisites

- The wrapper defaults to the exact reviewed entrypoint
  `npx --yes --package "@tiangong-ai/cli@0.0.62" -- tiangong-ai`; users do not need a
  preinstalled CLI. Set `TIANGONG_AI_CLI` or `TIANGONG_AI_CLI_BIN` only to
  override the CLI entrypoint intentionally.
- Set `TIANGONG_ESG_APIKEY` or `TIANGONG_AI_APIKEY`. Credentials are never
  accepted in wrapper JSON, request files, URLs, or CLI arguments.
- When `request_file` / `input_file` is provided, the wrapper loads `.env` from
  that file's directory by default. `env_file` can point to a different dotenv
  file. It must be an owner-only regular non-symlink file (`chmod 600`), and
  only documented Tiangong variables are loaded. Existing process variables win.
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
npx --yes --package "@tiangong-ai/cli@0.0.62" -- tiangong-ai research search --sources esg --query <query> --json
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
- `api_base_url`, `esg_url`, `region`, `timeout` as non-secret routing values.
- `top_k`, `ext_k`: only used in query mode.
