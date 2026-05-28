---
name: tiangong-kb-edu-search
description: "Search Tiangong knowledge-base general education sources through the Tiangong AI CLI. Use for broad education knowledge sources only. This skill searches only the edu source, not course or textbook."
---

# Tiangong KB Edu Search

Use this skill for Tiangong general education-source retrieval. It is
intentionally single-source: always search `edu`, never `all`, `course`, or
`textbook`.

## Prerequisites

- `tiangong-ai` must be available on `PATH`, or set `TIANGONG_AI_CLI` to the CLI
  executable path.
- Set the authentication environment variables expected by `tiangong-ai`.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query:

```bash
./scripts/edu_search.sh '{
  "query": "education policy curriculum reform",
  "top_k": 5
}'
```

The script calls:

```bash
tiangong-ai education search --query <query> --sources edu --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/edu_search.sh '{
  "request_file": "./edu-request.json",
  "dry_run": true
}'
```

## Raw Payload Filters

Wrapper JSON can include inline raw `edu_search` fields; the wrapper will
forward them through the CLI `--input` path. The same payload can also be put in
`request_file` / `input_file`:

```json
{
  "query": "water treatment filter backwashing",
  "filter": {
    "course": ["水处理工程"]
  },
  "topK": 5,
  "extK": 1
}
```

- Normal search currently uses only `filter.course` to narrow retrieval to one
  or more course names; other metadata fields are not applied in the retrieval
  path.
- `edu_search` has a separate `{"action":"list_filter_options"}` mode for
  discovering metadata values. In that mode, `fields` may include `course`,
  `type`, `file_type`, `language`, `chapter_number`, and `name`; optional
  `filter` narrows the listed options.
- `topK`, `extK`: raw edge-function names for result count and adjacent chunk
  expansion.
- `datefilter` and `getMeta` are not supported by `edu_search`.

## Input Fields

- `query` or `input`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `filter`, `action`, `fields`, `topK`, `extK`: optional inline raw payload
  fields for `edu_search`.
- `sources`: optional compatibility field; only `edu` or `default` is accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `edu_api_key`.
- `edu_url`, `region`, `timeout`.
- `top_k`, `ext_k`: only used in query mode.
