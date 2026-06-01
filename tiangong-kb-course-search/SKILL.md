---
name: tiangong-kb-course-search
description: "Search Tiangong knowledge-base course sources through the Tiangong AI CLI. Use for courseware, lessons, teaching material, Qinghua/Tsinghua humanities archive course collections, and evidence-grounded education-history retrieval. This skill searches only the course source."
---

# Tiangong KB Course Search

Use this skill for Tiangong course-source retrieval. It is intentionally
single-source: always search `course`, never `all`, `edu`, or `textbook`.

## Prerequisites

- The wrapper defaults to `npx @tiangong-ai/cli@latest`; users do not need a
  preinstalled CLI. Set `TIANGONG_AI_CLI` or `TIANGONG_AI_CLI_BIN` only to
  override the CLI entrypoint.
- Course search should use bearer auth. Pass `bearer_token`; if only `api_key`
  is provided to this wrapper, it is also forwarded as `--bearer-token` so the
  CLI emits `Authorization: Bearer ...` for the course source.
- When `request_file` / `input_file` is provided, the wrapper loads `.env` from
  that file's directory by default. `env_file` can point to a different dotenv
  file. Loaded dotenv values only fill unset environment variables; explicit
  JSON fields such as `api_key`, `bearer_token`, and `api_base_url` are passed
  as CLI flags and take precedence.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query:

```bash
./scripts/course_search.sh '{
  "query": "马约翰 清华 体育 孙立人 梁实秋 钱三强",
  "top_k": 8,
  "ext_k": 20
}'
```

The script calls:

```bash
npx @tiangong-ai/cli@latest education search --query <query> --sources course --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/course_search.sh '{
  "request_file": "./course-request.json",
  "dry_run": true
}'
```

The script calls:

```bash
npx @tiangong-ai/cli@latest education search --input <request.json> --sources course --json
```

## Raw Payload Filters

Wrapper JSON can include inline raw `course_search` fields; the wrapper will
forward them through the CLI `--input` path. The same payload can also be put in
`request_file` / `input_file`:

```json
{
  "query": "马约翰 清华 体育",
  "filter": {
    "tags": ["thu_humanities"]
  },
  "topK": 8,
  "extK": 20
}
```

- `filter`: metadata term filters, shaped as `{ "field": ["value"] }`.
  Course collections currently use metadata keys such as `tags` and
  `raw_relative_path` when those keys are indexed for the collection.
- `topK`, `extK`: raw edge-function names for result count and adjacent chunk
  expansion.
- `datefilter` is not exposed by this wrapper for course search.

## Input Fields

- `query` or `input`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `env_file`: optional dotenv file. Without it, `request_file` /
  `input_file` causes the wrapper to load `.env` from that file's directory.
- `filter`, `topK`, `extK`: optional inline raw payload fields for course
  search.
- `sources`: optional compatibility field; only `course` or `default` is
  accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `bearer_token`, `course_api_key`. For course
  search, prefer `bearer_token`; `api_key` is treated as the same token when
  `bearer_token` is omitted.
- `course_url`, `region`, `timeout`.
- `top_k`, `ext_k`: only used in query mode.

When writing reader-facing prose from retrieved results, state historical facts
directly and cite source titles when useful. Do not mention retrieval mechanics,
source grouping, or internal confidence checks.
