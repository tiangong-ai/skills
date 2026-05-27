---
name: tiangong-kb-education-search
description: "Search Tiangong knowledge-base education sources through the Tiangong AI CLI across course, edu, and textbook sources. Use when finding lessons, course material, textbooks, or education-oriented explanations."
---

# Tiangong KB Education Search

Use `tiangong-ai education search` as the entrypoint. The CLI owns source
selection, endpoint derivation, authentication headers, request forwarding, and
dry-run request plans. The skill should choose education source scope and shape
the query; it should not call edge functions directly for normal education
search.

## Source Selection

- Default source is `course`.
- Use `--sources all` when the user wants broad education coverage.
- Use `course` for courseware, lessons, and teaching material.
- Use `textbook` for textbook-style explanations and reference material.
- Use `edu` for general education knowledge sources.
- Accept comma-separated source lists such as `course,textbook`.

## Prerequisites

- `tiangong-ai` must be available on `PATH`, or set `TIANGONG_AI_CLI` to the CLI
  executable path.
- Course search can use `TIANGONG_EDUCATION_BEARER_TOKEN` or a bearer token
  passed to the wrapper script.
- All education sources can use `TIANGONG_AI_APIKEY`, `api_key`, or
  source-specific API keys.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query and source scope:

```bash
./scripts/education_search.sh '{
  "query": "activated sludge process principles",
  "sources": "course",
  "top_k": 5
}'
```

The script calls:

```bash
tiangong-ai education search --query <query> --sources <csv> --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/education_search.sh '{
  "request_file": "./course-request.json",
  "sources": "all",
  "dry_run": true
}'
```

The script calls:

```bash
tiangong-ai education search --input <request.json> --sources <csv> --json
```

## Common Options

- `query` or `input`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `sources`: array or comma-separated string, default `default`; presets are
  `default` and `all`.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `bearer_token`, `course_api_key`, `edu_api_key`,
  `textbook_api_key`.
- `course_url`, `edu_url`, `textbook_url`, `region`, `timeout`.
- `top_k`, `ext_k`: only used in query mode.
