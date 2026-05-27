---
name: tiangong-kb-research-search
description: "Search Tiangong knowledge-base research sources through the Tiangong AI CLI across SCI, report, and patent sources. Use when finding evidence, publications, reports, patents, or technical material around a claim or viewpoint."
---

# Tiangong KB Research Search

Use `tiangong-ai research search` as the entrypoint. The CLI owns source
selection, endpoint derivation, authentication headers, request forwarding, and
dry-run request plans. The skill should decide search intent and source scope;
it should not call edge functions directly for normal research search.

## Source Selection

- Default source is `sci`.
- Use `--sources all` when the user wants broad research coverage across
  papers, reports, and patents.
- Use `sci` for academic papers and scientific journal evidence.
- Use `report` for industry, policy, whitepaper, and institutional reports.
- Use `patent` for inventions, technical routes, claims, and prior art.
- Accept comma-separated source lists such as `sci,patent` when the request is
  narrower than `all`.

## Prerequisites

- `tiangong-ai` must be available on `PATH`, or set `TIANGONG_AI_CLI` to the CLI
  executable path.
- Set `TIANGONG_AI_APIKEY`, or pass `api_key` / source-specific API keys to the
  wrapper script.
- Optionally set `TIANGONG_AI_API_BASE_URL`; the CLI accepts a Supabase project
  root, `/functions/v1`, or `/rest/v1` and derives Functions URLs.

## Search

For normal searches, pass a query and source scope:

```bash
./scripts/research_search.sh '{
  "query": "mechanical recycling reduces lifecycle emissions",
  "sources": "all",
  "top_k": 5,
  "get_meta": true
}'
```

The script calls:

```bash
tiangong-ai research search --query <query> --sources <csv> --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/research_search.sh '{
  "request_file": "./sci-request.json",
  "sources": "sci,patent",
  "dry_run": true
}'
```

The script calls:

```bash
tiangong-ai research search --input <request.json> --sources <csv> --json
```

## Common Options

- `query`, `input`, or `claim`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `sources`: array or comma-separated string, default `default`; presets are
  `default` and `all`.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`, `api_key`, `sci_api_key`, `report_api_key`, `patent_api_key`.
- `sci_url`, `report_url`, `patent_url`, `region`, `timeout`.
- `top_k`, `ext_k`, `get_meta`: only used in query mode.
