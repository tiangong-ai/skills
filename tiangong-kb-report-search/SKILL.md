---
name: tiangong-kb-report-search
description: Perform one isolated owner-requested Tiangong report search outside Auto Research, or provide report-source method guidance for a separately reviewed locked broker capability. Searches only `report`. If an ancestor contains `.tiangong-research` and the request is open-ended, multi-source, analytical, builds on prior outputs, or produces a conclusion/artifact, route to `tiangong-auto-research` and do not execute this standalone wrapper. Inside a managed workspace, standalone use requires the user's explicit isolated-query request and `execution_mode=standalone`.
---

# Tiangong KB Report Search

Use this skill for Tiangong report-source retrieval. It is intentionally
single-source: always search `report`, never `all`, `sci`, or `patent`.

In Auto Research, an industry-report requirement must name the reviewed report
capability in `requiredCapabilityIds`; Brave or SCI results cannot stand in for
it. If that capability is absent, stop at preflight and have the owner import,
configure, license, credential, lock, and smoke the exact report capability.
The standalone wrapper never reads the workspace broker credential store.

## Prerequisites

- The standalone wrapper defaults to its separately tested CLI version
  `0.0.62`; this is not the Auto Research workspace runtime. Set
  `TIANGONG_AI_CLI` or `TIANGONG_AI_CLI_BIN` only to intentionally override the
  standalone entrypoint.
- Set `TIANGONG_REPORT_APIKEY` or the common fallback
  `TIANGONG_AI_APIKEY`. Credentials are rejected recursively in wrapper JSON,
  request files, URL query parameters, and CLI arguments.
- When `request_file` / `input_file` is provided, the wrapper loads `.env` from
  that file's directory by default. `env_file` can point to a different dotenv
  file. It must be owner-only, regular, and non-symlinked; only documented
  Tiangong report credential, endpoint, region, and timeout variables are
  loaded, and existing environment values win.
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

When the current directory is inside a managed workspace, this command is
blocked before credential loading or network. Only a user who explicitly asks
for one isolated query may add `"execution_mode":"standalone"`; the wrapper
then emits a non-secret audit event.

The script calls:

```bash
npx --yes --package "@tiangong-ai/cli@0.0.62" -- tiangong-ai research search --query <query> --sources report --json
```

For exact edge-function payloads, provide `request_file` or `input_file`:

```bash
./scripts/report_search.sh '{
  "request_file": "./report-request.json",
  "dry_run": true
}'
```

## Raw Payload Filters

Wrapper JSON can include inline raw `report_search` fields; the wrapper will
forward them through the CLI `--input` path. The same payload can also be put in
`request_file` / `input_file`:

```json
{
  "query": "mechanical recycling policy report",
  "filter": {
    "source": ["IEA"]
  },
  "topK": 5,
  "extK": 2
}
```

- `filter`: metadata term filters, shaped as `{ "field": ["value"] }`.
- `topK`, `extK`: raw edge-function names for result count and adjacent chunk
  expansion.
- `datefilter` and `getMeta` are not supported by `report_search`.

## Input Fields

- `query`, `input`, or `claim`: convenience query text.
- `request_file` or `input_file`: JSON body forwarded unchanged.
- `env_file`: optional dotenv file. Without it, `request_file` /
  `input_file` causes the wrapper to load `.env` from that file's directory.
- `execution_mode`: only `standalone`, and only after an explicit isolated
  owner request when a managed workspace is detected.
- `filter`, `topK`, `extK`: optional inline raw payload fields for report
  search.
- `sources`: optional compatibility field; only `report` or `default` is
  accepted.
- `dry_run`: true to return the exact request plan with masked credentials.
- `api_base_url`; credentials must come from allowed owner environment
  variables or an owner-only env file.
- `report_url`, `region`, `timeout`.
- `top_k`, `ext_k`: only used in query mode.
