---
name: tiangong-kb-sci-search
description: Perform one isolated owner-requested SCI search outside Tiangong Auto Research, or provide SCI method guidance when Auto Research discovery invokes the locked broker capability. Searches only `sci`. If an ancestor contains `.tiangong-research` and the request is open-ended, multi-source, analytical, builds on prior outputs, or produces a conclusion/artifact, route to `tiangong-auto-research` and do not execute this standalone wrapper. Inside a managed workspace, standalone use requires the user's explicit isolated-query request and `execution_mode=standalone`.
---

# Tiangong KB SCI Search

This Skill is deliberately single-source. Search `sci` only; never broaden to
`all`, `report`, or `patent`. Access requires the deployment owner's normal key
and entitlement. Stop on authentication, authorization, subscription, VPN, or
service failure; do not try to bypass it.

## Choose the execution mode

- For a direct standalone search outside a managed workspace, use
  `scripts/sci_search.sh` as described below. Inside a managed workspace, only
  an explicit user-requested isolated query may set
  `"execution_mode":"standalone"`; the wrapper records a non-secret mode event.
- Inside a Tiangong Auto Research discovery package, do **not** execute this
  script or its CLI/curl examples. Use locked capability ID
  `database.tiangong.sci-search` through `fetch_candidate_source`. The broker
  owns the exact POST method, endpoint, region header, credential injection,
  response bounds, sanitization, and permanent evidence receipt.

## Direct-search prerequisites

- The standalone wrapper defaults to its separately tested CLI version
  `0.0.62`. This is not the Auto Research workspace runtime. Managed research
  resolves its exact version from the immutable setup plan or
  `runtime-lock.json` and must use the broker.
  Set `TIANGONG_AI_CLI_BIN` to one exact executable path, or
  `TIANGONG_AI_CLI` to an explicitly reviewed command, only when overriding the
  standalone entrypoint intentionally.
- Put the source-specific key in `TIANGONG_SCI_APIKEY`, or use
  `TIANGONG_AI_APIKEY` as the common fallback. Credentials are never accepted in
  wrapper JSON, a request file, or CLI arguments.
- `jq` is required by the wrapper.
- Optional endpoint variables are `TIANGONG_SCI_SEARCH_URL`,
  `TIANGONG_RESEARCH_API_BASE_URL`, `TIANGONG_AI_SEARCH_API_BASE_URL`, or
  `TIANGONG_AI_API_BASE_URL`; `TIANGONG_REGION` and
  `TIANGONG_RESEARCH_TIMEOUT` are also supported.

## Direct query

```bash
export TIANGONG_SCI_APIKEY='owner-authorized key'
./scripts/sci_search.sh '{
  "query": "mechanical recycling reduces lifecycle emissions",
  "top_k": 5,
  "get_meta": true
}'
```

The wrapper invokes the pinned CLI equivalent of:

```bash
npx --yes --package "@tiangong-ai/cli@0.0.62" -- tiangong-ai research search \
  --query <query> --sources sci --json
```

To write a result, pass one explicit output path as the second argument. The
wrapper writes a unique temporary file and renames it only after the CLI exits
successfully; it never reports success based on file existence alone.

## Exact request payload

Inline supported edge-function fields are converted to a temporary request
file:

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

Or reference an existing regular non-symlink JSON file:

```bash
./scripts/sci_search.sh '{
  "request_file": "/absolute/path/to/sci-request.json",
  "dry_run": true
}'
```

`filter` accepts metadata term arrays. `datefilter` accepts numeric `gte`/`lte`
ranges. `topK`, `extK`, and `getMeta` are the raw service names; `top_k`,
`ext_k`, and `get_meta` are convenience query-mode names.

## Optional dotenv loading

An explicit `env_file` must exist, be a regular non-symlink file, and have
owner-only permissions (`chmod 600`). If `request_file` is supplied without an
explicit env file, a same-directory `.env` is loaded only when it exists and
passes the same checks. Existing process variables win.

The parser does not execute shell syntax and loads only documented Tiangong
key, endpoint, region, and timeout names. All other keys are ignored. Never put
an agent key, browser cookie, Authorization header, session token, proxy
password, or unrelated secret in this file.

## Accepted wrapper fields

- `query`, `input`, or `claim`;
- `request_file` or `input_file`;
- `env_file`;
- `execution_mode`, only `standalone` and only after an explicit isolated-query
  request when a managed workspace is detected;
- `filter`, `datefilter`, `topK`, `extK`, `getMeta`;
- `top_k`, `ext_k`, `get_meta` in query mode;
- `sources`, only `sci` or `default`;
- `api_base_url`, `sci_url`, `region`, `timeout` as non-secret routing values;
- `dry_run`.

Any key that looks like an API key, token, Authorization, Cookie, password,
credential, secret, or session is rejected recursively before the CLI starts.
The wrapper does not echo rejected values or failed command stdout.

## Auto Research broker call

After `research setup` has selected and configured this Skill, discovery uses:

```json
{
  "capability_id": "database.tiangong.sci-search",
  "url": "<the exact manifest endpoint>",
  "request_body": {
    "query": "research claim",
    "topK": 5,
    "getMeta": true
  }
}
```

Use only documented non-secret request fields. Never add the API key, headers,
cookies, endpoint override, or token to `request_body`. The broker injects the
logical `tiangong.sci.api-key` as `x-api-key`, sends the locked `x-region`,
refuses POST redirects, and records only the request-body SHA-256 outside the
permanent raw response object. This database supplements but does not replace
Auto Research's independent public-internet evidence requirement.
