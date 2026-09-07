---
name: gdelt-doc-search
description: Search GDELT DOC 2.0 through the Tiangong CLI for bounded article metadata, timeline aggregates, or tone-distribution bins using linted GDELT query syntax and optional split domain filters. Use for news-source discovery and trend reconnaissance; do not use for article bodies, raw GDELT event/GKG/mention rows, representative media measurement, fact verification, sentiment ground truth, or causal inference.
---

# GDELT DOC Search

Use the CLI-owned `gdelt.doc-search` capability. This Skill supplies intent
routing and result-use boundaries only; the CLI owns source discovery,
input/output schemas, HTTP behavior, limits, validation, and receipts.

## Before running

1. Read `references/tiangong-data-requirement.json`.
2. Use the caller- or workspace-resolved stable CLI. The requirement declares
   compatible capability and operation contract majors; it does not select a
   package build.
3. Run `data describe` with that same CLI. Continue only when the capability
   ID and required contract majors match, and copy the exact current
   capability/operation versions from that response into the run request.

```bash
tiangong-ai data describe gdelt.doc-search --json
```

Use the returned Discovery Metadata to confirm current source coverage,
freshness, restrictions, `provides`, and `doesNotProvide`. Do not substitute
facts remembered from an older Skill revision.

## Prepare the request

Build a `tiangong.data.run-request.v1` envelope. Replace the two version
placeholders with the exact versions from the same `data describe` response. This example requests an
article list for one bounded absolute UTC window:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "gdelt.doc-search",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "search",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "query": "(\"climate change\" OR pollution) sourcecountry:us",
    "mode": "artlist",
    "absoluteWindow": {
      "from": "2026-03-01T00:00:00Z",
      "to": "2026-03-07T23:59:59Z"
    },
    "maxRecords": 75,
    "sort": "hybridrel"
  }
}
```

Use the operation input schema returned by the same `data describe` response to select a
supported mode and its mode-specific fields. Use at most one time-window form;
omitting both uses the provider default window. Never silently widen a supplied
window or pass arbitrary DOC parameters absent from the schema.

The CLI lints every effective query before network access. Wrap boolean `OR`
groups in parentheses and use `exactDomains` (`domainis:`) or `domains`
(`domain:`), never `site:` or `inurl:`. Repeated domains become separate
bounded batches. Set `continueOnQueryError` only when a partial merged result is
useful and retain `batchQueries` plus `queryErrors`.

## Run

```bash
tiangong-ai data run gdelt.doc-search search \
  --input /absolute/path/to/request.json --json
```

The command emits a `tiangong.data.run-result.v1` envelope. Preserve its
`contract`, `warnings`, `errors`, and `receipt` with `data` when handing the
result to another workflow.

## Result boundaries

- Persistent DOC throttling is unavailable retrieval, not zero coverage. When
  literal topic discovery fits the task, consider the separately described
  `gdelt-web-ngrams-search` Skill as an explicit change of retrieval method.
  It does not support DOC operators or reproduce DOC aggregate timelines.
- Treat article-list results as source metadata and links, not downloaded
  article bodies, verified claims, or independent evidence units.
- Treat timelines, tone, language, and source-country outputs as automated
  aggregates whose meaning depends on the selected mode; do not compare unlike
  measures or infer causality from them.
- In `tonechart`, `articleCount` is the count assigned to a tone bin, not the
  tone score itself and not a public-sentiment estimate.
- GDELT source coverage and automated extraction can be uneven. Do not treat
  counts, tone, or rankings as representative measurements of public opinion,
  media prevalence, event truth, or sentiment ground truth.
- Surface `partial`, truncation warnings, and empty results. Never reinterpret
  them as complete absence of coverage.
- Use a dedicated GDELT Events, GKG, or Mentions Skill when structured feed rows
  are required: GKG supplies document annotations, Events supplies coded events,
  and Mentions links events to documents. This Skill must not invoke or combine
  them automatically, and their statistics are not interchangeable with DOC.
- Cross-source comparison, full-text acquisition, persistence, polling, and
  research evidence admission belong to the caller or Auto Research.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
