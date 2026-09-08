---
name: gdelt-web-ngrams-search
description: Find news article links containing literal words or short phrases in an explicit GDELT Web NGrams/TOC minute pair through the Tiangong CLI. Use for file-based topic discovery, including an explicitly selected alternative when DOC search is unavailable. Do not use for DOC query operators, sentiment timelines, full article text, relevance ranking, or complete time-range coverage from a single minute.
---

# GDELT Web NGrams Search

Use CLI-owned `gdelt.web-ngrams`; this Skill selects and explains the operation,
without a second download, search, or join implementation.

## Before running

1. Read `references/tiangong-data-requirement.json`.
2. Use the caller- or workspace-resolved stable CLI for both describe and run.
   Confirm that the capability and required operation contract majors exist.
   If unavailable, report the missing capability; do not assume every package
   with the same CLI version contains an unreleased candidate extension.
3. Read current Discovery Metadata and input/output schemas:

```bash
tiangong-ai data describe gdelt.web-ngrams --json
```

## Prepare and run

Select one explicit published UTC minute and literal phrases accepted by the
current input schema. Replace the version placeholders from that same describe
response. This historical minute is a documented sample, not a current-news
default:

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "gdelt.web-ngrams",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "search",
  "operationVersion": "<describe.manifest.operations[0].operationVersion>",
  "input": {
    "fileTimestamp": "2026-06-30T20:16:00Z",
    "phrases": ["disease", "diseases"],
    "match": "any"
  }
}
```

```bash
tiangong-ai data run gdelt.web-ngrams search \
  --input /absolute/path/to/request.json --json
```

Keep `data`, `contract`, `summary`, `warnings`, `errors`, and `receipt` together.

## Selection and interpretation

- Choose this operation for literal topic-to-article discovery. Switching from
  DOC is an explicit change of retrieval method, not an equivalent retry:
  do not translate DOC operators into phrases or invent DOC timelines.
- Choose GKG for extracted themes/entities/document tone, Events for coded
  events, and Mentions for event-to-document linkage. Use only the layers the
  question needs; this Skill does not automatically invoke or join them.
- A successful call covers only the requested file pair, not a complete day or
  all monitored news. The caller owns range enumeration and must retain missing
  minutes. Do not present a handful of available minutes as full-window coverage.
- Distinguish successful zero matches from blocked file acquisition and partial
  validation. For truncation, report returned versus matched versus omitted
  records and resolve the limit before claiming exhaustive retrieval.
- Identify documents by `(fileTimestamp, documentId)`. Matched phrases indicate
  presence, not counts: overlapping quadgrams must not be summed into phrase
  frequencies. Article links and optional unvalidated image-reference text are
  provider metadata, not downloaded or verified content.
- Full-text retrieval, longitudinal aggregation, persistence, and research
  evidence admission remain with the caller or Auto Research.
