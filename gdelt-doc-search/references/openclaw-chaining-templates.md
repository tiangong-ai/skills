# GDELT OpenClaw Chaining Templates (Canonical)

This is the canonical chaining template reference for:

- `$gdelt-doc-search`
- `$gdelt-events-fetch`
- `$gdelt-mentions-fetch`
- `$gdelt-gkg-fetch`

## Template A: Topic Recon -> Synchronized Three-Table Pull

Use when you need topical signal discovery first and then deterministic raw table ingestion over one shared UTC window.

### Step 1: DOC API recon

```text
Use $gdelt-doc-search.
Run:
python3 scripts/gdelt_doc_search.py search \
  --query '[QUERY_EXPRESSION]' \
  --mode timelinevolraw \
  --format json \
  --start-datetime [YYYYMMDDHHMMSS] \
  --end-datetime [YYYYMMDDHHMMSS] \
  --timeline-smooth 3 \
  --output [DOC_TIMELINE_JSON] \
  --pretty
Return only JSON.
```

### Step 2: Dry-run three raw tables with same window

```text
Use $gdelt-events-fetch, $gdelt-mentions-fetch, $gdelt-gkg-fetch.
Run:
python3 scripts/gdelt_events_fetch.py fetch --mode range --start-datetime [WIN_START] --end-datetime [WIN_END] --max-files [N] --dry-run --pretty
python3 scripts/gdelt_mentions_fetch.py fetch --mode range --start-datetime [WIN_START] --end-datetime [WIN_END] --max-files [N] --dry-run --pretty
python3 scripts/gdelt_gkg_fetch.py fetch --mode range --start-datetime [WIN_START] --end-datetime [WIN_END] --max-files [N] --dry-run --pretty
Return JSON for each.
```

### Step 3: Fetch and validate

```text
Use $gdelt-events-fetch, $gdelt-mentions-fetch, $gdelt-gkg-fetch.
Run:
python3 scripts/gdelt_events_fetch.py fetch --mode range --start-datetime [WIN_START] --end-datetime [WIN_END] --max-files [N] --output-dir [EVENTS_DIR] --validate-structure --expected-columns 61 --quarantine-dir [EVENTS_QUAR] --pretty
python3 scripts/gdelt_mentions_fetch.py fetch --mode range --start-datetime [WIN_START] --end-datetime [WIN_END] --max-files [N] --output-dir [MENTIONS_DIR] --validate-structure --expected-columns 16 --quarantine-dir [MENTIONS_QUAR] --pretty
python3 scripts/gdelt_gkg_fetch.py fetch --mode range --start-datetime [WIN_START] --end-datetime [WIN_END] --max-files [N] --output-dir [GKG_DIR] --validate-structure --expected-columns 27 --quarantine-dir [GKG_QUAR] --pretty
```

### Step 4: Unified return contract

Return one final summary including:

- time window used (`WIN_START`, `WIN_END`)
- per-table `selected_count`, `downloaded_count`, `skipped_count`
- per-table validation issue counts
- output artifact paths

## Template B: Direct Three-Table Window Pull (No DOC Recon)

Use when the UTC window is already decided.

```text
Use $gdelt-events-fetch, $gdelt-mentions-fetch, $gdelt-gkg-fetch.
For each table:
1. range dry-run
2. range fetch with table-specific expected-columns
3. return per-table manifest and validation summary
```

## Column and Schema Expectations

- Events expected columns: `61`
- Mentions expected columns: `16`
- GKG expected columns: `27`

## Orchestration Rules

- Keep one shared UTC window across all three table pulls.
- Keep each skill atomic. Do not add scheduler/polling loops inside skills.
- If periodic execution is required, schedule repeated invocations externally in OpenClaw.
