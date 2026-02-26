---
name: kb-abstract-fetch
description: Fetch and backfill missing `abstract` values from PostgreSQL `journals` by opening DOI redirect pages (`https://doi.org/{doi}`) with OpenClaw Browser and extracting page abstracts. Use when `kb-meta-fetch` has inserted metadata rows with empty abstract and you need latest-created rows processed in controlled batches (default 100) with dry-run and safe-write guards.
---

# KB Abstract Fetch

## Core Goal
- Reuse the same PostgreSQL connection env variables as `kb-meta-fetch`.
- Select rows whose `abstract` is empty and order by newest `created_at` first.
- Open `https://doi.org/<doi>` in OpenClaw Browser and extract abstract text.
- Write back only when the row is still empty at update time.
- Default to dry run; require explicit `--apply` to write.

## Required Environment
- `KB_DB_HOST`
- `KB_DB_PORT`
- `KB_DB_NAME`
- `KB_DB_USER`
- `KB_DB_PASSWORD`
- `KB_LOG_DIR` (required run log directory)

## Workflow
1. Run local self-test first (no DB/browser required):

```bash
python3 scripts/kb_abstract_fetch.py --self-test
```

2. Dry run first (default mode; no DB write):

```bash
python3 scripts/kb_abstract_fetch.py --limit 100
```

3. Apply updates after review:

```bash
python3 scripts/kb_abstract_fetch.py --limit 100 --apply
```

4. Override table/column names when needed (`created_at` is fixed and required):

```bash
python3 scripts/kb_abstract_fetch.py \
  --table journals \
  --doi-column doi \
  --abstract-column abstract \
  --limit 100 \
  --apply
```

## Safety Contract
- Selection filter:
  - DOI not empty
  - `abstract` empty (`NULL` or blank)
- Selection order:
  - newest `created_at` first (`ORDER BY created_at DESC NULLS LAST LIMIT n`)
- Update filter (second guard):
  - `WHERE doi = ? AND abstract is still empty`
- Run summary:
  - emit `RUN_SUMMARY_JSON=<json>` for current run only.
- Abort behavior:
  - stop early when errors exceed `--max-errors`.

## Browser Requirement
- `openclaw` CLI must be installed.
- Script checks `openclaw browser status`; if browser is not running, it tries `openclaw browser start`.
- If start fails (for example extension tab not attached), attach OpenClaw browser session first, then rerun.

## Script
- `scripts/kb_abstract_fetch.py`
