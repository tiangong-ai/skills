---
name: kb-meta-fetch
description: Fetch journal articles from Crossref published after a user-specified date and insert them into PostgreSQL `journals` with DOI deduplication. Use when incrementally ingesting journal metadata from `journals_issn` into `journals`.
---

# KB Meta Fetch

## Core Goal
- Pull `journal-article` records from Crossref after a given `--from-date`.
- Read ISSN seed rows from `journals_issn` (`journal`, `issn1`).
- Insert rows into `journals` with `ON CONFLICT (doi) DO NOTHING`.
- Keep the implementation aligned with `1_crossref_multi_increment.py`.

## Run Workflow
1. Set database connection env vars (user-managed keys prefixed with `KB_`):
- `KB_DB_HOST`
- `KB_DB_PORT`
- `KB_DB_NAME`
- `KB_DB_USER`
- `KB_DB_PASSWORD`
- `KB_LOG_DIR` (required, log output directory)

2. Run incremental fetch with a required date:

```bash
python3 scripts/crossref_multi_increment.py --from-date 2024-05-01
```

3. Check logs in:
- `${KB_LOG_DIR}/crossref-YYYYMMDD.log` (UTC date)

## Behavior Contract
- Query Crossref endpoint: `https://api.crossref.org/journals/{issn}/works`.
- Filter with `type:journal-article,from-pub-date:<from-date>`.
- Keep only items whose `container-title` equals target journal title (case-insensitive).
- Continue pagination with cursor until no matching items remain.
- Store fields in `journals`: `title`, `doi`, `journal`, `authors`, `date`.

## Scope Boundary
- Implement only Crossref incremental fetch + insert into `journals`.

## Script
- `scripts/crossref_multi_increment.py`
