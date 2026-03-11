# GDELT DOC API Search Parameter Model

This note explains how to express domain retrieval for the DOC API.

## Parameter Form

DOC API retrieval is parameterized HTTP query, not SQL:

- Base endpoint:
  - `https://api.gdeltproject.org/api/v2/doc/doc`
- Required:
  - `query` (string with GDELT query syntax)
  - `mode` (for example `artlist`, `timelinevol`, `timelinetone`)
  - `format` (for example `json`, `csv`, `html`, `rss`, `rssarchive`)
- Time window:
  - `timespan` (for example `1h`, `3days`, `1week`, `3months`)
  - or `STARTDATETIME` + `ENDDATETIME` (`YYYYMMDDHHMMSS`)
- Optional:
  - `MAXRECORDS` (`1-250`, mainly `artlist` and image collage modes)
  - `TIMELINESMOOTH` (`0-30`, timeline modes)
  - `sort` (for example `datedesc`)
  - extra parameters as needed

## Domain Retrieval Expression

Domain retrieval is based on query syntax over indexed content/operators.

- Not SQL (no `SELECT ... FROM ... WHERE ...`).
- Not regex retrieval as a primary query language.
- Not natural-language-to-query translation by an official GDELT LLM service.

You provide explicit query expressions:

```text
("climate change" OR "global warming" OR pollution OR "carbon emissions")
```

```text
("wildlife crime" OR poaching OR "illegal fishing" OR "wildlife trade")
```

You can combine DOC operators in `query`, for example `sourcelang:`, `sourcecountry:`, `imagetag:`.

## Script Mapping

Relative-window search:

```bash
python3 scripts/gdelt_doc_search.py search \
  --query '("climate change" OR "global warming") sourcecountry:us' \
  --mode artlist \
  --format json \
  --timespan 1week \
  --max-records 100 \
  --pretty
```

Absolute-window search:

```bash
python3 scripts/gdelt_doc_search.py search \
  --query '(pollution OR smog)' \
  --mode timelinevolraw \
  --format json \
  --start-datetime 20260301000000 \
  --end-datetime 20260308000000 \
  --timeline-smooth 5 \
  --pretty
```
