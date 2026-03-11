# GDELT GKG Schema Note

This skill uses line-level column-count validation as a lightweight structural guardrail.

## GKG

- Official codebook: `GDELT-Global_Knowledge_Graph_Codebook-V2.1.pdf`
- File pattern: `YYYYMMDDHHMMSS.gkg.csv.zip`
- Default expected columns in this skill: `27`

Validation basis:
- Live inspection of current public files shows 27 tab-separated columns.
- This aligns with the modern GKG 2.1 layout in current public feeds.

## Validation Strategy

The script checks:
- ZIP integrity (`ZipFile.testzip()`)
- UTF-8 strict decoding per line
- Tab-separated column counts per line

This is intentionally lightweight and stable for ingestion pipelines. It does not attempt deep semantic validation of field values.

## When to Override `--expected-columns`

Override only when:
- GDELT publishes a revised schema/codebook.
- You are consuming a distinct variant with a different field count.
- You have verified the live feed format and want a temporary compatibility override.
