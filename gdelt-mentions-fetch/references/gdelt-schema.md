# GDELT Mentions Schema Note

This skill uses line-level column-count validation as a lightweight structural guardrail.

## Global Mentions

- Official codebook: `GDELT-Global_Mentions_Codebook-V2.1.pdf`
- File pattern: `YYYYMMDDHHMMSS.mentions.CSV.zip`
- Default expected columns in this skill: `16`

Validation basis:
- Live inspection of current public files shows 16 tab-separated columns.
- The count matches the standard Global Mentions V2.1 field layout used in current GDELT 2.0 feeds.

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
