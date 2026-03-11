# GDELT DOC API Source Notes

This skill operates on GDELT's query API layer, not raw table file exports.

## What This Skill Uses

- Endpoint:
  - `https://api.gdeltproject.org/api/v2/doc/doc`
- Retrieval styles via `mode`:
  - Article lists (for example `artlist`)
  - Timelines (for example `timelinevol`, `timelinevolraw`, `timelinetone`)
  - Other DOC modes documented by GDELT

## What This Skill Does Not Use

- It does not fetch raw GDELT file tables (`*.export.CSV.zip`, `*.mentions.CSV.zip`, `*.gkg.csv.zip`).
- It does not run BigQuery SQL in this skill.

## Official Links

- DOC API guide:
  - `https://blog.gdeltproject.org/gdelt-doc-2-0-api-debuts/`
- GDELT 2.0 overview:
  - `https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/`
