# GDELT Data Sources Overview

This note summarizes official GDELT source families and the table selected by this skill.

## Official Source Families

From GDELT 2.0 documentation and public endpoints:

- Core 15-minute feeds:
  - `Events` table (`*.export.CSV.zip`)
  - `Global Mentions` table (`*.mentions.CSV.zip`)
  - `Global Knowledge Graph (GKG)` table (`*.gkg.csv.zip`)
- Translation variants:
  - `masterfilelist-translation.txt` exposes translated export/mentions/gkg files.
- Discovery/index files:
  - `lastupdate.txt` for latest snapshots.
  - `masterfilelist.txt` for full historical list.

## Table Chosen in This Skill

This skill intentionally implements only:

- `Global Mentions` files matching `YYYYMMDDHHMMSS.mentions.CSV.zip`

Why this table:

- It is the canonical mentions-level feed for source mention dynamics.
- It provides stable, incremental 15-minute snapshots.
- It maps cleanly to ingestion pipelines that need deterministic file-level checkpoints.

## Official Links

- GDELT 2.0 overview:
  - `https://blog.gdeltproject.org/gdelt-2-0-our-global-world-in-realtime/`
- Data access overview:
  - `https://www.gdeltproject.org/data.html`
- Latest snapshot index:
  - `http://data.gdeltproject.org/gdeltv2/lastupdate.txt`
- Historical master list:
  - `http://data.gdeltproject.org/gdeltv2/masterfilelist.txt`
- Translation master list:
  - `http://data.gdeltproject.org/gdeltv2/masterfilelist-translation.txt`
- Mentions codebook:
  - `http://data.gdeltproject.org/documentation/GDELT-Global_Mentions_Codebook-V2.1.pdf`
