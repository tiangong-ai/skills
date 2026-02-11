---
name: sustainability-summary
description: Retrieve time-windowed sustainability RSS evidence from SQLite and let the agent produce final summaries using RAG over selected records and fields. Use when generating daily, weekly, monthly, or custom-range sustainability digests directly in agent responses.
---

# Sustainability Summary

## Core Goal
- Pull the right records and fields for a requested time range.
- Package evidence into a compact JSON context for RAG.
- Let the agent synthesize final summary text from retrieved evidence.
- Support daily, weekly, monthly, and custom time windows.

## Triggering Conditions
- Receive requests for daily, weekly, or monthly sustainability digests.
- Receive requests for arbitrary date-range sustainability summaries.
- Need evidence-grounded summary output from RSS entries/fulltext.
- Need agent-generated summary style rather than rigid scripted report format.

## Input Requirements
- Required tables in SQLite: `feeds`, `entries` (from `sustainability-rss-fetch`).
- Optional table: `entry_content` (from `sustainability-fulltext-fetch`).
- Shared DB path should be the same across all sustainability RSS skills.

## RAG Workflow
1. Retrieve evidence context by time window.

```bash
python3 scripts/time_report.py \
  --db sustainability_rss.db \
  --period weekly \
  --date 2026-02-10 \
  --max-records 120 \
  --max-per-feed 20 \
  --summary-chars 8192 \
  --fulltext-chars 8192 \
  --pretty \
  --output /tmp/sustainability-weekly-context.json
```

2. Load retrieval output and generate final summary in agent response.
- Read `query`, `dataset`, `aggregates`, `records`.
- Prioritize `records` as evidence source.
- Mention key trends, major events, and notable changes grounded in records.

3. Include evidence anchors in summary.
- Reference `entry_id`, feed, and URL for key claims.
- If retrieval is truncated, state that summary is based on sampled top records.

## Time Window Modes
- `--period daily --date YYYY-MM-DD`
- `--period weekly --date YYYY-MM-DD`
- `--period monthly --date YYYY-MM-DD`
- `--period custom --start ... --end ...`

Custom boundaries support both `YYYY-MM-DD` and ISO datetime.

## Field Selection for RAG
- Use `--fields` to control token budget and relevance.
- Default fields are tuned for summarization:
  - `entry_id,timestamp_utc,timestamp_source,feed_title,feed_url,title,url,summary,fulltext_status,fulltext_length,fulltext_excerpt`
- Common minimal field set for tight context:
  - `entry_id,timestamp_utc,feed_title,title,url,summary`

## Recommended Agent Output Pattern
- Use this order in final response:
  1. Time range scope
  2. Top themes/trends
  3. Key developments (grouped)
  4. Risks/open questions
  5. Evidence list (entry ids + URLs)

## Configurable Parameters
- `--db`
- `--period`
- `--date`
- `--start`
- `--end`
- `--max-records`
- `--max-per-feed`
- `--summary-chars`
- `--fulltext-chars`
- `--top-feeds`
- `--top-keywords`
- `--fields`
- `--output`
- `--pretty`
- `--fail-on-empty`

## Error Handling
- Missing `feeds`/`entries`: fail fast with setup guidance.
- Invalid date/time/field list: return parse errors.
- Missing `entry_content`: continue in metadata-only mode.
- Empty retrieval set: return empty context; optionally fail with `--fail-on-empty`.

## References
- `references/time-window-rules.md`
- `references/report-format.md`

## Assets
- `assets/config.example.json`

## Scripts
- `scripts/time_report.py`
