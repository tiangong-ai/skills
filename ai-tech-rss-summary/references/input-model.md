# Input Model

## Accepted Input Sources
- Logs: `.txt`, `.md`, `.json`, `.csv`
- Events: calendar entries, timeline events, releases/incidents
- Task/activity status: task list exports, issue trackers, manual status notes
- RSS/external subscriptions: feed items or pre-summarized digest entries

## Normalized Record Schema
Normalize every input item to the following fields before filtering and reporting:

```json
{
  "timestamp": "2026-02-10T09:30:00+08:00",
  "source_type": "log|event|task|rss",
  "source_name": "optional source name",
  "title": "short title",
  "content": "full text or summary",
  "status": "todo|in_progress|done|blocked|delayed|na",
  "importance": 1,
  "tags": ["ai", "release"],
  "link": "https://example.com/item",
  "owner": "optional person/team"
}
```

## Minimal Parsing Rules by File Type
- `txt`/`md`: split by headings, dates, bullets; extract status keywords and links.
- `json`: accept arrays or nested objects; flatten to records with explicit field mapping.
- `csv`: map columns by aliases; common aliases:
  - date: `date`, `time`, `timestamp`
  - title: `title`, `summary`, `task`
  - status: `status`, `state`
  - link: `url`, `link`

## Status Interpretation
- `todo`: not started
- `in_progress`: active work
- `done`: completed in selected range
- `blocked`: cannot proceed due to dependency
- `delayed`: missed planned timeline
- `na`: non-task informational items (for example RSS news)

## RSS Source Asset
- Use `assets/hn-popular-blogs-2025.opml` as the default feed source list.
- Origin URL: `https://gist.github.com/emschwartz/e6d2bf860ccc367fe37ff953ba6de66b#file-hn-popular-blogs-2025-opml`
- The OPML contains 92 RSS entries and can be used as a candidate feed pool.
