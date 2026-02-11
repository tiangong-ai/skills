# Input Model

## Accepted Input Sources
- RSS XML feed URLs
- OPML feed lists (`.opml`)
- Topic prompt for semantic screening (default sustainability themes, user-customizable)

## Default Source Asset
- Use `assets/journal.opml` as the default feed source list.
- This file is sourced from user-provided journal feeds and should be treated as the first-choice input.

## Candidate Window Schema (`collect-window` output)

```json
{
  "generated_at_utc": "2026-02-11T12:00:00Z",
  "topic_prompt": "筛选与可持续主题相关的文章：生命周期评价、物质流分析、绿色供应链、绿电、绿色设计、减污降碳",
  "window": {
    "start_utc": "2026-02-01T00:00:00Z",
    "end_utc": "2026-02-11T00:00:00Z"
  },
  "feeds": [
    {
      "feed_url": "https://example.com/feed.xml",
      "feed_title": "Example Journal",
      "status": 200,
      "candidate_count": 10,
      "skipped_by_window": 2
    }
  ],
  "candidates": [
    {
      "candidate_id": 1,
      "title": "Article title",
      "published_at": "2026-02-10T09:30:00Z",
      "feed_title": "Example Journal",
      "url": "https://example.com/post",
      "summary": "Abstract snippet",
      "categories": ["lca", "carbon"],
      "dedupe_key": "guid:https://example.com/feed.xml:abc123",
      "entry_record": {
        "guid": "abc123",
        "canonical_url": "https://example.com/post"
      }
    }
  ]
}
```

## Persisted Record Schema (`insert-selected`)
Persist feed-level and entry-level metadata in SQLite.

```json
{
  "feed": {
    "feed_url": "https://example.com/feed.xml",
    "feed_title": "Example Feed",
    "site_url": "https://example.com/",
    "etag": "optional etag",
    "last_modified": "optional HTTP last-modified header",
    "last_checked_at": "2026-02-10T09:30:00Z",
    "last_status": 200
  },
  "entry": {
    "dedupe_key": "guid:<feed_url>:<id> | url:<canonical_url> | hash:<sha256>",
    "guid": "optional guid/id",
    "url": "https://example.com/post",
    "canonical_url": "https://example.com/post",
    "title": "entry title",
    "author": "optional author",
    "published_at": "2026-02-10T09:30:00Z",
    "updated_at": "2026-02-10T10:00:00Z",
    "summary": "feed summary/description",
    "categories": ["lca", "mfa"],
    "content_hash": "sha256(title+summary+timestamps+url)",
    "first_seen_at": "2026-02-10T10:05:00Z",
    "last_seen_at": "2026-02-10T10:05:00Z"
  }
}
```

## Parsing Rules
- RSS XML/Atom:
  - Parse feed metadata: `title`, `link`, HTTP caching headers, status.
  - Parse entry metadata: `id/guid`, `link`, `title`, `author`, `published`, `updated`, `summary`, tags.
- OPML:
  - Parse every `<outline xmlUrl="...">` as a feed URL.
- Topic screening:
  - Perform semantic screening in agent context using `topic_prompt` and user instructions.
  - Do not use regex-only rules for final relevance decisions.
