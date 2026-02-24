# Input Model

## Accepted Input Sources
- RSS XML feed URLs
- OPML feed lists (`.opml`)

## Normalized Record Schema
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
    "id": 123,
    "dedupe_key": "compat snapshot: guid:<feed_url>:<guid> | url:<canonical_url> | hash:<sha256>",
    "guid": "optional guid/id",
    "url": "https://example.com/post",
    "canonical_url": "https://example.com/post",
    "title": "entry title",
    "author": "optional author",
    "published_at": "2026-02-10T09:30:00Z",
    "updated_at": "2026-02-10T10:00:00Z",
    "summary": "feed summary/description",
    "categories": ["ai", "llm"],
    "content_hash": "sha256(title+summary+timestamps+url)",
    "match_confidence": "high | low",
    "first_seen_at": "2026-02-10T10:05:00Z",
    "last_seen_at": "2026-02-10T10:05:00Z"
  },
  "entry_identity": {
    "entry_id": 123,
    "key_type": "guid | canonical_url | legacy_guid | fallback_hash",
    "key_value": "identity value",
    "created_at": "2026-02-10T10:05:00Z"
  }
}
```

## Parsing Rules
- RSS XML/Atom:
  - Parse feed metadata: `title`, `link`, HTTP caching headers, status.
  - Parse entry metadata: `id/guid`, `link`, `title`, `author`, `published`, `updated`, `summary`, tags.
  - Build identity keys in `entry_identities` for robust matching across runs:
    - `guid` (feed-scoped id)
    - `canonical_url`
    - `legacy_guid` (historical compatibility key)
    - `fallback_hash` only when guid/link are unavailable
- OPML:
  - Parse every `<outline xmlUrl="...">` as a feed URL and insert into subscriptions.

## RSS Source Asset
- Use `assets/hn-popular-blogs-2025.opml` as the default feed source list.
- Origin URL: `https://gist.github.com/emschwartz/e6d2bf860ccc367fe37ff953ba6de66b#file-hn-popular-blogs-2025-opml`
- The OPML contains 92 RSS entries and can be used as a candidate feed pool.
