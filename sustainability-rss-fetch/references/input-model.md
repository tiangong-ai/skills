# Input Model

## Accepted Input Sources
- RSS XML feed URLs
- OPML feed lists (`.opml`)
- Topic prompt for semantic screening

## Default Source Asset
- `assets/journal.opml`

## Candidate Window Schema (`collect-window` output)

```json
{
  "generated_at_utc": "2026-02-11T12:00:00Z",
  "topic_prompt": "...",
  "window": {
    "start_utc": "2026-02-01T00:00:00Z",
    "end_utc": "2026-02-11T00:00:00Z"
  },
  "db_ingest": {
    "new": 100,
    "updated": 20,
    "unchanged": 300
  },
  "candidates": [
    {
      "candidate_id": 1,
      "doi": "10.1038/nature12373",
      "doi_is_surrogate": 0,
      "title": "Article title",
      "published_at": "2026-02-10T09:30:00Z",
      "feed_title": "Example Journal",
      "url": "https://example.com/post",
      "summary": "Abstract snippet",
      "categories": ["lca", "carbon"],
      "entry_record": {
        "doi": "10.1038/nature12373",
        "doi_is_surrogate": 0,
        "canonical_url": "https://example.com/post"
      }
    }
  ]
}
```

## Persisted Record Schema (`entries`)

```json
{
  "doi": "10.1038/nature12373",
  "doi_is_surrogate": 0,
  "is_relevant": 1,
  "feed_id": 12,
  "guid": "optional guid",
  "url": "https://example.com/post",
  "canonical_url": "https://example.com/post",
  "title": "entry title",
  "author": "optional author",
  "published_at": "2026-02-10T09:30:00Z",
  "updated_at": "2026-02-10T10:00:00Z",
  "summary": "feed summary/description",
  "categories": ["lca", "mfa"],
  "content_hash": "sha256(...)",
  "first_seen_at": "2026-02-10T10:05:00Z",
  "last_seen_at": "2026-02-10T10:05:00Z"
}
```

## Relevance Labeling
- `insert-selected` marks selected candidates as `is_relevant=1`.
- Unselected candidates are pruned to DOI-only (`is_relevant=0`).

## Parsing Rules
- Parse entry metadata from RSS/Atom (`id/guid`, `link`, `title`, `author`, `published`, `updated`, `summary`, tags).
- Resolve DOI from native fields, links, and text patterns.
- If DOI is missing, generate deterministic surrogate DOI (`rss-hash:...`) to keep full-ingestion behavior.
