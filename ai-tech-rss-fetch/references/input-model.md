# Input Model

## Accepted Input Sources
- RSS XML feed URLs
- OPML feed lists (`.opml`)
- Pre-collected RSS item files (`.json`, `.jsonl`, `.csv`, `.md`, `.txt`)

## Normalized Record Schema
Normalize every input item to the following fields before filtering and output:

```json
{
  "item_id": "optional stable id",
  "guid": "optional feed guid/id",
  "published_at": "2026-02-10T09:30:00+08:00",
  "source_type": "rss",
  "source_name": "feed title or source name",
  "title": "short title",
  "link": "https://example.com/item",
  "canonical_url": "normalized url for dedupe",
  "content": "rss content/description/summary if provided",
  "full_text": "extracted full article text if available",
  "final_text": "selected output text after fallback",
  "text_source": "fulltext|rss_content|rss_summary|none",
  "dedupe_key": "stable dedupe key",
  "content_hash": "sha256(final_text)",
  "tags": ["ai", "release"],
  "author": "optional author"
}
```

## Minimal Parsing Rules by File Type
- RSS XML/Atom:
  - Parse `title`, `link`, `pubDate`/`updated`, `content:encoded`, `description`, `summary`.
  - Keep raw content fields for fallback use.
- OPML:
  - Parse each `<outline>` `xmlUrl` as feed source.
  - Fetch and parse each feed URL as RSS XML/Atom.
- `json` / `jsonl`:
  - Accept arrays or line-delimited objects.
  - Common aliases:
    - time: `published_at`, `pub_date`, `date`, `timestamp`
    - title: `title`, `headline`
    - url: `link`, `url`
    - content: `content`, `description`, `summary`, `full_text`
- `csv`:
  - Map columns by aliases same as JSON aliases.
- `md` / `txt`:
  - Parse list-like entries with title, link, and optional text blocks.

## Text Source Selection Model
- `fulltext`: extracted from article URL and passes length/quality checks.
- `rss_content`: RSS `content:encoded` or `content`.
- `rss_summary`: RSS `description` or `summary`.
- `none`: no usable text fields found.

## RSS Source Asset
- Use `assets/hn-popular-blogs-2025.opml` as the default feed source list.
- Origin URL: `https://gist.github.com/emschwartz/e6d2bf860ccc367fe37ff953ba6de66b#file-hn-popular-blogs-2025-opml`
- The OPML contains 92 RSS entries and can be used as a candidate feed pool.
