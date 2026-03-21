# YouTube Comments API Notes

## Endpoints Used

- `GET /commentThreads`
  - Purpose: fetch top-level comment threads for a video.
  - Main parameters used by this skill:
    - `part=snippet,replies` or `part=snippet`
    - `videoId`
    - `order`
    - `textFormat`
    - `searchTerms`
    - `maxResults`
- `GET /comments`
  - Purpose: fetch replies for a top-level comment when embedded replies are incomplete.
  - Main parameters used by this skill:
    - `part=snippet`
    - `parentId`
    - `textFormat`
    - `maxResults`

## Engineering Notes

- Accept video IDs from CLI and from `$youtube-video-search` output files.
- Filter time windows client-side using `publishedAt` or `updatedAt`.
- Prefer `--order time` when using time windows, because it improves crawl efficiency.
- Use `--search-terms` only as a narrowing hint on already selected videos.
- Preserve both `text_display` and `text_original`; public API responses may not always expose them identically.

## Operational Advice

- Start with a small `--max-videos` and `--max-thread-pages`.
- Use `--dry-run` before large pulls.
- Save JSONL output and let downstream agents analyze that file rather than raw stderr logs.
