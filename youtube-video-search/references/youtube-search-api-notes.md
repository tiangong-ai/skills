# YouTube Video Search API Notes

## Endpoints Used

- `GET /search`
  - Purpose: discover public `video` resources from a query string.
  - Main parameters used by this skill:
    - `part=snippet`
    - `type=video`
    - `q`
    - `publishedAfter`
    - `publishedBefore`
    - `channelId`
    - `order`
    - optional video filters (`videoDuration`, `videoLicense`, etc.)
- `GET /videos`
  - Purpose: enrich discovered video IDs with metadata and public statistics.
  - Main parameters used by this skill:
    - `part=snippet,statistics,contentDetails,status,liveStreamingDetails`
    - `id`

## Engineering Notes

- Keep discovery and comment collection separate.
- Use `search.list` only to discover candidate videos; use `videos.list` to decide whether a video is worth comment crawling.
- Prefer `--order date` for fresh-domain reconnaissance and `--order relevance` when recall matters more than chronology.
- Use `--comment-count-min` or `--skip-without-comments` to suppress videos that are unlikely to produce useful downstream comment datasets.
- Save JSONL artifacts and pass them to `$youtube-comments-fetch`.

## Useful Query Patterns

- Broad domain:
  - `"climate change pollution renewable energy"`
- Narrow phrase:
  - `"environmental policy" "carbon tax"`
- Channel constrained:
  - same query plus `--channel-id [CHANNEL_ID]`
- Fresh-window discovery:
  - add `--published-after` and `--published-before`

The skill does not generate the query itself. Let OpenClaw decide `--query` from the upstream mission.
