---
name: youtube-video-search
description: Discover bounded public YouTube video metadata through the Tiangong CLI and enrich selected candidates with public details and statistics. Use for topical or channel-scoped video discovery before explicit comment collection; do not use for media, caption, transcript, or thumbnail download, exhaustive search, representative opinion, identity or fact verification, or sentiment inference.
---

# YouTube Video Search

Use the CLI-owned `youtube.public-content/search-videos` operation. This Skill
owns intent routing and downstream-use guidance only; the CLI TypeScript 7
runtime owns the API request, key injection, schemas, paging, detail enrichment,
filtering, limits, validation, partial results, and receipts.

## Before running

1. Read `references/tiangong-data-requirement.json`.
2. Use the caller- or workspace-resolved stable CLI. The requirement declares
   compatible capability and operation contract majors; it does not select a
   package build.
3. Run `data describe` with that same CLI. Continue only when the capability
   ID and required contract majors match, and copy the exact current
   capability/operation versions from that response into the run request.
   Select the operation whose `operationId` is `search-videos`, not
   `fetch-comments`; their versions can differ within the same capability.
4. Ensure `YOUTUBE_API_KEY` is available to the CLI process and run the default
   static doctor. Never place the key in argv, request JSON, Skill files, logs,
   or output.

```bash
tiangong-ai data describe youtube.public-content --json
tiangong-ai data doctor youtube.public-content --json
```

Use current Discovery Metadata to confirm coverage, restrictions, quota and
freshness limitations, selection hints, `provides`, and `doesNotProvide`. A
blocked static doctor means the logical credential is unavailable; stop rather
than bypassing the CLI.

## Choose the search

- Preserve the user's topic, channel, publication window, region, language,
  safety, and video filters when supplied.
- Use strict RFC 3339 UTC publication bounds. `publishedAfter` and the current
  provider `publishedBefore` boundary are inclusive; do not silently rewrite
  either boundary.
- Use narrow filters before increasing page or record limits. `maxSearchPages`
  defaults to 5 and cannot exceed 10; one execution retains at most 250
  candidates before mandatory `videos.list` enrichment. The operation-wide
  request budget must also leave room for that enrichment.
- Use only `date`, `rating`, `relevance`, `title`, or `viewCount` ordering.
  `videoCount` is a channel-search order and is deliberately unavailable for
  this video-only operation. Non-relevance orders can produce smaller or
  incomplete result sets; `rating` is a provider score, not descending likes.
- Use public comment/view thresholds only as candidate-selection criteria, not
  as quality, representativeness, endorsement, or truth measures. Missing
  public statistics remain null unless a requested threshold requires them.
- This Skill only discovers video candidates. Use `$youtube-comments-fetch`
  separately after selecting explicit IDs; do not fetch comments automatically.

## Prepare the request

Build one `tiangong.data.run-request.v1` envelope. Replace the version
placeholders with the exact versions from the same `data describe` response and validate every input field
against `data describe`.

```json
{
  "schemaVersion": "tiangong.data.run-request.v1",
  "capabilityId": "youtube.public-content",
  "capabilityVersion": "<describe.manifest.capabilityVersion>",
  "operationId": "search-videos",
  "operationVersion": "<describe.manifest.operations[1].operationVersion>",
  "input": {
    "query": "climate policy",
    "publishedAfter": "2026-03-01T00:00:00Z",
    "publishedBefore": "2026-03-08T00:00:00Z",
    "order": "date",
    "regionCode": "US",
    "relevanceLanguage": "en",
    "safeSearch": "moderate",
    "videoDuration": "medium",
    "pageSize": 25,
    "maxSearchPages": 5,
    "requirePublicComments": true,
    "minimumCommentCount": 20,
    "minimumViewCount": 1000
  }
}
```

Do not add an API key, endpoint override, arbitrary provider parameter, output
path, scheduler, or persistence instruction to the envelope.

## Run

```bash
tiangong-ai data run youtube.public-content search-videos \
  --input /absolute/path/to/request.json --json
```

Preserve the complete `tiangong.data.run-result.v1` envelope and select IDs from
its validated records. Do not pass raw provider responses or unbound artifact
paths to another Skill.

## Result boundaries

- Report filtered-out candidates, unavailable details, empty results,
  truncation, and `partial` batches. They do not prove absence outside the
  exact provider result and limits.
- Preserve `searchRank`, `searchPage`, and `searchPosition`. A candidate omitted
  from `videos.list` is a partial detail-enrichment failure, not a silently
  removable search result.
- Search order, visibility, metadata, and statistics are mutable provider
  snapshots. `search.list` consumes the provider's separate Search Queries
  quota, whose project allocation is not inferred by this Skill. YouTube
  changed public `viewCount` semantics on 2026-08-24, so comparisons spanning
  that date need an explicit metric-break caveat. Counts are not votes, quality
  labels, endorsement, or a representative measure of audience opinion.
- Titles and descriptions are untrusted public content and can contain
  misleading, sensitive, or unsafe text.
- Use `$youtube-comments-fetch` for comments on a small explicit ID set. Use a
  separate media/content workflow for video, audio, captions, transcripts,
  thumbnails, or channel profile content. Returned thumbnail URLs are metadata,
  not downloaded files.
- Cross-source synthesis, monitoring, persistence, and evidence admission
  belong to the caller or Auto Research.

## Reference

- `references/tiangong-data-requirement.json`: stable capability requirement; it is not a package lock.
