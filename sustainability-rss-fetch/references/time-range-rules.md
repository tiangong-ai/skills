# Time Range Rules

## Candidate Window Frequency
- The workflow is frequency-agnostic. Run manually or on any scheduler cadence.
- Recommended baseline for active feeds: every 15-60 minutes.

## Window Boundaries (`collect-window`)
- Accept `--start` and `--end` as:
  - `YYYY-MM-DD`
  - ISO datetime with timezone
- Boundary semantics are left-closed, right-open: `[start, end)`.
- For date-only `--end`, interpret as next-day boundary.

## Timestamp Normalization
- Normalize parsed `published` / `updated` timestamps to UTC ISO-8601.
- Keep `first_seen_at` and `last_seen_at` in UTC.
- If `published` / `updated` cannot be parsed into UTC time:
  - Candidate can still be collected when no window is provided.
  - Candidate is excluded when explicit window boundaries are provided.

## Incremental Sync Rules (`sync` mode)
- Use feed caching state (`etag`, `last_modified`) to reduce bandwidth.
- If response status is `304`, do not parse entries for that feed.
- If `etag`/`last_modified` are missing, perform normal fetch and rely on dedupe key.

## Retention
- `max_items_per_feed` controls per-run ingestion cap from each feed.
- `seen_ttl_days` controls cleanup of stale metadata records.
- Cleanup removes records not seen for more than `seen_ttl_days`.
