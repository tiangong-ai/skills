# Output Rules

## URL Canonicalization and Identity Keys
Canonicalize URL before dedupe:
- Lowercase scheme and host.
- Remove fragment (`#...`).
- Remove common tracking params (`utm_*`, `ref`, `source`, `fbclid`, `gclid`).
- Keep path and semantic query params.

Build identity keys per entry:
- `guid`: feed-scoped item id (`<feed_url>:<guid>`) to avoid cross-feed collisions.
- `canonical_url`: canonicalized `link`.
- `legacy_guid`: historical key format `guid:<guid>` for compatibility.
- `fallback_hash`: only when both `guid` and `link` are missing.

Build compatibility `dedupe_key` snapshot (for legacy/readability):
- `guid:<feed_url>:<guid>` if guid exists.
- `url:<canonical_url>` if guid missing but URL exists.
- `hash:<sha256(...)>` only as fallback.

## Persistent Dedupe (Frequency-Agnostic)
1. Keep per-feed cache state:
- `etag`
- `last_modified`
- `last_checked_at`

2. Use conditional requests when state exists:
- Send `If-None-Match: <etag>`.
- Send `If-Modified-Since: <last_modified>`.

3. If response is `304 Not Modified`:
- Mark feed as `no_change`.
- Skip item parsing for this feed.

4. Resolve existing entries in this order:
- Match any `(key_type, key_value)` in `entry_identities`.
- Fallback to legacy `entries.dedupe_key` lookup for backward compatibility.

5. Persist identity and content state:
- `entry_identities`: unique `(key_type, key_value)` for stable identity mapping.
- `entries.content_hash`: fingerprint for update detection.
- `entries.first_seen_at` / `entries.last_seen_at`.
- `entries.match_confidence`: `high` for guid/url identity, `low` for fallback hash only.

6. De-dup behavior:
- Existing identity + same `content_hash`: skip as duplicate (touch `last_seen_at`).
- Existing identity + changed `content_hash`: update metadata row.
- No identity match: insert a new metadata row and register identity keys.

7. Retention:
- Delete seen records older than `seen_ttl_days` (default `30`).
- This workflow applies to any run cadence (manual, cron, webhook, or ad hoc).

## Error Handling
- Invalid feed/input format: report failed source and continue with valid sources.
- Missing `guid` and `link`: use hash fallback identity and mark low confidence.
- Seen store unavailable: use in-memory dedupe for current run and emit warning.

## Quality Control Checklist
- URL canonicalization was applied before dedupe.
- Conditional request state was applied when available.
- Every output item has at least one stable identity key.
- Feed and entry metadata were persisted successfully.
- No full-text extraction and no summarization were performed.
