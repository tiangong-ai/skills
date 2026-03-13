# Regulations.gov Comments Fetch Constraints and Safety Notes

## Officially Documented Constraints

- API key authentication required (header `X-Api-Key`).
- `page[size]` must be within `5..250`.
- Time filter format requirements:
  - `postedDate`: `yyyy-MM-dd`
  - `lastModifiedDate`: `yyyy-MM-dd HH:mm:ss`

## Rate Limits

- Public guidance indicates `DEMO_KEY` has very low quota.
- Real keys can have different limits by API gateway policy.
- Effective limits should be read from response headers:
  - `X-Ratelimit-Limit`
  - `X-Ratelimit-Remaining`
  - `Retry-After` (on throttling)

## Observed Runtime Behavior (verified 2026-03-12 UTC)

- On throttling, API returns `429 Too Many Requests`.
- `Retry-After` can be very large (multi-hour).
- Error body commonly includes:
  - `error.code = OVER_RATE_LIMIT`
  - `error.message` with support guidance.

## Built-in Protections in This Skill

- Retry for transient statuses (`429/500/502/503/504`).
- Exponential backoff with configurable base and multiplier.
- Minimum request interval throttle.
- `Retry-After` cap (`REGGOV_MAX_RETRY_AFTER_SECONDS`) to avoid blocking for hours.
- Run safety caps:
  - max pages per run
  - max records per run
- Structure validation with optional quarantine issue files.

## Scope Boundaries

- This skill fetches list-level `comments` resources only.
- It does not run a scheduler/poller internally.
- Repeated polling should be orchestrated by the caller (OpenClaw/job runner).
