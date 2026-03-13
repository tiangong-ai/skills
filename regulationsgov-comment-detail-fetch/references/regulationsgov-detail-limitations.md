# Regulations.gov Comment Detail Fetch Constraints and Safety Notes

## Request-Cost Characteristics

Detail fetch is an `N+1` pattern:

- 1 request per comment ID (plus retries).
- This is significantly more expensive than list fetch by pages.

Use this skill only when detail payloads are required.

## Official and Observed Constraints

- API key authentication required.
- Throttling can return `429` with `Retry-After`.
- Effective quotas should be observed from response headers:
  - `X-Ratelimit-Limit`
  - `X-Ratelimit-Remaining`

## Built-in Protections in This Skill

- Retry transient errors (`429/500/502/503/504`).
- Exponential backoff and minimum request interval throttle.
- `Retry-After` cap (`REGGOV_MAX_RETRY_AFTER_SECONDS`) to avoid long blocking.
- Max IDs safety cap per run (`REGGOV_MAX_COMMENT_IDS_PER_RUN`).
- Optional partial-failure mode (`--no-fail-on-item-error`) to continue on invalid IDs.
- Structure validation with optional quarantine issue files.

## Scope Boundaries

- This skill does not discover IDs by time window.
- Upstream list fetch should be handled by `$regulationsgov-comments-fetch`.
- No internal scheduler/poller is included.
