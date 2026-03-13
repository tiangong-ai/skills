# Environment Variables

Use environment variables for all runtime behavior. Do not hardcode API keys or mutable operational settings.

- `REGGOV_BASE_URL`
  - Default: `https://api.regulations.gov/v4`
  - Base URL for Regulations.gov API v4.
- `REGGOV_API_KEY` (required)
  - Regulations.gov API key sent in `X-Api-Key` header.
- `REGGOV_TIMEOUT_SECONDS`
  - Default: `60`
  - HTTP timeout for each request.
- `REGGOV_MAX_RETRIES`
  - Default: `4`
  - Retry count for transient failures (total attempts = retries + 1).
- `REGGOV_RETRY_BACKOFF_SECONDS`
  - Default: `1.5`
  - Initial retry delay in seconds.
- `REGGOV_RETRY_BACKOFF_MULTIPLIER`
  - Default: `2.0`
  - Exponential multiplier for retry backoff.
- `REGGOV_MIN_REQUEST_INTERVAL_SECONDS`
  - Default: `1.2`
  - Minimum interval between requests to reduce burst traffic.
- `REGGOV_PAGE_SIZE`
  - Default: `25`
  - Page size for comments fetch. Must be between `5` and `250`.
- `REGGOV_MAX_PAGES_PER_RUN`
  - Default: `20`
  - Hard safety cap for `--max-pages`.
- `REGGOV_MAX_RECORDS_PER_RUN`
  - Default: `2000`
  - Hard safety cap for `--max-records`.
- `REGGOV_MAX_RETRY_AFTER_SECONDS`
  - Default: `120`
  - Maximum `Retry-After` value accepted for auto-wait. Larger values fail fast.
- `REGGOV_USER_AGENT`
  - Default: `regulationsgov-comments-fetch/1.0`
  - User-Agent header.

Example:

```bash
export REGGOV_BASE_URL="https://api.regulations.gov/v4"
export REGGOV_API_KEY="<your_api_key>"
export REGGOV_TIMEOUT_SECONDS="60"
export REGGOV_MAX_RETRIES="4"
export REGGOV_RETRY_BACKOFF_SECONDS="1.5"
export REGGOV_RETRY_BACKOFF_MULTIPLIER="2.0"
export REGGOV_MIN_REQUEST_INTERVAL_SECONDS="1.2"
export REGGOV_PAGE_SIZE="25"
export REGGOV_MAX_PAGES_PER_RUN="20"
export REGGOV_MAX_RECORDS_PER_RUN="2000"
export REGGOV_MAX_RETRY_AFTER_SECONDS="120"
export REGGOV_USER_AGENT="regulationsgov-comments-fetch/1.0"
```
