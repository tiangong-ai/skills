# Environment Variables

Use environment variables for all runtime behavior. Do not hardcode API keys.

- `REGGOV_BASE_URL`
  - Default: `https://api.regulations.gov/v4`
  - API base URL.
- `REGGOV_API_KEY` (required)
  - Regulations.gov API key sent via `X-Api-Key` header.
- `REGGOV_TIMEOUT_SECONDS`
  - Default: `60`
  - HTTP timeout per request.
- `REGGOV_MAX_RETRIES`
  - Default: `4`
  - Retry count for transient failures.
- `REGGOV_RETRY_BACKOFF_SECONDS`
  - Default: `1.5`
  - Initial retry delay.
- `REGGOV_RETRY_BACKOFF_MULTIPLIER`
  - Default: `2.0`
  - Exponential retry multiplier.
- `REGGOV_MIN_REQUEST_INTERVAL_SECONDS`
  - Default: `1.2`
  - Minimum request interval for throttling.
- `REGGOV_MAX_COMMENT_IDS_PER_RUN`
  - Default: `300`
  - Hard safety cap for IDs processed in one run.
- `REGGOV_MAX_RETRY_AFTER_SECONDS`
  - Default: `120`
  - Maximum `Retry-After` seconds accepted by auto-retry.
- `REGGOV_USER_AGENT`
  - Default: `regulationsgov-comment-detail-fetch/1.0`
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
export REGGOV_MAX_COMMENT_IDS_PER_RUN="300"
export REGGOV_MAX_RETRY_AFTER_SECONDS="120"
export REGGOV_USER_AGENT="regulationsgov-comment-detail-fetch/1.0"
```
