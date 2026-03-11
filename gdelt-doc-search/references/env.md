# Environment Variables

Use environment variables for all runtime behavior. The script does not hardcode secrets or mutable operational settings.

- `GDELT_DOC_API_BASE_URL`
  - Default: `https://api.gdeltproject.org/api/v2/doc/doc`
  - Base URL for DOC API retrieval.
- `GDELT_TIMEOUT_SECONDS`
  - Default: `60`
  - HTTP timeout for each request.
- `GDELT_MAX_RETRIES`
  - Default: `4`
  - Retry count for transient failures (total attempts = retries + 1).
- `GDELT_RETRY_BACKOFF_SECONDS`
  - Default: `1.5`
  - Initial retry delay in seconds.
- `GDELT_RETRY_BACKOFF_MULTIPLIER`
  - Default: `2.0`
  - Exponential factor for retry delay growth.
- `GDELT_MIN_REQUEST_INTERVAL_SECONDS`
  - Default: `5.0`
  - Minimum sleep interval between requests to reduce burst traffic.
- `GDELT_USER_AGENT`
  - Default: `gdelt-doc-search/1.0`
  - Value sent in HTTP `User-Agent`.

Example:

```bash
export GDELT_DOC_API_BASE_URL="https://api.gdeltproject.org/api/v2/doc/doc"
export GDELT_TIMEOUT_SECONDS="60"
export GDELT_MAX_RETRIES="4"
export GDELT_RETRY_BACKOFF_SECONDS="1.5"
export GDELT_RETRY_BACKOFF_MULTIPLIER="2.0"
export GDELT_MIN_REQUEST_INTERVAL_SECONDS="5.0"
export GDELT_USER_AGENT="gdelt-doc-search/1.0"
```
