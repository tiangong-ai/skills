# Environment Variables

Use environment variables for all runtime behavior. Do not hardcode API keys or mutable operational settings.

- `YOUTUBE_BASE_URL`
  - Default: `https://www.googleapis.com/youtube/v3`
  - Base URL for YouTube Data API v3.
- `YOUTUBE_API_KEY` (required)
  - API key used for public YouTube Data API requests.
- `YOUTUBE_TIMEOUT_SECONDS`
  - Default: `60`
  - HTTP timeout for each request.
- `YOUTUBE_MAX_RETRIES`
  - Default: `4`
  - Retry count for transient failures (total attempts = retries + 1).
- `YOUTUBE_RETRY_BACKOFF_SECONDS`
  - Default: `1.5`
  - Initial retry delay in seconds.
- `YOUTUBE_RETRY_BACKOFF_MULTIPLIER`
  - Default: `2.0`
  - Exponential multiplier for retry backoff.
- `YOUTUBE_MIN_REQUEST_INTERVAL_SECONDS`
  - Default: `0.6`
  - Minimum interval between requests to avoid burst traffic.
- `YOUTUBE_SEARCH_PAGE_SIZE`
  - Default: `25`
  - Configured upper bound for `--page-size`. Must be between `1` and `50`.
- `YOUTUBE_MAX_SEARCH_PAGES_PER_RUN`
  - Default: `10`
  - Hard safety cap for `--max-pages`.
- `YOUTUBE_MAX_SEARCH_RESULTS_PER_RUN`
  - Default: `250`
  - Hard safety cap for `--max-results`.
- `YOUTUBE_MAX_VIDEO_DETAILS_PER_RUN`
  - Default: `250`
  - Hard safety cap for enriched videos fetched via `videos.list`.
- `YOUTUBE_MAX_RETRY_AFTER_SECONDS`
  - Default: `120`
  - Maximum `Retry-After` value accepted for auto-wait. Larger values fail fast.
- `YOUTUBE_USER_AGENT`
  - Default: `youtube-video-search/1.0`
  - User-Agent header.

Example:

```bash
export YOUTUBE_BASE_URL="https://www.googleapis.com/youtube/v3"
export YOUTUBE_API_KEY="<your_api_key>"
export YOUTUBE_TIMEOUT_SECONDS="60"
export YOUTUBE_MAX_RETRIES="4"
export YOUTUBE_RETRY_BACKOFF_SECONDS="1.5"
export YOUTUBE_RETRY_BACKOFF_MULTIPLIER="2.0"
export YOUTUBE_MIN_REQUEST_INTERVAL_SECONDS="0.6"
export YOUTUBE_SEARCH_PAGE_SIZE="25"
export YOUTUBE_MAX_SEARCH_PAGES_PER_RUN="10"
export YOUTUBE_MAX_SEARCH_RESULTS_PER_RUN="250"
export YOUTUBE_MAX_VIDEO_DETAILS_PER_RUN="250"
export YOUTUBE_MAX_RETRY_AFTER_SECONDS="120"
export YOUTUBE_USER_AGENT="youtube-video-search/1.0"
```
