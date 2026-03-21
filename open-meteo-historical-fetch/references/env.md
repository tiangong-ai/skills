# Environment Variables

Use environment variables for all runtime behavior. The script does not hardcode mutable operational settings.

- `OPEN_METEO_BASE_URL`
  - Default: `https://archive-api.open-meteo.com/v1/archive`
  - Historical archive endpoint URL.
  - Override this when using a customer-specific Open-Meteo endpoint.
- `OPEN_METEO_API_KEY`
  - Default: unset
  - Optional Open-Meteo API key.
  - Leave unset for public development/evaluation usage.
- `OPEN_METEO_TIMEOUT_SECONDS`
  - Default: `45`
  - HTTP timeout for each request.
- `OPEN_METEO_MAX_RETRIES`
  - Default: `4`
  - Retry count for transient failures.
- `OPEN_METEO_RETRY_BACKOFF_SECONDS`
  - Default: `1.5`
  - Initial retry delay in seconds.
- `OPEN_METEO_RETRY_BACKOFF_MULTIPLIER`
  - Default: `2.0`
  - Exponential factor for retry delay growth.
- `OPEN_METEO_MIN_REQUEST_INTERVAL_SECONDS`
  - Default: `0.4`
  - Minimum sleep interval between requests to reduce burst traffic.
- `OPEN_METEO_MAX_LOCATIONS_PER_RUN`
  - Default: `10`
  - Hard safety cap for repeated `--location`.
- `OPEN_METEO_MAX_DAYS_PER_RUN`
  - Default: `366`
  - Hard safety cap for inclusive date span.
- `OPEN_METEO_MAX_HOURLY_VARIABLES_PER_RUN`
  - Default: `12`
  - Hard safety cap for repeated `--hourly-var`.
- `OPEN_METEO_MAX_DAILY_VARIABLES_PER_RUN`
  - Default: `12`
  - Hard safety cap for repeated `--daily-var`.
- `OPEN_METEO_MAX_RETRY_AFTER_SECONDS`
  - Default: `120`
  - Maximum accepted `Retry-After` value before failing fast.
- `OPEN_METEO_DEFAULT_TIMEZONE`
  - Default: `GMT`
  - Default timezone request parameter used when `--timezone` is not supplied.
- `OPEN_METEO_USER_AGENT`
  - Default: `open-meteo-historical-fetch/1.0`
  - Value sent in HTTP `User-Agent`.

Example:

```bash
export OPEN_METEO_BASE_URL="https://archive-api.open-meteo.com/v1/archive"
export OPEN_METEO_API_KEY=""
export OPEN_METEO_TIMEOUT_SECONDS="45"
export OPEN_METEO_MAX_RETRIES="4"
export OPEN_METEO_RETRY_BACKOFF_SECONDS="1.5"
export OPEN_METEO_RETRY_BACKOFF_MULTIPLIER="2.0"
export OPEN_METEO_MIN_REQUEST_INTERVAL_SECONDS="0.4"
export OPEN_METEO_MAX_LOCATIONS_PER_RUN="10"
export OPEN_METEO_MAX_DAYS_PER_RUN="366"
export OPEN_METEO_MAX_HOURLY_VARIABLES_PER_RUN="12"
export OPEN_METEO_MAX_DAILY_VARIABLES_PER_RUN="12"
export OPEN_METEO_MAX_RETRY_AFTER_SECONDS="120"
export OPEN_METEO_DEFAULT_TIMEZONE="GMT"
export OPEN_METEO_USER_AGENT="open-meteo-historical-fetch/1.0"
```
