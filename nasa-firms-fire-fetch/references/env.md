# NASA FIRMS Fire Fetch Environment

Copy `assets/config.example.env` to `assets/config.env` for real use. The repository ignores `assets/config.env`, so it is the correct place for the real `NASA_FIRMS_MAP_KEY`.

- `NASA_FIRMS_BASE_URL`
  - Default: `https://firms.modaps.eosdis.nasa.gov`
  - Root host for FIRMS API paths.
- `NASA_FIRMS_MAP_KEY`
  - Required for remote fetches.
  - Get it from the FIRMS MAP_KEY signup flow.
- `NASA_FIRMS_TIMEOUT_SECONDS`
  - Per-request timeout.
- `NASA_FIRMS_MAX_RETRIES`
  - Retry count for transient HTTP and network failures.
- `NASA_FIRMS_RETRY_BACKOFF_SECONDS`
  - Initial retry delay.
- `NASA_FIRMS_RETRY_BACKOFF_MULTIPLIER`
  - Exponential backoff factor.
- `NASA_FIRMS_MIN_REQUEST_INTERVAL_SECONDS`
  - Minimum sleep between outbound requests.
- `NASA_FIRMS_MAX_DAYS_PER_RUN`
  - Local safety cap for inclusive `start_date` to `end_date`.
- `NASA_FIRMS_MAX_CHUNK_DAYS`
  - Internal chunk size.
  - Must stay within FIRMS `area/csv` request limit.
- `NASA_FIRMS_MAX_ESTIMATED_TRANSACTIONS_PER_RUN`
  - Local cap for estimated FIRMS transaction weight.
- `NASA_FIRMS_MAX_RETRY_AFTER_SECONDS`
  - Fail if server `Retry-After` is higher than this value.
- `NASA_FIRMS_ENABLE_AVAILABILITY_PROBE`
  - Default boolean for `--check-availability` when the CLI flag is omitted.
- `NASA_FIRMS_USER_AGENT`
  - User-Agent sent on every request.
