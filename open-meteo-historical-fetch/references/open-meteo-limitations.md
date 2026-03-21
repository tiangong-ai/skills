# Open-Meteo Historical Fetch Limits

## Operational Constraints

- This skill uses the public historical archive endpoint by default.
- The public endpoint is suitable for development and evaluation, but production/commercial usage may require customer-specific Open-Meteo access and API key configuration.
- Free-tier request quotas are not emitted as rate-limit headers on every response, so this skill applies its own throttling and safety caps.

## Skill Safety Caps

Defaults from this skill:

- `OPEN_METEO_MAX_LOCATIONS_PER_RUN=10`
- `OPEN_METEO_MAX_DAYS_PER_RUN=366`
- `OPEN_METEO_MAX_HOURLY_VARIABLES_PER_RUN=12`
- `OPEN_METEO_MAX_DAILY_VARIABLES_PER_RUN=12`

These caps are local safeguards. Raise them only when there is a clear downstream need.

## Modeling Caveats

- Historical data is model/reanalysis driven, not a raw station dump.
- Returned coordinates may be snapped to the model grid and differ slightly from the requested coordinates.
- Local timezones can introduce daylight-saving effects in hourly point counts. Use `GMT` for deterministic hour counts across locations.

## Scope Boundaries

- This skill does not cover flood forecasts or river discharge. Use a separate Open-Meteo flood skill for that.
- This skill does not geocode place names. Resolve coordinates before calling it.
