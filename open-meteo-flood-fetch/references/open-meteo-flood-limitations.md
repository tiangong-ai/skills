# Open-Meteo Flood Fetch Limits

## Operational Constraints

- This skill uses the public flood endpoint by default.
- The public endpoint is suitable for development and evaluation, but production or commercial usage may require customer-specific Open-Meteo access and API key configuration.
- This skill keeps local request throttling and safety caps because public responses do not expose a stable per-request quota header contract.

## Data Caveats

- Flood data is based on GloFAS reanalysis and forecast products, not raw in-situ gauge feeds.
- Returned coordinates may be snapped to the hydrological grid and differ slightly from the requested coordinates.
- Ensemble mode can expand the response with many additional `river_discharge_memberNN` series, which increases payload size significantly.

## Skill Safety Caps

Defaults from this skill:

- `OPEN_METEO_FLOOD_MAX_LOCATIONS_PER_RUN=10`
- `OPEN_METEO_FLOOD_MAX_DAYS_PER_RUN=366`
- `OPEN_METEO_FLOOD_MAX_DAILY_VARIABLES_PER_RUN=8`

These caps are local safeguards. Raise them only when there is a clear downstream need.

## Scope Boundaries

- This skill does not geocode place names.
- This skill does not compute alert thresholds or classify flood severity.
- This skill does not merge with weather or soil variables. Use `open-meteo-historical-fetch` separately for that background context.
