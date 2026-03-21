# NASA FIRMS API Notes

## Endpoints Used By This Skill

- Key status:
  - `/mapserver/mapkey_status/?MAP_KEY=...`
  - Returns JSON with current transaction information.
- Data availability:
  - `/api/data_availability/csv/[MAP_KEY]/[SENSOR]`
  - Returns CSV with `data_id`, `min_date`, `max_date`.
- Active fire area query:
  - `/api/area/csv/[MAP_KEY]/[SOURCE]/[AREA_COORDINATES]/[DAY_RANGE]/[DATE]`
  - `AREA_COORDINATES` uses `west,south,east,north`.
  - `DATE` is the inclusive chunk start date in `YYYY-MM-DD`.

## Supported Sources In This Skill

- `LANDSAT_NRT`
- `MODIS_NRT`
- `MODIS_SP`
- `VIIRS_NOAA20_NRT`
- `VIIRS_NOAA20_SP`
- `VIIRS_NOAA21_NRT`
- `VIIRS_SNPP_NRT`
- `VIIRS_SNPP_SP`

## Request Model

- FIRMS only accepts up to 5 days in one `area/csv` request.
- This skill accepts `--start-date` and `--end-date` and splits the window into compliant chunks automatically.
- The skill keeps one bbox per invocation to stay atomic and predictable.

## Response Shape

- Key status:
  - JSON object with fields such as `transaction_limit`, `current_transactions`, and `transaction_interval`.
- Data availability:
  - CSV records keyed by `data_id`.
- Area fire query:
  - CSV with sensor-specific fire attributes.
  - The skill expects at least:
    - `latitude`
    - `longitude`
    - `acq_date`
    - `acq_time`

## Validation Strategy In This Skill

The script validates:

1. Key configuration and basic source/bbox/date arguments.
2. Transport-level response headers and UTF-8 decoding.
3. JSON parse for key status and CSV parse for availability/fire responses.
4. Required fire columns and consistent headers across chunks.
5. Coordinate parseability and bbox inclusion.
6. Acquisition date/time parseability and requested-window inclusion.
