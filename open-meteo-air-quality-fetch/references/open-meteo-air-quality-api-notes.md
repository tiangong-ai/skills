# Open-Meteo Air Quality API Notes

## Endpoint

- Air-quality endpoint:
  - `https://air-quality-api.open-meteo.com/v1/air-quality`

## Request Parameters Used By This Skill

- `latitude`
  - One coordinate or comma-separated coordinate list.
- `longitude`
  - One coordinate or comma-separated coordinate list.
- `start_date`
  - Inclusive `YYYY-MM-DD`.
- `end_date`
  - Inclusive `YYYY-MM-DD`.
- `hourly`
  - Comma-separated hourly variables.
- `domains`
  - One of:
    - `auto`
    - `cams_europe`
    - `cams_global`
- `timezone`
  - Deterministic default in this skill is `GMT`.
  - You can also pass `auto` or a named timezone.
- `cell_selection`
  - Allowed values used by this skill:
    - `nearest`
    - `land`
    - `sea`
- `apikey`
  - Optional. Only attached when configured.

## Response Shape

- One coordinate:
  - Top-level JSON object.
- Multiple coordinates:
  - Top-level JSON array of objects in request order.

Each response record usually contains:

- `latitude`
- `longitude`
- `generationtime_ms`
- `utc_offset_seconds`
- `timezone`
- `timezone_abbreviation`
- `elevation`
- `hourly_units` and `hourly`

## Error Shape

Open-Meteo error responses are JSON objects such as:

```json
{
  "reason": "Cannot initialize WeatherVariable from invalid String value tempeture_2m for key hourly",
  "error": true
}
```

## Validation Strategy In This Skill

The script validates:

1. Top-level response object/list shape.
2. Presence of requested `hourly` and `hourly_units` sections.
3. Presence of requested variables in both data and units objects.
4. Equal lengths between `hourly.time` and each requested variable array.
5. Parseability and range of returned timestamps.

## Useful Ecology-Oriented Variables

- `pm2_5`
- `pm10`
- `carbon_monoxide`
- `nitrogen_dioxide`
- `sulphur_dioxide`
- `ozone`
- `dust`
- `aerosol_optical_depth`
- `uv_index`
- `european_aqi`
- `us_aqi`
