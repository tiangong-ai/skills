# Open-Meteo Historical API Notes

## Endpoint

- Historical archive endpoint:
  - `https://archive-api.open-meteo.com/v1/archive`

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
- `daily`
  - Comma-separated daily variables.
- `timezone`
  - Deterministic default in this skill is `GMT`.
  - You can also pass `auto` or a named timezone.
- `models`
  - Optional comma-separated model selection such as `era5`.
- `temperature_unit`
  - Optional.
- `wind_speed_unit`
  - Optional.
- `precipitation_unit`
  - Optional.
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
- `daily_units` and `daily`

## Error Shape

Open-Meteo error responses are JSON objects such as:

```json
{
  "reason": "End-date must be larger or equals than start-date",
  "error": true
}
```

## Validation Strategy In This Skill

The script validates:

1. Top-level response object/list shape.
2. Presence of requested `hourly` and `daily` sections.
3. Presence of requested variables in both data and units objects.
4. Equal lengths between the section `time` array and each requested variable array.
5. Parseability and range of returned timestamps/dates.

## Useful Ecology-Oriented Variables

Examples for environmental verification:

- Hourly:
  - `temperature_2m`
  - `relative_humidity_2m`
  - `precipitation`
  - `wind_speed_10m`
  - `soil_temperature_0cm`
  - `soil_moisture_0_to_7cm`
  - `soil_moisture_7_to_28cm`
- Daily:
  - `temperature_2m_max`
  - `temperature_2m_min`
  - `precipitation_sum`
  - `rain_sum`
  - `et0_fao_evapotranspiration`
  - `evapotranspiration`
