# Open-Meteo Flood API Notes

## Endpoint

- Flood endpoint:
  - `https://flood-api.open-meteo.com/v1/flood`

## Request Parameters Used By This Skill

- `latitude`
  - One coordinate or comma-separated coordinate list.
- `longitude`
  - One coordinate or comma-separated coordinate list.
- `start_date`
  - Inclusive `YYYY-MM-DD`.
- `end_date`
  - Inclusive `YYYY-MM-DD`.
- `daily`
  - Comma-separated daily variables.
- `timezone`
  - Deterministic default in this skill is `GMT`.
- `ensemble`
  - Optional boolean.
  - When enabled with `river_discharge`, the API also returns per-member fields such as `river_discharge_member01`.
- `cell_selection`
  - This skill sends an explicit value for deterministic behavior.
  - Allowed values used by this skill: `nearest`, `land`, `sea`.
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
- `daily_units` and `daily`

## Daily Variables

Useful values for this skill:

- `river_discharge`
- `river_discharge_mean`
- `river_discharge_median`
- `river_discharge_max`
- `river_discharge_min`
- `river_discharge_p25`
- `river_discharge_p75`

## Error Shape

Open-Meteo error responses are JSON objects such as:

```json
{
  "error": true,
  "reason": "Data corrupted at path ''. Cannot initialize ForecastVariableDaily from invalid String value bad_var."
}
```

## Validation Strategy In This Skill

The script validates:

1. Top-level response object/list shape.
2. Presence of `daily` and `daily_units`.
3. Presence of requested variables in both data and units objects.
4. Equal lengths between `daily.time` and each requested variable array.
5. Parseability and range of returned dates.
6. Ensemble member field alignment when `--ensemble` is enabled and `river_discharge` is requested.
