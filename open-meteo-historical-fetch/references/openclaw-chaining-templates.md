# OpenClaw Chaining Templates

Use the skill atomically. Let OpenClaw decide when to chain with GDELT, Bluesky, YouTube, Regulations.gov, or other physical-source skills.

## Pattern 1: Verify Environment Claim

```text
Use $open-meteo-historical-fetch.
Fetch historical weather and shallow-soil data for:
- coordinate: [LATITUDE,LONGITUDE]
- date window: [YYYY-MM-DD] to [YYYY-MM-DD]
- hourly vars: temperature_2m, precipitation, wind_speed_10m, soil_moisture_0_to_7cm
- daily vars: precipitation_sum, evapotranspiration
Use timezone GMT and return only the JSON result.
```

## Pattern 2: Cross-Location Background Context

```text
Use $open-meteo-historical-fetch.
Fetch the same date window and variable set for each coordinate:
- [LAT1,LON1]
- [LAT2,LON2]
- [LAT3,LON3]
Return only the JSON result and compare validation_summary.record_summaries.
```

## Pattern 3: Before Air-Quality Interpretation

```text
Use $open-meteo-historical-fetch.
Fetch historical meteorology for:
- coordinate: [LATITUDE,LONGITUDE]
- date window: [YYYY-MM-DD] to [YYYY-MM-DD]
- hourly vars: temperature_2m, relative_humidity_2m, wind_speed_10m, precipitation
Use timezone GMT and return only the JSON result.
```
