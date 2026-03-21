# OpenClaw Chaining Templates

Use the skill atomically. Let OpenClaw decide when to chain with GDELT, Bluesky, YouTube, Regulations.gov, or other physical-source skills.

## Pattern 1: Verify Flood Claim

```text
Use $open-meteo-flood-fetch.
Fetch flood data for:
- coordinate: [LATITUDE,LONGITUDE]
- date window: [YYYY-MM-DD] to [YYYY-MM-DD]
- daily vars: river_discharge, river_discharge_p75
- ensemble: enabled
Use timezone GMT and return only the JSON result.
```

## Pattern 2: Cross-Location River Comparison

```text
Use $open-meteo-flood-fetch.
Fetch the same date window and daily variable set for each coordinate:
- [LAT1,LON1]
- [LAT2,LON2]
- [LAT3,LON3]
Use cell-selection nearest and return only the JSON result.
```

## Pattern 3: Pair With Weather Background

```text
Use $open-meteo-flood-fetch.
Fetch flood data for:
- coordinate: [LATITUDE,LONGITUDE]
- date window: [YYYY-MM-DD] to [YYYY-MM-DD]
- daily vars: river_discharge
Use timezone GMT and return only the JSON result before calling $open-meteo-historical-fetch for meteorological background.
```
