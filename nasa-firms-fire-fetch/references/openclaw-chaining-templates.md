# NASA FIRMS OpenClaw Templates

## Fire Source Verification

Use when public-opinion or news signals mention wildfire, field burning, or smoke source regions.

```text
Use $nasa-firms-fire-fetch.
Run:
python3 scripts/nasa_firms_fire_fetch.py fetch \
  --source VIIRS_NOAA20_NRT \
  --bbox [WEST,SOUTH,EAST,NORTH] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --check-availability \
  --pretty
Return only the JSON result.
```

## Key Probe

Use when setup is uncertain or rate-limit diagnostics are needed.

```text
Use $nasa-firms-fire-fetch.
Run:
python3 scripts/nasa_firms_fire_fetch.py check-config \
  --probe-map-key \
  --probe-source [FIRMS_SOURCE] \
  --pretty
Return only the JSON result.
```
