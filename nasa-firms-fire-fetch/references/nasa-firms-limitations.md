# NASA FIRMS Fire Fetch Limits

## Operational Constraints

- FIRMS requires a `MAP_KEY` for API use.
- `area/csv` accepts only 1 to 5 days per request, so longer windows are chunked.
- FIRMS exposes transaction quotas at the key level, and larger requests can count as multiple transactions.
- This skill adds local throttling and estimated-transaction caps to reduce accidental heavy calls.

## Data Caveats

- `LANDSAT_NRT` is limited to US and Canada.
- Source coverage and available date windows differ between NRT and SP products.
- Fire attributes vary by sensor, so this skill validates only the common spatial and acquisition fields plus generic CSV integrity.
- Empty fire results are valid and do not mean the request failed.

## Scope Boundaries

- This skill does not geocode place names.
- This skill does not do world or country scans.
- This skill does not classify severity or merge with weather, air quality, or flood data.
- Use separate skills for meteorology, flood, and air-quality context.
