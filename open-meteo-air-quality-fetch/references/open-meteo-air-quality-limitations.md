# Open-Meteo Air Quality Fetch Limits

## Operational Constraints

- This skill uses the public air-quality endpoint by default.
- The public endpoint is suitable for development and evaluation, but production or commercial usage may require customer-specific Open-Meteo access and API key configuration.
- This skill keeps local request throttling and safety caps because public responses do not expose a stable per-request quota header contract.

## Data Caveats

- Open-Meteo air-quality data is a modeled background field, not a direct station-observation feed.
- European-only variables such as `ammonia` or pollen fields are not globally available.
- Coverage combines CAMS Europe and CAMS Global, and `domains=auto` may switch data provenance by location and time.
- This skill validates structure and timing only; it does not infer exposure risk or source attribution.

## Scope Boundaries

- This skill does not geocode place names.
- This skill does not merge with OpenAQ or other station sources.
- This skill does not compute alert thresholds or health advisories.
- Use `openaq-data-fetch` for station or provider-centric observation retrieval.
