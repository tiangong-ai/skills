# YouTube Video Search Constraints and Safety Notes

## API Characteristics

- `search.list` is materially more expensive than `videos.list`; use small page caps first.
- Discovery quality depends on `q`, channel constraints, and publish window choices made by the caller.
- This skill only discovers videos. It does not fetch comments.

## Built-in Protections

- Retry transient failures with exponential backoff.
- Respect `Retry-After` with a configurable upper cap.
- Throttle request rate with a minimum interval.
- Enforce hard caps on pages, results, and detail enrichment volume.
- Validate transport and response structure before emitting records.

## Scope Boundaries

- No scheduler/polling loop.
- No automatic query expansion inside the script.
- No sentiment analysis or ranking model inside the skill.
- No automatic follow-on comment fetch; chaining is external via OpenClaw.
