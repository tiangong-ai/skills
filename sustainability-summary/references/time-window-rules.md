# Time Window Rules

## Window Semantics

- All windows use UTC.
- Window boundary is left-closed, right-open: `[start, end)`.
- Entries with timestamp exactly equal to `end` are excluded from current retrieval result.

## Supported Periods

### Daily
- Input: `--period daily [--date YYYY-MM-DD]`
- Start: `date 00:00:00Z`
- End: `date + 1 day 00:00:00Z`

### Weekly
- Input: `--period weekly [--date YYYY-MM-DD]`
- Week starts on Monday (ISO-like behavior).
- Start: Monday `00:00:00Z` of anchor week.
- End: next Monday `00:00:00Z`.

### Monthly
- Input: `--period monthly [--date YYYY-MM-DD]`
- Start: first day of month `00:00:00Z`.
- End: first day of next month `00:00:00Z`.

### Custom
- Input: `--period custom --start ... --end ...`
- `--start/--end` accept:
  - `YYYY-MM-DD`
  - ISO datetime (with or without timezone)
- For date-only `--end`, tool interprets end as next-day boundary.
  - Example: `--start 2026-02-01 --end 2026-02-10`
  - Effective range: `[2026-02-01T00:00:00Z, 2026-02-11T00:00:00Z)`

## Timestamp Selection

For each entry in RAG retrieval, choose timestamp in this order:
1. `published_at` if parseable
2. `first_seen_at`
3. `last_seen_at`

If all fail to parse, skip that entry from time-range filtering.
