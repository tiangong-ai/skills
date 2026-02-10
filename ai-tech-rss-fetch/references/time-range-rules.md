# Time Range Rules

## Supported Time Inputs
- Single date (`YYYY-MM-DD`)
- Explicit range (`start`, `end`)
- Relative expressions (`today`, `yesterday`, `last 7 days`, `this week`)

## Timezone Handling
- Prefer user-specified timezone.
- If missing, use runtime/local timezone and state the assumption.
- Convert all timestamps to one timezone before filtering/sorting.

## Default Behavior
- If no date/range is provided, select the most recent items by publish time.
- Use `max_items` as the default cap for no-range requests.

## Range Validation
- If `start > end`, return a format error and request corrected input.
- Range boundaries are inclusive.
- If range is valid but no records match, return `no rss items in selected range`.

## Missing Publish Time
- If an item has no `published_at` and a strict date/range filter is applied, drop the item by default.
- If no date/range filter is applied, keep undated items after dated items.
