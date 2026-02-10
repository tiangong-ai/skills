# Time Range Rules

## Daily Report
- Use one calendar date.
- If user provides "today", resolve to current local date of execution context.
- If user provides a full range for daily mode, keep daily structure and summarize the range as one daily-style digest.

## Weekly Report
- Use Monday to Sunday as the default week boundary.
- Accept explicit week expressions (`YYYY-W##`) or explicit date range.
- When only a date is provided in weekly mode, compute the week containing that date.

## Flexible Range
- Accept explicit start/end date in either mode.
- Include both boundaries (inclusive range).
- Preserve the report structure selected by `report_type`.

## Timezone Handling
- Prefer user-specified timezone.
- If missing, use runtime/local timezone and state the assumption in the report.
- Convert all timestamps to one timezone before ranking and grouping.

## Range Validation
- If `start > end`, return a format error and request corrected input.
- If the range is valid but no records match, output the full report structure with "no data" notes.
