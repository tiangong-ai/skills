# Output Rules

## Content Filtering
1. Remove duplicates.
- Treat records as duplicates when `title + link` matches exactly.
- If `link` is missing, dedupe by `normalized title + same day`.

2. Remove irrelevant items.
- Keep only items aligned with report scope (project/activity/AI tech signal).
- Drop low-information noise (for example empty status pings).

3. Apply keyword filters.
- Include filter: keep items containing any `include_keywords`.
- Exclude filter: remove items containing any `exclude_keywords`.
- Apply include first, then exclude.

## Importance Ranking
Use a simple additive score to order items in "completed", "risk", and "plan" sections:

```text
importance_score =
  base_importance(1-5)
  + status_weight
  + event_weight
  + recency_weight
```

- `status_weight`: `done=2`, `blocked=3`, `delayed=3`, `in_progress=1`, others `0`
- `event_weight`: major release/incident/security/event markers `+2`
- `recency_weight`: item in last 24h (daily) or last 2 days (weekly) `+1`

## Output Format Guidance
- `markdown`: default and recommended.
- `text`: strip markdown headings/list markers while preserving section order.
- `html`: convert headings/lists/links from markdown.
- `pdf`: generate from markdown/html if converter is available; otherwise return markdown plus a clear conversion note.

## Error Handling
- Invalid input format: report which input failed and continue with valid inputs.
- Missing critical fields (`report_type`, date/range): request clarification.
- Missing optional fields (`link`, `owner`, `tags`): continue and omit gracefully.

## Quality Control Checklist
- Completeness: all required sections are present.
- Coverage: completed items, delayed items, risks, and next plan are all represented when data exists.
- Clarity: concise summary, no duplicate bullets, clear section boundaries.
- Traceability: include reference links when `include_links=true`.
