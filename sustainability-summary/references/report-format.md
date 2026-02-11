# RAG Context Format

`time_report.py` outputs JSON context for agent-side summarization.

## Top-level schema

```json
{
  "query": {
    "period": "daily|weekly|monthly|custom",
    "period_label": "Daily|Weekly|Monthly|Custom",
    "start_utc": "2026-02-10T00:00:00Z",
    "end_utc": "2026-02-11T00:00:00Z",
    "selected_fields": ["entry_id", "title", "..."],
    "max_records": 80,
    "max_per_feed": 0
  },
  "dataset": {
    "generated_at_utc": "2026-02-10T18:00:00Z",
    "has_entry_content": true,
    "records_in_range_before_limit": 240,
    "records_returned": 80,
    "truncated": true
  },
  "aggregates": {
    "feed_counts_top": [{"feed": "example.com", "count": 20}],
    "fulltext_status_counts": {"ready": 60, "failed": 20},
    "top_keywords": [{"keyword": "agent", "count": 12}]
  },
  "records": [
    {
      "entry_id": 123,
      "timestamp_utc": "2026-02-10T08:00:00Z",
      "feed_title": "Example Feed",
      "title": "Example title",
      "url": "https://...",
      "summary": "..."
    }
  ]
}
```

## Agent summary guidance

1. Treat `records` as the primary evidence.
2. Use `aggregates` as secondary hints for trend framing.
3. If `dataset.truncated=true`, explicitly mention coverage limitation.
4. Anchor important claims with `entry_id` and URL references.
5. Avoid claims that cannot be traced to retrieved evidence.
