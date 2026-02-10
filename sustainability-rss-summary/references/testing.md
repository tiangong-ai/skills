# Testing

## 1) Single DOI smoke tests

```bash
export OPENALEX_EMAIL="you@example.com"
```

OpenAlex path expected:

```bash
python3 scripts/abstract_pipeline.py fetch \
  --doi "10.1177/014920639101700108" \
  --pretty
```

Fallback path expected (OpenAlex no abstract, S2 has abstract):

```bash
python3 scripts/abstract_pipeline.py fetch \
  --doi "10.1038/nature12373" \
  --pretty
```

## 2) Queue smoke test

```bash
python3 scripts/abstract_pipeline.py queue-add \
  --db /tmp/sustainability-rss-summary-test.db \
  --jsonl assets/rss-events.example.jsonl \
  --pretty

python3 scripts/abstract_pipeline.py queue-run \
  --db /tmp/sustainability-rss-summary-test.db \
  --limit 20 \
  --pretty

python3 scripts/abstract_pipeline.py queue-list \
  --db /tmp/sustainability-rss-summary-test.db \
  --pretty

# Optional: force-run pending retries for debugging
python3 scripts/abstract_pipeline.py queue-run \
  --db /tmp/sustainability-rss-summary-test.db \
  --force \
  --pretty
```

## Expected Results
- At least one DOI becomes `ready` from `openalex`.
- At least one DOI becomes `ready` from `semanticscholar`.
- Invalid DOI enters retry or `failed` depending on retry budget.
- If Semantic Scholar returns `429`, record transient retry without incrementing `retry_count`.
