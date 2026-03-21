# OpenClaw Chaining Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (query plan)

```text
Use $youtube-video-search.
Run:
python3 scripts/youtube_video_search.py search \
  --query "[QUERY_STRING]" \
  --published-after [YYYY-MM-DDTHH:MM:SSZ] \
  --published-before [YYYY-MM-DDTHH:MM:SSZ] \
  --order date \
  --max-pages [N] \
  --max-results [M] \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Search (candidate video IDs)

```text
Use $youtube-video-search.
Run:
python3 scripts/youtube_video_search.py search \
  --query "[QUERY_STRING]" \
  --published-after [YYYY-MM-DDTHH:MM:SSZ] \
  --published-before [YYYY-MM-DDTHH:MM:SSZ] \
  --order date \
  --max-pages [N] \
  --max-results [M] \
  --comment-count-min [K] \
  --output-dir [OUTPUT_DIR] \
  --pretty
Return only the JSON result.
```

3. Chain to comment fetch

```text
Use $youtube-video-search.
Run:
python3 scripts/youtube_video_search.py search \
  --query "[QUERY_STRING]" \
  --published-after [YYYY-MM-DDTHH:MM:SSZ] \
  --published-before [YYYY-MM-DDTHH:MM:SSZ] \
  --order date \
  --max-pages [N] \
  --max-results [M] \
  --output-dir [OUTPUT_DIR] \
  --pretty
Take artifacts.output_jsonl and provide it to $youtube-comments-fetch with --video-ids-file.
Return JSON plus the artifact path.
```
