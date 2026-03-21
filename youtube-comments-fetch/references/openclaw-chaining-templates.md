# OpenClaw Chaining Templates

Use these templates directly in OpenClaw and only replace bracketed placeholders.

1. Recon (dry-run)

```text
Use $youtube-comments-fetch.
Run:
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file [VIDEO_IDS_FILE] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --max-videos [N] \
  --max-thread-pages [M] \
  --dry-run \
  --pretty
Return only the JSON result.
```

2. Fetch (windowed comments)

```text
Use $youtube-comments-fetch.
Run:
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file [VIDEO_IDS_FILE] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --time-field published \
  --include-replies \
  --order time \
  --max-videos [N] \
  --max-thread-pages [M] \
  --max-reply-pages [R] \
  --max-comments [K] \
  --output-dir [OUTPUT_DIR] \
  --pretty
Return only the JSON result.
```

3. Validate (small-scope quality gate)

```text
Use $youtube-comments-fetch.
Run:
python3 scripts/youtube_comments_fetch.py fetch \
  --video-ids-file [VIDEO_IDS_FILE] \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --max-videos 1 \
  --max-thread-pages 1 \
  --max-reply-pages 1 \
  --max-comments 50 \
  --pretty
Check validation_summary.total_issue_count, failures, and fetch_summary.record_count.
Return JSON plus one-line pass/fail verdict.
```
