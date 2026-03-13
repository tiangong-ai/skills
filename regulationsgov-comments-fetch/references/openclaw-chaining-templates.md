# Regulations.gov OpenClaw Chaining Templates (Canonical)

This is the canonical chaining reference for:

- `$regulationsgov-comments-fetch`
- `$regulationsgov-comment-detail-fetch`

## Template A: Time-Window Comments -> Detail Enrichment

Use when you need a policy-opinion window first, then enrich selected comments with full detail payloads.

### Step 1: Comments window fetch

```text
Use $regulationsgov-comments-fetch.
Run:
python3 scripts/regulationsgov_comments_fetch.py fetch \
  --filter-mode last-modified \
  --start-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --end-datetime [YYYY-MM-DDTHH:MM:SSZ] \
  --agency-id [OPTIONAL_AGENCY] \
  --max-pages [P] \
  --max-records [R] \
  --output-dir [COMMENTS_DIR] \
  --quarantine-dir [COMMENTS_QUAR] \
  --pretty
Return JSON only.
```

### Step 2: Feed list artifact into detail skill

Use the `output_file` returned in Step 1 directly as ID source.

```text
Use $regulationsgov-comment-detail-fetch.
Run:
python3 scripts/regulationsgov_comment_detail_fetch.py fetch \
  --comment-ids-file [COMMENTS_OUTPUT_FILE_FROM_STEP1] \
  --max-comments [N] \
  --include attachments \
  --no-fail-on-item-error \
  --output-dir [DETAIL_DIR] \
  --quarantine-dir [DETAIL_QUAR] \
  --pretty
Return JSON only.
```

### Step 3: Unified return contract

Return one final summary including:

- Step 1: `pages_fetched`, `records_fetched`, `output_file`, validation summary
- Step 2: `requested_count`, `success_count`, `failure_count`, `output_file`, validation summary
- Any quarantine artifact paths

## Template B: Fast List-Only Monitoring (No Detail)

Use when trend monitoring is enough and full comment body enrichment is unnecessary.

```text
Use $regulationsgov-comments-fetch only.
Run window fetch with save-response enabled.
Return summary + output_file for downstream optional use.
```

## Input/Output Contract Notes

- Step 1 output file is JSONL where each line is a comment object containing `id`.
- Detail skill accepts this file directly via `--comment-ids-file`.
- Keep atomic skills unchanged; orchestration belongs to OpenClaw flow.

## Safety Notes

- Detail stage is higher request cost (`N+1` per comment ID).
- Keep explicit caps (`--max-records` in list stage, `--max-comments` in detail stage).
- Prefer `--no-fail-on-item-error` in large batches to preserve partial success.
