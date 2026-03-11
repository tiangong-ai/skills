# GDELT Mentions Fetch Constraints and Safety Notes

This skill uses public GDELT 2.0 file endpoints for `Global Mentions` exports.

## Constraints Confirmed from Official Documentation

- Update cadence:
  - GDELT 2.0 updates core feeds every 15 minutes.
- File granularity:
  - `lastupdate.txt` lists current snapshots by table.
  - `masterfilelist.txt` is the complete historical index and can be very large.
- Table format:
  - Mentions exports are ZIP-compressed tab-delimited files.

## Request Limits

The reviewed official GDELT pages and public index endpoints do not publish a clear numeric QPS, per-IP quota, or daily download ceiling for `lastupdate.txt`, `masterfilelist.txt`, or `*.mentions.CSV.zip`.

Because no official numeric request ceiling was confirmed, this skill applies client-side protections:

- Configurable timeout (`GDELT_TIMEOUT_SECONDS`)
- Retries with exponential backoff (`GDELT_MAX_RETRIES`, `GDELT_RETRY_BACKOFF_*`)
- Request throttling (`GDELT_MIN_REQUEST_INTERVAL_SECONDS`)
- Safety cap on selected files (`GDELT_MAX_FILES_PER_RUN`)
- Dry-run mode before download (`fetch --dry-run`)
- Transport and structure validation after download:
  - ZIP CRC check
  - UTF-8 strict decode validation
  - Column-count validation (`--expected-columns`, default `16`)
  - Optional issue quarantine (`--quarantine-dir`)
- Atomic invocation design: no internal polling loop in this skill.

## Practical Caveats

- `masterfilelist.txt` is large and full-file scanning is linear; use small time windows when possible.
- Mentions files can be high volume, so keep selection windows tight.
- If GDELT changes schema, callers should override `--expected-columns` with explicit change notes.
