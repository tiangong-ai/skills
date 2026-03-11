# GDELT Events Fetch Constraints and Safety Notes

This skill uses public GDELT 2.0 file endpoints for `Events` exports.

## Constraints Confirmed from Official Documentation

- Update cadence:
  - GDELT 2.0 updates core feeds every 15 minutes.
- File granularity:
  - `lastupdate.txt` lists current snapshots by table.
  - `masterfilelist.txt` is the complete historical index and can be very large.
- Table format:
  - Events exports are ZIP-compressed tab-delimited CSV.

## Limits for This Skill's Requests

The reviewed official pages do not publish a numeric QPS/quota specifically for
`lastupdate.txt`, `masterfilelist.txt`, or `*.export.CSV.zip` downloads.

Because of that, this skill applies client-side protections:

- Configurable timeout (`GDELT_TIMEOUT_SECONDS`)
- Retries with exponential backoff (`GDELT_MAX_RETRIES`, `GDELT_RETRY_BACKOFF_*`)
- Request throttling (`GDELT_MIN_REQUEST_INTERVAL_SECONDS`)
- Safety cap on selected files (`GDELT_MAX_FILES_PER_RUN`)
- Dry-run mode before download (`fetch --dry-run`)
- Transport and structure validation after download:
  - ZIP CRC check
  - UTF-8 decode validation
  - Column-count validation (`--expected-columns`, default `61`)
  - Optional issue quarantine (`--quarantine-dir`)
- Atomic invocation design: no internal polling loop in this skill.
