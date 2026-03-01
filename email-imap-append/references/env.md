# Environment Configuration

Configure IMAP connection and draft defaults for APPEND mode.

## Required

- `IMAP_HOST`: IMAP server hostname.
- `IMAP_USERNAME`: IMAP login username.
- `IMAP_PASSWORD`: IMAP login password or app password.

## Optional Connection Settings

- `IMAP_NAME`: account name label in output, default `default`.
- `IMAP_PORT`: default `993`.
- `IMAP_SSL`: `true|false`, default `true`.
- `IMAP_MAILBOX`: base mailbox fallback, default `INBOX`.
- `IMAP_CONNECT_TIMEOUT`: connection timeout seconds, default `20`.

## Optional Draft Defaults

These values are used when `append-draft` arguments are not provided:

- `IMAP_APPEND_MAILBOX`: target mailbox for APPEND, default `Drafts`.
- `IMAP_APPEND_FLAGS`: comma-separated IMAP flags, default `\Draft`.
- `IMAP_APPEND_FROM`: default sender address, default `IMAP_USERNAME`.
- `IMAP_APPEND_TO`: default To recipients, comma-separated.
- `IMAP_APPEND_CC`: default Cc recipients, comma-separated.
- `IMAP_APPEND_BCC`: default Bcc recipients, comma-separated.
- `IMAP_APPEND_SUBJECT`: default draft subject.
- `IMAP_APPEND_BODY`: default draft body.
- `IMAP_APPEND_CONTENT_TYPE`: `plain` or `html`, default `plain`.
- `IMAP_APPEND_MAX_ATTACHMENT_BYTES`: max bytes per attachment, default `26214400` (25 MiB).

## Commands

Validate config:

```bash
python3 scripts/imap_append.py check-config
```

Append one unsent draft:

```bash
python3 scripts/imap_append.py append-draft \
  --to reviewer@example.com \
  --subject "Draft: weekly update" \
  --body "Please review this draft."
```

Append HTML draft:

```bash
python3 scripts/imap_append.py append-draft \
  --content-type html \
  --subject "Draft: release note" \
  --body "<p>Release note draft.</p>"
```

Append draft with attachments:

```bash
python3 scripts/imap_append.py append-draft \
  --to reviewer@example.com \
  --subject "Draft: WG2 table package" \
  --body "Please review attached files." \
  --attach ./wg2-table.xlsx \
  --attach ./wg2-summary.docx
```
