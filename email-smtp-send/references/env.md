# Environment Configuration

Configure SMTP delivery, attachment limits, and optional IMAP sent-sync defaults via env vars.

## Required

- `SMTP_HOST`: SMTP server hostname. Example: `mail.tsinghua.edu.cn`.
- `SMTP_USERNAME`: SMTP login username.
- `SMTP_PASSWORD`: SMTP login password or app password.

## Optional SMTP Connection Settings

- `SMTP_PORT`: default `465`.
- `SMTP_SSL`: `true|false`, default `true`.
- `SMTP_STARTTLS`: `true|false`, default `false`.
- `SMTP_FROM`: sender address, default `SMTP_USERNAME`.
- `SMTP_CONNECT_TIMEOUT`: default `20` seconds.

## Optional Message Defaults

These values are used when `send` arguments are not provided:

- `SMTP_SUBJECT`: default email subject.
- `SMTP_BODY`: default email body.
- `SMTP_CONTENT_TYPE`: `plain` or `html`, default `plain`.
- `SMTP_MAX_ATTACHMENT_BYTES`: max bytes per attachment, default `26214400` (25 MiB).

## Optional Sent Sync Defaults (IMAP APPEND)

Enable this when SMTP-sent mail must appear in other clients' "Sent Items":

- `SMTP_SYNC_SENT`: `true|false`, default `false`.
- `SMTP_SYNC_SENT_REQUIRED`: `true|false`, default `false`. If true, sync failure returns non-zero.
- `SMTP_SENT_IMAP_HOST`: IMAP host for sent sync.
- `SMTP_SENT_IMAP_PORT`: default `993`.
- `SMTP_SENT_IMAP_SSL`: `true|false`, default `true`.
- `SMTP_SENT_IMAP_USERNAME`: IMAP username for sent sync.
- `SMTP_SENT_IMAP_PASSWORD`: IMAP password/app password for sent sync.
- `SMTP_SENT_IMAP_MAILBOX`: default `Sent Items`.
- `SMTP_SENT_IMAP_FLAGS`: default `\Seen`.
- `SMTP_SENT_IMAP_CONNECT_TIMEOUT`: default `20` seconds.

Compatibility fallback env keys are also supported:

- `IMAP_HOST`, `IMAP_PORT`, `IMAP_SSL`, `IMAP_USERNAME`, `IMAP_PASSWORD`, `IMAP_CONNECT_TIMEOUT`.

## Dependency

Sent-sync mode requires `imapclient`:

```bash
python3 -m pip install imapclient
```

## Commands

Validate config:

```bash
python3 scripts/smtp_send.py check-config
```

Send one email:

```bash
python3 scripts/smtp_send.py send \
  --to recipient@example.com \
  --subject "SMTP test" \
  --body "Hello from email-smtp-send"
```

Send with attachments:

```bash
python3 scripts/smtp_send.py send \
  --to recipient@example.com \
  --subject "Package delivery" \
  --body "See attached files." \
  --attach ./report.pdf \
  --attach ./appendix.xlsx
```

Send and sync to sent mailbox:

```bash
python3 scripts/smtp_send.py send \
  --to recipient@example.com \
  --subject "Synced send" \
  --body "Store this in Sent Items." \
  --sync-sent \
  --sent-mailbox "Sent Items"
```

Multiple recipients are supported by repeating `--to` or using comma-separated values.
