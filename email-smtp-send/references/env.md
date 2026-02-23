# Environment Configuration

Configure SMTP connection and default message fields via env vars.

## Required

- `SMTP_HOST`: SMTP server hostname. Example: `mail.tsinghua.edu.cn`.
- `SMTP_USERNAME`: SMTP login username.
- `SMTP_PASSWORD`: SMTP login password or app password.

## Optional Connection Settings

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

Multiple recipients are supported by repeating `--to` or using comma-separated values.
