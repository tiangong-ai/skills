# Environment Configuration

Configure mailbox accounts with one of the following methods.

## Option A: `IMAP_ACCOUNTS_JSON` (recommended)

Set one env variable containing a JSON array:

```bash
export IMAP_ACCOUNTS_JSON='[
  {
    "name": "work",
    "host": "imap.gmail.com",
    "port": 993,
    "username": "work@example.com",
    "password": "app-password-work",
    "mailbox": "INBOX",
    "ssl": true
  },
  {
    "name": "personal",
    "host": "imap.qq.com",
    "port": 993,
    "username": "personal@example.com",
    "password": "app-password-personal",
    "mailbox": "INBOX",
    "ssl": true
  }
]'
```

Account fields:
- Required: `host`, `username`, `password`
- Optional:
  - `name` (default `account-{n}`)
  - `port` (default `993`)
  - `mailbox` (default `INBOX`)
  - `ssl` (default `true`)

## Option B: `IMAP_ACCOUNT_IDS` + prefixed env vars

Define account ids, then define vars for each id:

```bash
export IMAP_ACCOUNT_IDS="work,personal"

export IMAP_WORK_HOST="imap.gmail.com"
export IMAP_WORK_PORT="993"
export IMAP_WORK_USERNAME="work@example.com"
export IMAP_WORK_PASSWORD="app-password-work"
export IMAP_WORK_MAILBOX="INBOX"
export IMAP_WORK_SSL="true"

export IMAP_PERSONAL_HOST="imap.qq.com"
export IMAP_PERSONAL_PORT="993"
export IMAP_PERSONAL_USERNAME="personal@example.com"
export IMAP_PERSONAL_PASSWORD="app-password-personal"
export IMAP_PERSONAL_MAILBOX="INBOX"
export IMAP_PERSONAL_SSL="true"
```

Naming rules:
- Account id token is normalized to uppercase with `_` separators.
- `work-mail` becomes `IMAP_WORK_MAIL_*`.
- Optional account display name: `IMAP_<ID>_NAME`.

## Optional Runtime Defaults

- `IMAP_CYCLES`: default cycles per account (`0`, `0` means forever)
- `IMAP_IDLE_MODE`: `poll` (default) or `idle`
- `IMAP_IDLE_SECONDS`: default IDLE wait seconds (`120`)
- `IMAP_POLL_SECONDS`: polling interval when mode is `poll` (`300`)
- `IMAP_MAX_MESSAGES`: max unread fetch per cycle (`10`)
- `IMAP_MARK_SEEN`: `true|false` (`false`)
- `IMAP_SNIPPET_CHARS`: preview length limit (`240`)
- `IMAP_CONNECT_TIMEOUT`: IMAP connect timeout seconds (`20`)
- `IMAP_RETRY_SECONDS`: delay between retries (`15`)

## IDLE Support Mode

This skill no longer performs runtime IDLE capability probing or capability caching.
Choose mode explicitly via env:

- `IMAP_IDLE_MODE=idle`: force IDLE.
- `IMAP_IDLE_MODE=poll`: force polling.

If your server does not support IDLE, set `IMAP_IDLE_MODE=poll`.

## OpenClaw Webhooks Forwarding

After each email is fetched, the script can POST to OpenClaw webhooks.

Core variables:
- `OPENCLAW_WEBHOOKS_ENABLED`: `true|false`. Defaults to `true` when `OPENCLAW_WEBHOOKS_TOKEN` exists, otherwise `false`.
- `OPENCLAW_WEBHOOKS_TOKEN`: required when forwarding is enabled. Sent as `Authorization: Bearer <token>`.
- `OPENCLAW_WEBHOOKS_BASE_URL`: default `http://127.0.0.1:18789`.
- `OPENCLAW_WEBHOOKS_MODE`: `agent` (default) or `wake`.
- `OPENCLAW_WEBHOOKS_ENDPOINT`: optional full endpoint override. Example `http://127.0.0.1:18789/hooks/email`.
- `OPENCLAW_WEBHOOKS_PATH`: endpoint prefix when override is not used (default `/hooks`).

Behavior variables:
- `OPENCLAW_WEBHOOKS_WAKE_MODE`: `now` (default) or `next-heartbeat`.
- `OPENCLAW_WEBHOOKS_DELIVER`: only used by `agent` mode, default `true`.
- `OPENCLAW_WEBHOOKS_TIMEOUT`: HTTP timeout seconds, default `15`.

Agent mode optional fields (`/hooks/agent` payload):
- `OPENCLAW_WEBHOOKS_NAME`
- `OPENCLAW_WEBHOOKS_AGENT_ID`
- `OPENCLAW_WEBHOOKS_CHANNEL` (default `last`)
- `OPENCLAW_WEBHOOKS_TO`
- `OPENCLAW_WEBHOOKS_MODEL`
- `OPENCLAW_WEBHOOKS_THINKING`
- `OPENCLAW_WEBHOOKS_AGENT_TIMEOUT_SECONDS` (maps to `timeoutSeconds`)
- `OPENCLAW_WEBHOOKS_SESSION_KEY_PREFIX` (script appends message id/uid)

Wake mode payload (`/hooks/wake`):
- Sends `{ "text": "...", "mode": "now|next-heartbeat" }`.

## Always-On Operation

Use long-running mode to keep receiving near-real-time notifications.

Foreground smoke-test command:

```bash
python3 scripts/imap_idle_fetch.py listen
```

Notes:
- `--cycles 0` (or `IMAP_CYCLES=0`) keeps listener running.
- If process exits, server cannot push to this client until process starts again.
- In production, you must run this listener under `systemd`, `launchd`, `supervisor`, or an equivalent daemon manager.
- Do not keep production listener processes attached to interactive exec/shell sessions; ending the session stops the process.

## Legacy Single-Account Fallback

If neither `IMAP_ACCOUNTS_JSON` nor `IMAP_ACCOUNT_IDS` is set, script falls back to:
- `IMAP_HOST`, `IMAP_USERNAME`, `IMAP_PASSWORD` (required)
- `IMAP_NAME`, `IMAP_PORT`, `IMAP_MAILBOX`, `IMAP_SSL` (optional)
