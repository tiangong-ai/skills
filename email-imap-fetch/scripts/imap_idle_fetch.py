#!/usr/bin/env python3
"""Listen for new emails via IMAP IDLE and fetch unread messages."""

from __future__ import annotations

import argparse
import email
import imaplib
import json
import os
import re
import select
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from typing import Any, Mapping, Sequence


TRUE_VALUES = {"1", "true", "yes", "on", "y"}
FALSE_VALUES = {"0", "false", "no", "off", "n"}
UID_RE = re.compile(rb"UID (\d+)")
SPACE_RE = re.compile(r"\s+")
HTML_TAG_RE = re.compile(r"<[^>]+>")
ACCOUNT_TOKEN_RE = re.compile(r"[^A-Za-z0-9_]+")
SESSION_TOKEN_RE = re.compile(r"[^A-Za-z0-9:_-]+")
WAKE_MODE_VALUES = {"now", "next-heartbeat"}
HOOK_MODE_VALUES = {"agent", "wake"}
IDLE_MODE_VALUES = {"idle", "poll"}


@dataclass(frozen=True)
class AccountConfig:
    name: str
    host: str
    username: str
    password: str
    port: int = 993
    mailbox: str = "INBOX"
    use_ssl: bool = True


@dataclass(frozen=True)
class ListenOptions:
    cycles: int
    idle_seconds: int
    poll_seconds: int
    idle_mode: str
    max_messages: int
    mark_seen: bool
    snippet_chars: int
    connect_timeout: int
    retry_seconds: int


@dataclass(frozen=True)
class OpenClawWebhookConfig:
    endpoint_url: str
    token: str
    mode: str
    timeout: int
    wake_mode: str
    deliver: bool
    name: str
    agent_id: str | None
    channel: str | None
    to: str | None
    model: str | None
    thinking: str | None
    timeout_seconds: int | None
    session_key_prefix: str | None


class IdleNotSupportedError(RuntimeError):
    """Raised when the IMAP server does not support the IDLE command."""


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_bool_value(raw: Any, label: str) -> bool:
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int):
        if raw in (0, 1):
            return bool(raw)
        raise ValueError(f"{label} must be true/false, got integer {raw!r}")

    text = str(raw).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    raise ValueError(f"{label} must be true/false, got {raw!r}")


def parse_int_value(raw: Any, label: str, minimum: int = 1) -> int:
    try:
        value = int(str(raw).strip())
    except Exception as exc:  # pragma: no cover - defensive
        raise ValueError(f"{label} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"{label} must be >= {minimum}, got {value}")
    return value


def parse_env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return parse_int_value(raw, name, minimum=minimum)


def parse_env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return parse_bool_value(raw, name)


def normalize_account_token(token: str) -> str:
    normalized = ACCOUNT_TOKEN_RE.sub("_", token.strip()).strip("_").upper()
    return normalized


def parse_account_dict(index: int, value: Mapping[str, Any]) -> AccountConfig:
    name = str(value.get("name") or f"account-{index}").strip()
    host = str(value.get("host") or "").strip()
    username = str(value.get("username") or "").strip()
    password = str(value.get("password") or "")
    mailbox = str(value.get("mailbox") or "INBOX").strip() or "INBOX"
    port = parse_int_value(value.get("port", 993), f"accounts[{index}].port", minimum=1)
    use_ssl = parse_bool_value(value.get("ssl", True), f"accounts[{index}].ssl")

    if not host:
        raise ValueError(f"accounts[{index}].host is required")
    if not username:
        raise ValueError(f"accounts[{index}].username is required")
    if not password:
        raise ValueError(f"accounts[{index}].password is required")

    return AccountConfig(
        name=name,
        host=host,
        username=username,
        password=password,
        port=port,
        mailbox=mailbox,
        use_ssl=use_ssl,
    )


def parse_accounts_from_json(raw: str) -> list[AccountConfig]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"IMAP_ACCOUNTS_JSON is invalid JSON: {exc}") from exc
    if not isinstance(payload, list) or not payload:
        raise ValueError("IMAP_ACCOUNTS_JSON must be a non-empty JSON array")

    accounts: list[AccountConfig] = []
    for idx, item in enumerate(payload, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"accounts[{idx}] must be a JSON object")
        accounts.append(parse_account_dict(idx, item))
    return accounts


def parse_accounts_from_ids(raw_ids: str, env: Mapping[str, str]) -> list[AccountConfig]:
    source_ids = [item.strip() for item in raw_ids.split(",") if item.strip()]
    if not source_ids:
        raise ValueError("IMAP_ACCOUNT_IDS must contain at least one account id")

    accounts: list[AccountConfig] = []
    for idx, source_id in enumerate(source_ids, start=1):
        token = normalize_account_token(source_id)
        if not token:
            raise ValueError(f"IMAP_ACCOUNT_IDS contains invalid token {source_id!r}")

        prefix = f"IMAP_{token}_"
        name = (env.get(prefix + "NAME") or source_id).strip()
        host = (env.get(prefix + "HOST") or "").strip()
        username = (env.get(prefix + "USERNAME") or "").strip()
        password = env.get(prefix + "PASSWORD") or ""
        mailbox = (env.get(prefix + "MAILBOX") or "INBOX").strip() or "INBOX"
        port = parse_int_value(env.get(prefix + "PORT", "993"), prefix + "PORT", minimum=1)
        use_ssl = parse_bool_value(env.get(prefix + "SSL", "true"), prefix + "SSL")

        if not host:
            raise ValueError(f"{prefix}HOST is required")
        if not username:
            raise ValueError(f"{prefix}USERNAME is required")
        if not password:
            raise ValueError(f"{prefix}PASSWORD is required")

        accounts.append(
            AccountConfig(
                name=name,
                host=host,
                username=username,
                password=password,
                port=port,
                mailbox=mailbox,
                use_ssl=use_ssl,
            )
        )
    return accounts


def parse_single_account(env: Mapping[str, str]) -> list[AccountConfig]:
    host = (env.get("IMAP_HOST") or "").strip()
    if not host:
        raise ValueError("Missing account config: set IMAP_ACCOUNTS_JSON or IMAP_ACCOUNT_IDS")

    username = (env.get("IMAP_USERNAME") or "").strip()
    password = env.get("IMAP_PASSWORD") or ""
    if not username:
        raise ValueError("IMAP_USERNAME is required when IMAP_HOST is used")
    if not password:
        raise ValueError("IMAP_PASSWORD is required when IMAP_HOST is used")

    return [
        AccountConfig(
            name=(env.get("IMAP_NAME") or "default").strip() or "default",
            host=host,
            username=username,
            password=password,
            port=parse_int_value(env.get("IMAP_PORT", "993"), "IMAP_PORT", minimum=1),
            mailbox=(env.get("IMAP_MAILBOX") or "INBOX").strip() or "INBOX",
            use_ssl=parse_bool_value(env.get("IMAP_SSL", "true"), "IMAP_SSL"),
        )
    ]


def load_accounts_from_env(env: Mapping[str, str] | None = None) -> list[AccountConfig]:
    env_map = dict(os.environ if env is None else env)
    raw_json = (env_map.get("IMAP_ACCOUNTS_JSON") or "").strip()
    if raw_json:
        return parse_accounts_from_json(raw_json)

    raw_ids = (env_map.get("IMAP_ACCOUNT_IDS") or "").strip()
    if raw_ids:
        return parse_accounts_from_ids(raw_ids, env_map)

    return parse_single_account(env_map)


def normalize_webhooks_path(path_value: str) -> str:
    path = path_value.strip() or "/hooks"
    if not path.startswith("/"):
        path = "/" + path
    path = path.rstrip("/")
    return path or "/hooks"


def build_endpoint_url(base_url: str, endpoint: str) -> str:
    if endpoint.startswith("http://") or endpoint.startswith("https://"):
        return endpoint
    return base_url.rstrip("/") + "/" + endpoint.lstrip("/")


def maybe_text(value: str | None) -> str | None:
    text = (value or "").strip()
    return text or None


def parse_optional_int(name: str, raw: str | None, minimum: int = 1) -> int | None:
    if raw is None or not raw.strip():
        return None
    return parse_int_value(raw, name, minimum=minimum)


def parse_idle_mode(raw: str) -> str:
    mode = (raw or "").strip().lower()
    if mode not in IDLE_MODE_VALUES:
        raise ValueError(
            f"IMAP_IDLE_MODE must be one of {sorted(IDLE_MODE_VALUES)}, got {raw!r}"
        )
    return mode


def load_openclaw_webhook_config(env: Mapping[str, str] | None = None) -> OpenClawWebhookConfig | None:
    env_map = dict(os.environ if env is None else env)
    token = (env_map.get("OPENCLAW_WEBHOOKS_TOKEN") or "").strip()
    enabled_default = "true" if token else "false"
    enabled = parse_bool_value(
        env_map.get("OPENCLAW_WEBHOOKS_ENABLED", enabled_default),
        "OPENCLAW_WEBHOOKS_ENABLED",
    )
    if not enabled:
        return None
    if not token:
        raise ValueError(
            "OPENCLAW_WEBHOOKS_TOKEN is required when OPENCLAW_WEBHOOKS_ENABLED=true"
        )

    mode = (env_map.get("OPENCLAW_WEBHOOKS_MODE") or "agent").strip().lower()
    if mode not in HOOK_MODE_VALUES:
        raise ValueError(
            f"OPENCLAW_WEBHOOKS_MODE must be one of {sorted(HOOK_MODE_VALUES)}, got {mode!r}"
        )

    base_url = (env_map.get("OPENCLAW_WEBHOOKS_BASE_URL") or "http://127.0.0.1:18789").strip()
    if not base_url.startswith(("http://", "https://")):
        raise ValueError("OPENCLAW_WEBHOOKS_BASE_URL must start with http:// or https://")
    base_url = base_url.rstrip("/")

    endpoint_override = (env_map.get("OPENCLAW_WEBHOOKS_ENDPOINT") or "").strip()
    if endpoint_override:
        endpoint_url = build_endpoint_url(base_url, endpoint_override)
    else:
        hooks_path = normalize_webhooks_path(
            env_map.get("OPENCLAW_WEBHOOKS_PATH", "/hooks")
        )
        endpoint_url = f"{base_url}{hooks_path}/{mode}"

    wake_mode = (env_map.get("OPENCLAW_WEBHOOKS_WAKE_MODE") or "now").strip().lower()
    if wake_mode not in WAKE_MODE_VALUES:
        raise ValueError(
            f"OPENCLAW_WEBHOOKS_WAKE_MODE must be one of {sorted(WAKE_MODE_VALUES)}, got {wake_mode!r}"
        )

    timeout = parse_int_value(
        env_map.get("OPENCLAW_WEBHOOKS_TIMEOUT", "15"),
        "OPENCLAW_WEBHOOKS_TIMEOUT",
        minimum=1,
    )
    deliver = parse_bool_value(
        env_map.get("OPENCLAW_WEBHOOKS_DELIVER", "true"),
        "OPENCLAW_WEBHOOKS_DELIVER",
    )

    return OpenClawWebhookConfig(
        endpoint_url=endpoint_url,
        token=token,
        mode=mode,
        timeout=timeout,
        wake_mode=wake_mode,
        deliver=deliver,
        name=(env_map.get("OPENCLAW_WEBHOOKS_NAME") or "Email").strip() or "Email",
        agent_id=maybe_text(env_map.get("OPENCLAW_WEBHOOKS_AGENT_ID")),
        channel=maybe_text(env_map.get("OPENCLAW_WEBHOOKS_CHANNEL", "last")),
        to=maybe_text(env_map.get("OPENCLAW_WEBHOOKS_TO")),
        model=maybe_text(env_map.get("OPENCLAW_WEBHOOKS_MODEL")),
        thinking=maybe_text(env_map.get("OPENCLAW_WEBHOOKS_THINKING")),
        timeout_seconds=parse_optional_int(
            "OPENCLAW_WEBHOOKS_AGENT_TIMEOUT_SECONDS",
            env_map.get("OPENCLAW_WEBHOOKS_AGENT_TIMEOUT_SECONDS"),
            minimum=1,
        ),
        session_key_prefix=maybe_text(env_map.get("OPENCLAW_WEBHOOKS_SESSION_KEY_PREFIX")),
    )


def decode_part_text(part: email.message.Message) -> str:
    payload = part.get_payload(decode=True)
    if payload is None:
        value = part.get_payload()
        return value if isinstance(value, str) else ""

    charsets = []
    charset = part.get_content_charset()
    if charset:
        charsets.append(charset)
    charsets.extend(["utf-8", "latin-1"])

    for encoding in charsets:
        try:
            return payload.decode(encoding, errors="replace")
        except LookupError:
            continue
    return payload.decode("utf-8", errors="replace")


def normalize_message_id(raw_message_id: str | None) -> str:
    value = str(raw_message_id or "").strip()
    if not value:
        return ""
    return value.replace("<", "").replace(">", "").strip().lower()


def extract_attachment_manifest(message: email.message.Message) -> list[dict[str, Any]]:
    manifest: list[dict[str, Any]] = []
    for part in message.walk():
        if part.get_content_maintype() == "multipart":
            continue
        disposition = (part.get_content_disposition() or "").strip().lower()
        filename = part.get_filename()
        if disposition not in {"attachment", "inline"} and not filename:
            continue
        payload = part.get_payload(decode=True)
        manifest.append(
            {
                "filename": str(filename or ""),
                "content_type": part.get_content_type(),
                "bytes": len(payload) if isinstance(payload, (bytes, bytearray)) else 0,
                "disposition": disposition,
            }
        )
    return manifest


def build_mail_ref(
    account: str,
    mailbox: str,
    uid: str | None,
    message_id_raw: str,
    message_id_norm: str,
    date_value: str,
) -> dict[str, str]:
    return {
        "account": account,
        "mailbox": mailbox,
        "uid": str(uid or ""),
        "message_id_raw": message_id_raw,
        "message_id_norm": message_id_norm,
        "date": date_value,
    }


def extract_snippet(message: email.message.Message, limit: int) -> str:
    plain_text = ""
    html_text = ""

    if message.is_multipart():
        for part in message.walk():
            if part.get_content_maintype() == "multipart":
                continue
            if part.get_content_disposition() == "attachment":
                continue
            content_type = part.get_content_type()
            if content_type == "text/plain" and not plain_text:
                plain_text = decode_part_text(part)
            elif content_type == "text/html" and not html_text:
                html_text = decode_part_text(part)
    else:
        content_type = message.get_content_type()
        if content_type == "text/plain":
            plain_text = decode_part_text(message)
        elif content_type == "text/html":
            html_text = decode_part_text(message)

    source = plain_text or HTML_TAG_RE.sub(" ", html_text)
    compact = SPACE_RE.sub(" ", source).strip()
    if len(compact) <= limit:
        return compact
    return compact[: max(1, limit - 3)].rstrip() + "..."


def open_imap_connection(account: AccountConfig, connect_timeout: int) -> imaplib.IMAP4:
    if account.use_ssl:
        client: imaplib.IMAP4 = imaplib.IMAP4_SSL(account.host, account.port, timeout=connect_timeout)
    else:
        client = imaplib.IMAP4(account.host, account.port, timeout=connect_timeout)

    status, _ = client.login(account.username, account.password)
    if status != "OK":
        raise RuntimeError(f"LOGIN failed for account={account.name}")
    return client


def safe_logout(client: imaplib.IMAP4 | None) -> None:
    if client is None:
        return
    try:
        client.logout()
    except Exception:
        return


def wait_for_idle(client: imaplib.IMAP4, idle_seconds: int) -> list[str]:
    sock = getattr(client, "sock", None)
    if sock is None:
        raise RuntimeError("IMAP socket not available, cannot run IDLE")

    tag = client._new_tag()
    tag_bytes = tag if isinstance(tag, bytes) else str(tag).encode("ascii")
    client.send(tag_bytes + b" IDLE\r\n")
    continuation = client.readline()
    if not continuation.startswith(b"+"):
        message = continuation.decode("utf-8", errors="replace").strip()
        upper = message.upper()
        if "BAD" in upper or "NOT SUPPORT" in upper or "UNKNOWN" in upper:
            raise IdleNotSupportedError(
                f"IDLE rejected by server: {message or 'missing continuation'}"
            )
        raise RuntimeError(f"IDLE rejected by server: {message or 'missing continuation'}")

    events: list[str] = []
    deadline = time.time() + idle_seconds
    while time.time() < deadline:
        wait_seconds = max(0.0, deadline - time.time())
        readable, _, _ = select.select([sock], [], [], wait_seconds)
        if not readable:
            break
        line = client.readline()
        if not line:
            break
        text = line.decode("utf-8", errors="replace").strip()
        if text:
            events.append(text)
        upper = text.upper()
        if "EXISTS" in upper or "RECENT" in upper:
            break

    client.send(b"DONE\r\n")
    for _ in range(20):
        readable, _, _ = select.select([sock], [], [], 2.0)
        if not readable:
            break
        line = client.readline()
        if not line:
            break
        if line.startswith(tag_bytes):
            break
    return events


def wait_for_poll(poll_seconds: int) -> list[str]:
    time.sleep(poll_seconds)
    return [f"POLL interval_seconds={poll_seconds}"]


def parse_fetch_payload(data: Any) -> tuple[str | None, bytes | None]:
    uid: str | None = None
    payload: bytes | None = None
    if not isinstance(data, list):
        return uid, payload

    for item in data:
        if isinstance(item, tuple):
            metadata = item[0] if len(item) >= 1 and isinstance(item[0], (bytes, bytearray)) else b""
            matched = UID_RE.search(bytes(metadata))
            if matched:
                uid = matched.group(1).decode("ascii", errors="ignore")
            if len(item) >= 2 and isinstance(item[1], (bytes, bytearray)):
                payload = bytes(item[1])
    return uid, payload


def fetch_unseen_messages(
    client: imaplib.IMAP4,
    account: AccountConfig,
    max_messages: int,
    mark_seen: bool,
    snippet_chars: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    status, data = client.search(None, "UNSEEN")
    if status != "OK":
        raise RuntimeError(f"SEARCH UNSEEN failed for account={account.name}")

    if not data or not data[0]:
        return [], []

    ids = data[0].split()
    ids = ids[-max_messages:]
    messages: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    fetch_query = "(UID RFC822)" if mark_seen else "(UID BODY.PEEK[])"

    for raw_seq in ids:
        seq = raw_seq.decode("ascii", errors="ignore")
        try:
            fetch_status, fetch_data = client.fetch(raw_seq, fetch_query)
            if fetch_status != "OK":
                raise RuntimeError(f"FETCH failed, status={fetch_status}")

            uid, raw_payload = parse_fetch_payload(fetch_data)
            if raw_payload is None:
                raise RuntimeError("FETCH returned no message payload")

            message = email.message_from_bytes(raw_payload, policy=policy.default)
            message_id_raw = str(message.get("Message-Id", ""))
            message_id_norm = normalize_message_id(message_id_raw)
            date_value = str(message.get("Date", ""))
            attachment_manifest = extract_attachment_manifest(message)
            mail_ref = build_mail_ref(
                account=account.name,
                mailbox=account.mailbox,
                uid=uid,
                message_id_raw=message_id_raw,
                message_id_norm=message_id_norm,
                date_value=date_value,
            )
            messages.append(
                {
                    "type": "message",
                    "at": utc_now_iso(),
                    "account": account.name,
                    "mailbox": account.mailbox,
                    "seq": seq,
                    "uid": uid,
                    "subject": str(message.get("Subject", "")),
                    "from": str(message.get("From", "")),
                    "to": str(message.get("To", "")),
                    "date": date_value,
                    "message_id": message_id_raw,
                    "message_id_raw": message_id_raw,
                    "message_id_norm": message_id_norm,
                    "snippet": extract_snippet(message, snippet_chars),
                    "attachment_count": len(attachment_manifest),
                    "attachment_manifest": attachment_manifest,
                    "mail_ref": mail_ref,
                }
            )
        except Exception as exc:
            errors.append(
                {
                    "type": "error",
                    "at": utc_now_iso(),
                    "account": account.name,
                    "mailbox": account.mailbox,
                    "seq": seq,
                    "error": str(exc),
                }
            )
    return messages, errors


def compose_webhook_message(record: Mapping[str, Any]) -> str:
    mail_ref_data = record.get("mail_ref")
    if isinstance(mail_ref_data, Mapping):
        mail_ref = {
            "account": str(mail_ref_data.get("account") or ""),
            "mailbox": str(mail_ref_data.get("mailbox") or ""),
            "uid": str(mail_ref_data.get("uid") or ""),
            "message_id_raw": str(mail_ref_data.get("message_id_raw") or ""),
            "message_id_norm": str(mail_ref_data.get("message_id_norm") or ""),
            "date": str(mail_ref_data.get("date") or ""),
        }
    else:
        message_id_raw = str(record.get("message_id_raw") or record.get("message_id") or "")
        mail_ref = build_mail_ref(
            account=str(record.get("account") or ""),
            mailbox=str(record.get("mailbox") or ""),
            uid=str(record.get("uid") or ""),
            message_id_raw=message_id_raw,
            message_id_norm=normalize_message_id(message_id_raw),
            date_value=str(record.get("date") or ""),
        )

    manifest_data = record.get("attachment_manifest")
    attachment_manifest: list[dict[str, Any]] = []
    if isinstance(manifest_data, list):
        for item in manifest_data:
            if not isinstance(item, Mapping):
                continue
            try:
                bytes_value = int(item.get("bytes") or 0)
            except Exception:
                bytes_value = 0
            attachment_manifest.append(
                {
                    "filename": str(item.get("filename") or ""),
                    "content_type": str(item.get("content_type") or ""),
                    "bytes": bytes_value,
                }
            )

    lines = [
        "New email received via IMAP mailbox listener.",
        f"Account: {record.get('account') or ''}",
        f"Mailbox: {record.get('mailbox') or ''}",
        f"From: {record.get('from') or ''}",
        f"To: {record.get('to') or ''}",
        f"Subject: {record.get('subject') or ''}",
        f"Date: {record.get('date') or ''}",
        f"UID: {record.get('uid') or ''}",
        f"Message-Id: {mail_ref['message_id_raw']}",
        "<<<MAIL_REF_JSON>>>",
        json.dumps(mail_ref, ensure_ascii=False, separators=(",", ":")),
        "<<<END_MAIL_REF_JSON>>>",
        "<<<ATTACHMENT_MANIFEST_JSON>>>",
        json.dumps(attachment_manifest, ensure_ascii=False, separators=(",", ":")),
        "<<<END_ATTACHMENT_MANIFEST_JSON>>>",
    ]
    snippet = str(record.get("snippet") or "").strip()
    if snippet:
        lines.append("Snippet:")
        lines.append(snippet)
    return "\n".join(lines)


def build_session_key(record: Mapping[str, Any], prefix: str) -> str:
    raw_component = (
        str(record.get("message_id_norm") or "").strip()
        or normalize_message_id(str(record.get("message_id_raw") or record.get("message_id") or ""))
        or str(record.get("uid") or "").strip()
        or str(record.get("seq") or "").strip()
        or str(int(time.time()))
    )
    component = SESSION_TOKEN_RE.sub("-", raw_component).strip("-")
    if not component:
        component = str(int(time.time()))
    return prefix + component


def build_openclaw_webhook_payload(
    record: Mapping[str, Any],
    config: OpenClawWebhookConfig,
) -> dict[str, Any]:
    message_text = compose_webhook_message(record)
    if config.mode == "wake":
        return {"text": message_text, "mode": config.wake_mode}

    payload: dict[str, Any] = {
        "message": message_text,
        "name": config.name,
        "wakeMode": config.wake_mode,
        "deliver": config.deliver,
    }
    if config.agent_id:
        payload["agentId"] = config.agent_id
    if config.channel:
        payload["channel"] = config.channel
    if config.to:
        payload["to"] = config.to
    if config.model:
        payload["model"] = config.model
    if config.thinking:
        payload["thinking"] = config.thinking
    if config.timeout_seconds is not None:
        payload["timeoutSeconds"] = config.timeout_seconds
    if config.session_key_prefix:
        payload["sessionKey"] = build_session_key(record, config.session_key_prefix)
    return payload


def send_openclaw_webhook(
    record: Mapping[str, Any],
    config: OpenClawWebhookConfig,
) -> tuple[int, str]:
    payload = build_openclaw_webhook_payload(record, config)
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url=config.endpoint_url,
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {config.token}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=config.timeout) as response:
            body = response.read(2048).decode("utf-8", errors="replace")
            return int(response.status), body
    except urllib.error.HTTPError as exc:
        detail = exc.read(2048).decode("utf-8", errors="replace")
        raise RuntimeError(
            f"OpenClaw webhook HTTP {exc.code}: {detail.strip() or exc.reason}"
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"OpenClaw webhook connection failed: {exc.reason}") from exc


def run_single_cycle(
    account: AccountConfig,
    options: ListenOptions,
) -> tuple[str, list[str], list[dict[str, Any]], list[dict[str, Any]]]:
    client: imaplib.IMAP4 | None = None
    try:
        client = open_imap_connection(account, options.connect_timeout)
        status, _ = client.select(account.mailbox, readonly=not options.mark_seen)
        if status != "OK":
            raise RuntimeError(f"SELECT failed for mailbox={account.mailbox!r}")

        wait_mode = options.idle_mode
        if options.idle_mode == "poll":
            wait_events = wait_for_poll(options.poll_seconds)
        else:
            try:
                wait_events = wait_for_idle(client, options.idle_seconds)
            except IdleNotSupportedError as exc:
                raise RuntimeError(
                    f"Server does not support IDLE. Set IMAP_IDLE_MODE=poll. detail={exc}"
                ) from exc

        messages, message_errors = fetch_unseen_messages(
            client=client,
            account=account,
            max_messages=options.max_messages,
            mark_seen=options.mark_seen,
            snippet_chars=options.snippet_chars,
        )
        return wait_mode, wait_events, messages, message_errors
    finally:
        safe_logout(client)


def emit_record(lock: threading.Lock, payload: Mapping[str, Any]) -> None:
    with lock:
        print(json.dumps(payload, ensure_ascii=False), flush=True)


def run_account_listener(
    account: AccountConfig,
    options: ListenOptions,
    webhook_config: OpenClawWebhookConfig | None,
    output_lock: threading.Lock,
    stop_event: threading.Event,
) -> int:
    cycle = 0
    error_count = 0

    while not stop_event.is_set():
        if options.cycles > 0 and cycle >= options.cycles:
            break
        cycle += 1

        emit_record(
            output_lock,
            {
                "type": "status",
                "at": utc_now_iso(),
                "account": account.name,
                "event": "cycle_started",
                "cycle": cycle,
            },
        )

        try:
            wait_mode, wait_events, messages, message_errors = run_single_cycle(
                account=account,
                options=options,
            )
            webhook_error_count = 0
            for record in messages:
                emit_record(output_lock, record)
                if webhook_config is None:
                    continue
                try:
                    status_code, _ = send_openclaw_webhook(record, webhook_config)
                    emit_record(
                        output_lock,
                        {
                            "type": "status",
                            "at": utc_now_iso(),
                            "account": account.name,
                            "event": "webhook_delivered",
                            "mode": webhook_config.mode,
                            "endpoint": webhook_config.endpoint_url,
                            "uid": record.get("uid"),
                            "message_id": record.get("message_id"),
                            "http_status": status_code,
                        },
                    )
                except Exception as exc:
                    webhook_error_count += 1
                    error_count += 1
                    emit_record(
                        output_lock,
                        {
                            "type": "error",
                            "at": utc_now_iso(),
                            "account": account.name,
                            "event": "webhook_failed",
                            "mode": webhook_config.mode,
                            "endpoint": webhook_config.endpoint_url,
                            "uid": record.get("uid"),
                            "message_id": record.get("message_id"),
                            "error": str(exc),
                        },
                    )
            for record in message_errors:
                error_count += 1
                emit_record(output_lock, record)
            emit_record(
                output_lock,
                {
                    "type": "status",
                    "at": utc_now_iso(),
                    "account": account.name,
                    "event": "cycle_completed",
                    "cycle": cycle,
                    "wait_mode": wait_mode,
                    "wait_events": wait_events,
                    "fetched_count": len(messages),
                    "fetch_error_count": len(message_errors),
                    "webhook_error_count": webhook_error_count,
                },
            )
        except Exception as exc:
            error_count += 1
            emit_record(
                output_lock,
                {
                    "type": "error",
                    "at": utc_now_iso(),
                    "account": account.name,
                    "cycle": cycle,
                    "error": str(exc),
                },
            )
            if options.cycles > 0 and cycle >= options.cycles:
                break
            if stop_event.is_set():
                break
            time.sleep(options.retry_seconds)

    emit_record(
        output_lock,
        {
            "type": "status",
            "at": utc_now_iso(),
            "account": account.name,
            "event": "listener_stopped",
            "cycles_completed": cycle,
            "errors": error_count,
        },
    )
    return error_count


def non_negative_int(value: str) -> int:
    parsed = parse_int_value(value, "value", minimum=0)
    return parsed


def positive_int(value: str) -> int:
    parsed = parse_int_value(value, "value", minimum=1)
    return parsed


def build_parser() -> argparse.ArgumentParser:
    default_cycles = parse_env_int("IMAP_CYCLES", default=0, minimum=0)
    default_idle_mode = parse_idle_mode(os.environ.get("IMAP_IDLE_MODE", "poll"))
    default_idle_seconds = parse_env_int("IMAP_IDLE_SECONDS", default=120, minimum=1)
    default_poll_seconds = parse_env_int("IMAP_POLL_SECONDS", default=300, minimum=1)
    default_max_messages = parse_env_int("IMAP_MAX_MESSAGES", default=10, minimum=1)
    default_mark_seen = parse_env_bool("IMAP_MARK_SEEN", default=False)
    default_snippet_chars = parse_env_int("IMAP_SNIPPET_CHARS", default=240, minimum=1)
    default_connect_timeout = parse_env_int("IMAP_CONNECT_TIMEOUT", default=20, minimum=1)
    default_retry_seconds = parse_env_int("IMAP_RETRY_SECONDS", default=15, minimum=1)

    parser = argparse.ArgumentParser(
        description="Listen for new emails with IMAP IDLE and fetch unread messages as JSONL.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "check-config",
        help="Validate env config and print sanitized account summary JSON.",
    )

    listen_parser = subparsers.add_parser(
        "listen",
        help="Run IMAP IDLE listener and fetch unread messages.",
    )
    listen_parser.add_argument(
        "--cycles",
        type=non_negative_int,
        default=default_cycles,
        help="IDLE cycles per account. 0 means run forever.",
    )
    listen_parser.add_argument(
        "--idle-seconds",
        type=positive_int,
        default=default_idle_seconds,
        help="Seconds to keep each IDLE call open before DONE.",
    )
    listen_parser.add_argument(
        "--poll-seconds",
        type=positive_int,
        default=default_poll_seconds,
        help="Seconds between checks when using polling mode.",
    )
    listen_parser.add_argument(
        "--idle-mode",
        choices=sorted(IDLE_MODE_VALUES),
        default=default_idle_mode,
        help="IDLE behavior: idle (force IDLE) or poll (force polling).",
    )
    listen_parser.add_argument(
        "--max-messages",
        type=positive_int,
        default=default_max_messages,
        help="Max unread messages fetched each cycle per account.",
    )
    listen_parser.add_argument(
        "--snippet-chars",
        type=positive_int,
        default=default_snippet_chars,
        help="Max snippet length for each message.",
    )
    listen_parser.add_argument(
        "--connect-timeout",
        type=positive_int,
        default=default_connect_timeout,
        help="IMAP connect timeout in seconds.",
    )
    listen_parser.add_argument(
        "--retry-seconds",
        type=positive_int,
        default=default_retry_seconds,
        help="Retry delay after account-level failure.",
    )

    bool_action = getattr(argparse, "BooleanOptionalAction", None)
    if bool_action is not None:
        listen_parser.add_argument(
            "--mark-seen",
            action=bool_action,
            default=default_mark_seen,
            help="Mark fetched emails as seen (default from IMAP_MARK_SEEN).",
        )
    else:
        group = listen_parser.add_mutually_exclusive_group()
        group.add_argument("--mark-seen", action="store_true", dest="mark_seen")
        group.add_argument("--no-mark-seen", action="store_false", dest="mark_seen")
        listen_parser.set_defaults(mark_seen=default_mark_seen)

    return parser


def command_check_config(
    accounts: Sequence[AccountConfig],
    webhook_config: OpenClawWebhookConfig | None,
) -> int:
    idle_mode = parse_idle_mode(os.environ.get("IMAP_IDLE_MODE", "poll"))
    poll_seconds = parse_env_int("IMAP_POLL_SECONDS", default=300, minimum=1)

    payload = {
        "account_count": len(accounts),
        "accounts": [
            {
                "name": account.name,
                "host": account.host,
                "port": account.port,
                "username": account.username,
                "mailbox": account.mailbox,
                "ssl": account.use_ssl,
            }
            for account in accounts
        ],
        "openclaw_webhooks": {
            "enabled": webhook_config is not None,
        },
        "idle_runtime": {
            "idle_mode": idle_mode,
            "poll_seconds": poll_seconds,
        },
    }
    if webhook_config is not None:
        payload["openclaw_webhooks"] = {
            "enabled": True,
            "endpoint_url": webhook_config.endpoint_url,
            "mode": webhook_config.mode,
            "wake_mode": webhook_config.wake_mode,
            "deliver": webhook_config.deliver,
            "name": webhook_config.name,
            "agent_id": webhook_config.agent_id,
            "channel": webhook_config.channel,
            "to": webhook_config.to,
            "model": webhook_config.model,
            "thinking": webhook_config.thinking,
            "timeout_seconds": webhook_config.timeout_seconds,
            "session_key_prefix": webhook_config.session_key_prefix,
            "timeout": webhook_config.timeout,
        }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def command_listen(
    accounts: Sequence[AccountConfig],
    webhook_config: OpenClawWebhookConfig | None,
    args: argparse.Namespace,
) -> int:
    options = ListenOptions(
        cycles=args.cycles,
        idle_mode=args.idle_mode,
        idle_seconds=args.idle_seconds,
        poll_seconds=args.poll_seconds,
        max_messages=args.max_messages,
        mark_seen=args.mark_seen,
        snippet_chars=args.snippet_chars,
        connect_timeout=args.connect_timeout,
        retry_seconds=args.retry_seconds,
    )
    output_lock = threading.Lock()
    stop_event = threading.Event()

    emit_record(
        output_lock,
        {
            "type": "status",
            "at": utc_now_iso(),
            "event": "listener_started",
            "accounts": [account.name for account in accounts],
            "options": {
                "cycles": options.cycles,
                "idle_mode": options.idle_mode,
                "idle_seconds": options.idle_seconds,
                "poll_seconds": options.poll_seconds,
                "max_messages": options.max_messages,
                "mark_seen": options.mark_seen,
                "snippet_chars": options.snippet_chars,
                "openclaw_webhooks_enabled": webhook_config is not None,
                "openclaw_webhook_mode": webhook_config.mode if webhook_config is not None else None,
                "openclaw_webhook_endpoint": webhook_config.endpoint_url if webhook_config is not None else None,
            },
        },
    )

    total_errors = 0
    try:
        with ThreadPoolExecutor(max_workers=max(1, len(accounts))) as executor:
            futures = [
                executor.submit(
                    run_account_listener,
                    account,
                    options,
                    webhook_config,
                    output_lock,
                    stop_event,
                )
                for account in accounts
            ]
            for future in futures:
                total_errors += future.result()
    except KeyboardInterrupt:
        stop_event.set()
        emit_record(
            output_lock,
            {
                "type": "status",
                "at": utc_now_iso(),
                "event": "interrupted",
            },
        )
        return 130

    emit_record(
        output_lock,
        {
            "type": "status",
            "at": utc_now_iso(),
            "event": "listener_finished",
            "total_errors": total_errors,
        },
    )
    return 1 if total_errors > 0 else 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        accounts = load_accounts_from_env()
        webhook_config = load_openclaw_webhook_config()
    except ValueError as exc:
        print(f"IMAP_IDLE_ERR reason=config_error message={exc}", file=sys.stderr)
        return 2

    if args.command == "check-config":
        return command_check_config(accounts, webhook_config)
    if args.command == "listen":
        return command_listen(accounts, webhook_config, args)

    print(f"IMAP_IDLE_ERR reason=unknown_command command={args.command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
