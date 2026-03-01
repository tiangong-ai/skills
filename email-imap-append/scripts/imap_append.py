#!/usr/bin/env python3
"""Append unsent draft emails to IMAP mailbox via APPEND."""

from __future__ import annotations

import argparse
import imaplib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid
from typing import Any, Mapping, Sequence


TRUE_VALUES = {"1", "true", "yes", "on", "y"}
FALSE_VALUES = {"0", "false", "no", "off", "n"}
CONTENT_TYPES = {"plain", "html"}
APPENDUID_RE = re.compile(r"\[APPENDUID\s+(\d+)\s+(\d+)\]", re.IGNORECASE)


@dataclass(frozen=True)
class AccountConfig:
    name: str
    host: str
    username: str
    password: str
    mailbox: str
    port: int
    use_ssl: bool


@dataclass(frozen=True)
class DraftDefaults:
    mailbox: str
    flags: list[str]
    from_addr: str
    to_addrs: list[str]
    cc_addrs: list[str]
    bcc_addrs: list[str]
    subject: str
    body: str
    content_type: str
    connect_timeout: int


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


def parse_content_type(raw: str, label: str) -> str:
    value = str(raw).strip().lower()
    if value not in CONTENT_TYPES:
        raise ValueError(f"{label} must be one of {sorted(CONTENT_TYPES)}, got {raw!r}")
    return value


def parse_recipients(values: Sequence[str]) -> list[str]:
    recipients: list[str] = []
    for item in values:
        parts = [value.strip() for value in item.split(",")]
        for part in parts:
            if part:
                recipients.append(part)
    return recipients


def parse_recipients_env(value: str | None) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    return parse_recipients([raw])


def parse_flags(raw: str | None, label: str) -> list[str]:
    text = str(raw or "").strip()
    if not text:
        return []

    parts: list[str] = []
    for chunk in text.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        parts.extend(token for token in chunk.split() if token)

    normalized: list[str] = []
    seen: set[str] = set()
    for part in parts:
        flag = part if part.startswith("\\") else f"\\{part}"
        key = flag.upper()
        if key not in seen:
            seen.add(key)
            normalized.append(flag)
    return normalized


def load_account_from_env(env: Mapping[str, str] | None = None) -> AccountConfig:
    env_map = dict(os.environ if env is None else env)
    host = str(env_map.get("IMAP_HOST") or "").strip()
    username = str(env_map.get("IMAP_USERNAME") or "").strip()
    password = str(env_map.get("IMAP_PASSWORD") or "")
    if not host:
        raise ValueError("IMAP_HOST is required")
    if not username:
        raise ValueError("IMAP_USERNAME is required")
    if not password:
        raise ValueError("IMAP_PASSWORD is required")

    mailbox = str(env_map.get("IMAP_MAILBOX") or "INBOX").strip() or "INBOX"
    return AccountConfig(
        name=str(env_map.get("IMAP_NAME") or "default").strip() or "default",
        host=host,
        username=username,
        password=password,
        mailbox=mailbox,
        port=parse_int_value(env_map.get("IMAP_PORT", "993"), "IMAP_PORT", minimum=1),
        use_ssl=parse_bool_value(env_map.get("IMAP_SSL", "true"), "IMAP_SSL"),
    )


def load_draft_defaults(account: AccountConfig, env: Mapping[str, str] | None = None) -> DraftDefaults:
    env_map = dict(os.environ if env is None else env)
    default_flags = parse_flags(env_map.get("IMAP_APPEND_FLAGS", "\\Draft"), "IMAP_APPEND_FLAGS")
    if not default_flags:
        default_flags = ["\\Draft"]
    mailbox_default = (
        str(env_map.get("IMAP_APPEND_MAILBOX") or "").strip()
        or str(env_map.get("IMAP_MAILBOX") or "").strip()
        or "Drafts"
    )

    return DraftDefaults(
        mailbox=mailbox_default,
        flags=default_flags,
        from_addr=(str(env_map.get("IMAP_APPEND_FROM") or account.username).strip() or account.username),
        to_addrs=parse_recipients_env(env_map.get("IMAP_APPEND_TO")),
        cc_addrs=parse_recipients_env(env_map.get("IMAP_APPEND_CC")),
        bcc_addrs=parse_recipients_env(env_map.get("IMAP_APPEND_BCC")),
        subject=str(env_map.get("IMAP_APPEND_SUBJECT") or "[draft] email-imap-append"),
        body=str(env_map.get("IMAP_APPEND_BODY") or ""),
        content_type=parse_content_type(
            str(env_map.get("IMAP_APPEND_CONTENT_TYPE") or "plain"),
            "IMAP_APPEND_CONTENT_TYPE",
        ),
        connect_timeout=parse_int_value(
            env_map.get("IMAP_CONNECT_TIMEOUT", "20"),
            "IMAP_CONNECT_TIMEOUT",
            minimum=1,
        ),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Append unsent draft email into IMAP mailbox.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("check-config", help="Validate IMAP env config and print sanitized summary.")

    append_parser = subparsers.add_parser("append-draft", help="Append one unsent draft email.")
    append_parser.add_argument("--to", action="append", default=[], help="Draft recipient. Repeat or use comma-separated values.")
    append_parser.add_argument("--cc", action="append", default=[], help="Draft CC recipient. Repeat or use comma-separated values.")
    append_parser.add_argument("--bcc", action="append", default=[], help="Draft BCC recipient. Repeat or use comma-separated values.")
    append_parser.add_argument("--subject", default=None, help="Draft subject.")
    append_parser.add_argument("--body", default=None, help="Draft body text.")
    append_parser.add_argument(
        "--content-type",
        choices=sorted(CONTENT_TYPES),
        default=None,
        help="Draft body content type.",
    )
    append_parser.add_argument("--from", dest="from_addr", default=None, help="Override sender address.")
    append_parser.add_argument("--mailbox", default=None, help="Target mailbox for APPEND (default Drafts).")
    append_parser.add_argument("--flags", default=None, help="APPEND flags, comma-separated. Example: '\\Draft,\\Seen'.")
    append_parser.add_argument("--message-id", default=None, help="Optional Message-ID header.")
    append_parser.add_argument("--in-reply-to", default=None, help="Optional In-Reply-To header.")
    append_parser.add_argument("--references", default=None, help="Optional References header.")

    return parser


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


def extract_append_uid(response_data: Sequence[Any]) -> tuple[str | None, str | None]:
    for item in response_data:
        text = item.decode("utf-8", errors="replace") if isinstance(item, (bytes, bytearray)) else str(item)
        matched = APPENDUID_RE.search(text)
        if matched:
            return matched.group(1), matched.group(2)
    return None, None


def build_flags_argument(flags: list[str]) -> str | None:
    if not flags:
        return None
    return "(" + " ".join(flags) + ")"


def build_draft_message(
    from_addr: str,
    to_addrs: list[str],
    cc_addrs: list[str],
    bcc_addrs: list[str],
    subject: str,
    body: str,
    content_type: str,
    message_id: str | None,
    in_reply_to: str | None,
    references: str | None,
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = from_addr
    if to_addrs:
        message["To"] = ", ".join(to_addrs)
    if cc_addrs:
        message["Cc"] = ", ".join(cc_addrs)
    if bcc_addrs:
        message["Bcc"] = ", ".join(bcc_addrs)
    message["Subject"] = subject
    message["Date"] = format_datetime(datetime.now(timezone.utc))
    message["Message-ID"] = (message_id or make_msgid()).strip()
    if in_reply_to:
        message["In-Reply-To"] = in_reply_to.strip()
    if references:
        message["References"] = references.strip()
    message.set_content(body, subtype=content_type)
    return message


def command_check_config(account: AccountConfig, defaults: DraftDefaults) -> int:
    payload = {
        "imap": {
            "name": account.name,
            "host": account.host,
            "port": account.port,
            "ssl": account.use_ssl,
            "username": account.username,
            "mailbox": account.mailbox,
        },
        "append_defaults": {
            "mailbox": defaults.mailbox,
            "flags": defaults.flags,
            "from": defaults.from_addr,
            "to": defaults.to_addrs,
            "cc": defaults.cc_addrs,
            "bcc_count": len(defaults.bcc_addrs),
            "subject": defaults.subject,
            "content_type": defaults.content_type,
            "connect_timeout": defaults.connect_timeout,
        },
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def command_append_draft(account: AccountConfig, defaults: DraftDefaults, args: argparse.Namespace) -> int:
    mailbox = (args.mailbox or defaults.mailbox).strip() or defaults.mailbox
    flags = parse_flags(args.flags, "--flags") if args.flags is not None else list(defaults.flags)
    if not flags:
        flags = ["\\Draft"]

    from_addr = (args.from_addr or defaults.from_addr).strip() or defaults.from_addr
    to_addrs = parse_recipients(args.to) if args.to else list(defaults.to_addrs)
    cc_addrs = parse_recipients(args.cc) if args.cc else list(defaults.cc_addrs)
    bcc_addrs = parse_recipients(args.bcc) if args.bcc else list(defaults.bcc_addrs)
    subject = args.subject if args.subject is not None else defaults.subject
    body = args.body if args.body is not None else defaults.body
    content_type = args.content_type if args.content_type is not None else defaults.content_type

    message = build_draft_message(
        from_addr=from_addr,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        bcc_addrs=bcc_addrs,
        subject=subject,
        body=body,
        content_type=content_type,
        message_id=args.message_id,
        in_reply_to=args.in_reply_to,
        references=args.references,
    )

    client: imaplib.IMAP4 | None = None
    try:
        client = open_imap_connection(account, defaults.connect_timeout)
        status, _ = client.select(mailbox, readonly=False)
        if status != "OK":
            raise RuntimeError(f"SELECT failed for mailbox={mailbox!r}, status={status}")

        status, response_data = client.append(
            mailbox,
            build_flags_argument(flags),
            None,
            message.as_bytes(),
        )
        if status != "OK":
            raise RuntimeError(f"APPEND failed for mailbox={mailbox!r}, status={status}, detail={response_data!r}")

        append_uidvalidity, append_uid = extract_append_uid(response_data if isinstance(response_data, Sequence) else [])
    except Exception as exc:
        print(
            json.dumps(
                {
                    "type": "error",
                    "at": utc_now_iso(),
                    "event": "imap_append_failed",
                    "error": str(exc),
                    "mailbox": mailbox,
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1
    finally:
        safe_logout(client)

    print(
        json.dumps(
            {
                "type": "status",
                "at": utc_now_iso(),
                "event": "imap_draft_appended",
                "account": account.name,
                "mailbox": mailbox,
                "from": from_addr,
                "to": to_addrs,
                "cc": cc_addrs,
                "bcc_count": len(bcc_addrs),
                "subject": subject,
                "message_id": message.get("Message-ID"),
                "flags": flags,
                "append_uidvalidity": append_uidvalidity,
                "append_uid": append_uid,
                "not_sent": True,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        account = load_account_from_env()
        defaults = load_draft_defaults(account)
    except ValueError as exc:
        print(f"IMAP_APPEND_ERR reason=config_error message={exc}", file=sys.stderr)
        return 2

    if args.command == "check-config":
        return command_check_config(account, defaults)
    if args.command == "append-draft":
        return command_append_draft(account, defaults, args)

    print(f"IMAP_APPEND_ERR reason=unknown_command command={args.command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
