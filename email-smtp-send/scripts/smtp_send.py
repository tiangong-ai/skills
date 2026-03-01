#!/usr/bin/env python3
"""Send email via SMTP and optionally append the sent message to IMAP Sent mailbox."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import re
import smtplib
import ssl
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from email.utils import format_datetime, make_msgid
from pathlib import Path
from typing import Any, Sequence


TRUE_VALUES = {"1", "true", "yes", "on", "y"}
FALSE_VALUES = {"0", "false", "no", "off", "n"}
CONTENT_TYPES = {"plain", "html"}
DEFAULT_MAX_ATTACHMENT_BYTES = 25 * 1024 * 1024
APPENDUID_RE = re.compile(r"\[APPENDUID\s+(\d+)\s+(\d+)\]", re.IGNORECASE)


@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    use_ssl: bool
    starttls: bool
    username: str
    password: str
    from_addr: str
    timeout: int


@dataclass(frozen=True)
class MessageDefaults:
    subject: str
    body: str
    content_type: str
    max_attachment_bytes: int


@dataclass(frozen=True)
class SentSyncConfig:
    enabled: bool
    required: bool
    host: str
    port: int
    use_ssl: bool
    username: str
    password: str
    mailbox: str
    flags: list[str]
    timeout: int


@dataclass(frozen=True)
class AttachmentPayload:
    source_path: Path
    filename: str
    content_type: str
    bytes_size: int
    payload: bytes


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
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"{label} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"{label} must be >= {minimum}, got {value}")
    return value


def parse_content_type(raw: str, label: str) -> str:
    value = str(raw).strip().lower()
    if value not in CONTENT_TYPES:
        raise ValueError(f"{label} must be one of {sorted(CONTENT_TYPES)}, got {raw!r}")
    return value


def parse_recipients(values: Sequence[str], *, required: bool = False) -> list[str]:
    recipients: list[str] = []
    for item in values:
        parts = [value.strip() for value in item.split(",")]
        for part in parts:
            if part:
                recipients.append(part)
    if required and not recipients:
        raise ValueError("At least one recipient is required")
    return recipients


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


def first_nonempty_env(names: Sequence[str], default: str = "") -> str:
    for name in names:
        raw = os.environ.get(name)
        if raw is None:
            continue
        text = str(raw).strip()
        if text:
            return text
    return default


def parse_env_int_with_fallback(names: Sequence[str], default: int, minimum: int) -> int:
    for name in names:
        raw = os.environ.get(name)
        if raw is None or not str(raw).strip():
            continue
        return parse_int_value(raw, name, minimum=minimum)
    return default


def parse_env_bool_with_fallback(names: Sequence[str], default: bool) -> bool:
    for name in names:
        raw = os.environ.get(name)
        if raw is None or not str(raw).strip():
            continue
        return parse_bool_value(raw, name)
    return default


def parse_attachment_paths(values: Sequence[str]) -> list[Path]:
    attachments: list[Path] = []
    seen: set[str] = set()
    for item in values:
        parts = [value.strip() for value in item.split(",")]
        for part in parts:
            if not part:
                continue
            path = Path(part).expanduser().resolve()
            path_key = str(path)
            if path_key in seen:
                continue
            if not path.exists():
                raise ValueError(f"Attachment file not found: {path}")
            if not path.is_file():
                raise ValueError(f"Attachment path must be a file: {path}")
            attachments.append(path)
            seen.add(path_key)
    return attachments


def parse_mime_type(content_type: str) -> tuple[str, str]:
    normalized = str(content_type or "").strip().lower()
    if "/" not in normalized:
        return "application", "octet-stream"
    maintype, subtype = normalized.split("/", 1)
    maintype = maintype.strip() or "application"
    subtype = subtype.strip() or "octet-stream"
    return maintype, subtype


def read_attachments(attachment_paths: Sequence[Path], max_attachment_bytes: int) -> list[AttachmentPayload]:
    payloads: list[AttachmentPayload] = []
    for path in attachment_paths:
        stat = path.stat()
        size = int(stat.st_size)
        if size > max_attachment_bytes:
            raise ValueError(
                f"Attachment exceeds max bytes ({size} > {max_attachment_bytes}): {path}"
            )
        content_type, _ = mimetypes.guess_type(path.name)
        payloads.append(
            AttachmentPayload(
                source_path=path,
                filename=path.name,
                content_type=(content_type or "application/octet-stream").lower(),
                bytes_size=size,
                payload=path.read_bytes(),
            )
        )
    return payloads


def load_smtp_config_from_env() -> SmtpConfig:
    host = (os.environ.get("SMTP_HOST") or "").strip()
    if not host:
        raise ValueError("SMTP_HOST is required")

    username = (os.environ.get("SMTP_USERNAME") or "").strip()
    if not username:
        raise ValueError("SMTP_USERNAME is required")

    password = os.environ.get("SMTP_PASSWORD") or ""
    if not password:
        raise ValueError("SMTP_PASSWORD is required")

    return SmtpConfig(
        host=host,
        port=parse_env_int_with_fallback(["SMTP_PORT"], default=465, minimum=1),
        use_ssl=parse_env_bool_with_fallback(["SMTP_SSL"], default=True),
        starttls=parse_env_bool_with_fallback(["SMTP_STARTTLS"], default=False),
        username=username,
        password=password,
        from_addr=(os.environ.get("SMTP_FROM") or username).strip() or username,
        timeout=parse_env_int_with_fallback(["SMTP_CONNECT_TIMEOUT"], default=20, minimum=1),
    )


def load_message_defaults_from_env() -> MessageDefaults:
    return MessageDefaults(
        subject=str(os.environ.get("SMTP_SUBJECT") or "[smtp-test] email-smtp-send"),
        body=str(os.environ.get("SMTP_BODY") or "SMTP test message from email-smtp-send."),
        content_type=parse_content_type(
            str(os.environ.get("SMTP_CONTENT_TYPE") or "plain"),
            "SMTP_CONTENT_TYPE",
        ),
        max_attachment_bytes=parse_env_int_with_fallback(
            ["SMTP_MAX_ATTACHMENT_BYTES"],
            default=DEFAULT_MAX_ATTACHMENT_BYTES,
            minimum=1,
        ),
    )


def load_sent_sync_config_from_env() -> SentSyncConfig:
    enabled = parse_env_bool_with_fallback(["SMTP_SYNC_SENT"], default=False)
    required = parse_env_bool_with_fallback(["SMTP_SYNC_SENT_REQUIRED"], default=False)

    config = SentSyncConfig(
        enabled=enabled,
        required=required,
        host=first_nonempty_env(["SMTP_SENT_IMAP_HOST", "IMAP_HOST"]),
        port=parse_env_int_with_fallback(["SMTP_SENT_IMAP_PORT", "IMAP_PORT"], default=993, minimum=1),
        use_ssl=parse_env_bool_with_fallback(["SMTP_SENT_IMAP_SSL", "IMAP_SSL"], default=True),
        username=first_nonempty_env(["SMTP_SENT_IMAP_USERNAME", "IMAP_USERNAME", "SMTP_USERNAME"]),
        password=first_nonempty_env(["SMTP_SENT_IMAP_PASSWORD", "IMAP_PASSWORD"]),
        mailbox=first_nonempty_env(["SMTP_SENT_IMAP_MAILBOX", "IMAP_SENT_MAILBOX"], default="Sent Items"),
        flags=parse_flags(
            first_nonempty_env(["SMTP_SENT_IMAP_FLAGS"], default="\\Seen"),
            "SMTP_SENT_IMAP_FLAGS",
        )
        or ["\\Seen"],
        timeout=parse_env_int_with_fallback(
            ["SMTP_SENT_IMAP_CONNECT_TIMEOUT", "IMAP_CONNECT_TIMEOUT"],
            default=20,
            minimum=1,
        ),
    )
    if config.enabled:
        validate_sent_sync_config(config, mailbox_override=None)
    return config


def build_parser(defaults: MessageDefaults) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send email via SMTP server configured by env vars.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("check-config", help="Validate SMTP/IMAP env config and print sanitized summary.")

    send_parser = subparsers.add_parser("send", help="Send one email.")
    send_parser.add_argument(
        "--to",
        required=True,
        action="append",
        help="Recipient email. Repeat or use comma-separated values.",
    )
    send_parser.add_argument(
        "--cc",
        action="append",
        default=[],
        help="CC recipient email. Repeat or use comma-separated values.",
    )
    send_parser.add_argument(
        "--bcc",
        action="append",
        default=[],
        help="BCC recipient email. Repeat or use comma-separated values.",
    )
    send_parser.add_argument("--subject", default=defaults.subject, help="Email subject.")
    send_parser.add_argument("--body", default=defaults.body, help="Email body text.")
    send_parser.add_argument(
        "--content-type",
        choices=sorted(CONTENT_TYPES),
        default=defaults.content_type,
        help="Email body content type.",
    )
    send_parser.add_argument("--from", dest="from_addr", default=None, help="Override sender address.")
    send_parser.add_argument(
        "--attach",
        action="append",
        default=[],
        help="Attachment file path. Repeat or use comma-separated values.",
    )
    send_parser.add_argument(
        "--max-attachment-bytes",
        default=defaults.max_attachment_bytes,
        help="Maximum allowed bytes per attachment.",
    )
    send_parser.add_argument("--message-id", default=None, help="Optional Message-ID header.")
    send_parser.add_argument("--in-reply-to", default=None, help="Optional In-Reply-To header.")
    send_parser.add_argument("--references", default=None, help="Optional References header.")

    sync_group = send_parser.add_mutually_exclusive_group()
    sync_group.add_argument(
        "--sync-sent",
        dest="sync_sent",
        action="store_true",
        help="Append sent message to IMAP sent mailbox after SMTP send.",
    )
    sync_group.add_argument(
        "--no-sync-sent",
        dest="sync_sent",
        action="store_false",
        help="Do not append sent message to IMAP sent mailbox.",
    )
    send_parser.set_defaults(sync_sent=None)
    send_parser.add_argument(
        "--sent-mailbox",
        default=None,
        help="Sent mailbox used for IMAP APPEND (default SMTP_SENT_IMAP_MAILBOX or Sent Items).",
    )
    send_parser.add_argument(
        "--sent-flags",
        default=None,
        help="IMAP APPEND flags for sent message, comma-separated (default \\Seen).",
    )
    send_parser.add_argument(
        "--sent-sync-required",
        action="store_true",
        help="Return non-zero when SMTP succeeds but sent-mailbox sync fails.",
    )

    return parser


def detect_imapclient_error() -> str | None:
    try:
        import imapclient  # noqa: F401
    except Exception as exc:
        return f"{type(exc).__name__}: {exc}"
    return None


def command_check_config(
    smtp_config: SmtpConfig,
    defaults: MessageDefaults,
    sent_sync: SentSyncConfig,
) -> int:
    imapclient_error = detect_imapclient_error()
    payload = {
        "smtp": {
            "host": smtp_config.host,
            "port": smtp_config.port,
            "ssl": smtp_config.use_ssl,
            "starttls": smtp_config.starttls,
            "username": smtp_config.username,
            "from": smtp_config.from_addr,
            "timeout": smtp_config.timeout,
        },
        "message_defaults": {
            "subject": defaults.subject,
            "content_type": defaults.content_type,
            "max_attachment_bytes": defaults.max_attachment_bytes,
        },
        "sent_sync": {
            "enabled": sent_sync.enabled,
            "required": sent_sync.required,
            "imap_host": sent_sync.host,
            "imap_port": sent_sync.port,
            "imap_ssl": sent_sync.use_ssl,
            "imap_username": sent_sync.username,
            "sent_mailbox": sent_sync.mailbox,
            "flags": sent_sync.flags,
            "timeout": sent_sync.timeout,
            "imapclient_available": imapclient_error is None,
        },
    }
    if imapclient_error is not None:
        payload["sent_sync"]["imapclient_error"] = imapclient_error
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def build_message(
    from_addr: str,
    to_addrs: list[str],
    cc_addrs: list[str],
    subject: str,
    body: str,
    content_type: str,
    message_id: str | None,
    in_reply_to: str | None,
    references: str | None,
    attachments: Sequence[AttachmentPayload],
) -> EmailMessage:
    message = EmailMessage()
    message["From"] = from_addr
    message["To"] = ", ".join(to_addrs)
    if cc_addrs:
        message["Cc"] = ", ".join(cc_addrs)
    message["Subject"] = subject
    message["Date"] = format_datetime(datetime.now(timezone.utc))
    message["Message-ID"] = (message_id or make_msgid()).strip()
    if in_reply_to:
        message["In-Reply-To"] = in_reply_to.strip()
    if references:
        message["References"] = references.strip()
    message.set_content(body, subtype=content_type)
    for attachment in attachments:
        maintype, subtype = parse_mime_type(attachment.content_type)
        message.add_attachment(
            attachment.payload,
            maintype=maintype,
            subtype=subtype,
            filename=attachment.filename,
        )
    return message


def send_via_smtp(config: SmtpConfig, message: EmailMessage, recipients: Sequence[str]) -> None:
    if config.use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(config.host, config.port, timeout=config.timeout, context=context) as client:
            client.login(config.username, config.password)
            client.send_message(message, to_addrs=list(recipients))
        return

    with smtplib.SMTP(config.host, config.port, timeout=config.timeout) as client:
        client.ehlo()
        if config.starttls:
            context = ssl.create_default_context()
            client.starttls(context=context)
            client.ehlo()
        client.login(config.username, config.password)
        client.send_message(message, to_addrs=list(recipients))


def flatten_response_items(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple)):
        flattened: list[Any] = []
        for item in value:
            flattened.extend(flatten_response_items(item))
        return flattened
    return [value]


def extract_append_uid(response_data: Any) -> tuple[str | None, str | None]:
    for item in flatten_response_items(response_data):
        if item is None:
            continue
        if isinstance(item, int):
            return None, str(item)
        text = item.decode("utf-8", errors="replace") if isinstance(item, (bytes, bytearray)) else str(item)
        matched = APPENDUID_RE.search(text)
        if matched:
            return matched.group(1), matched.group(2)
    return None, None


def validate_sent_sync_config(config: SentSyncConfig, mailbox_override: str | None) -> None:
    if not config.host:
        raise ValueError("Sent sync requires SMTP_SENT_IMAP_HOST or IMAP_HOST")
    if not config.username:
        raise ValueError("Sent sync requires SMTP_SENT_IMAP_USERNAME or IMAP_USERNAME")
    if not config.password:
        raise ValueError("Sent sync requires SMTP_SENT_IMAP_PASSWORD or IMAP_PASSWORD")
    mailbox = (mailbox_override or config.mailbox).strip()
    if not mailbox:
        raise ValueError("Sent sync mailbox cannot be empty")


def append_to_sent_mailbox(
    sync_config: SentSyncConfig,
    mailbox: str,
    flags: list[str],
    message_bytes: bytes,
) -> tuple[str | None, str | None]:
    try:
        from imapclient import IMAPClient  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Sent mailbox sync requires imapclient. Install with: python3 -m pip install imapclient"
        ) from exc

    client = IMAPClient(sync_config.host, port=sync_config.port, ssl=sync_config.use_ssl, timeout=sync_config.timeout)
    try:
        client.login(sync_config.username, sync_config.password)
        append_response = client.append(
            mailbox,
            message_bytes,
            flags=tuple(flags) if flags else (),
        )
        append_uidvalidity, append_uid = extract_append_uid(append_response)

        if append_uid is None:
            low_level = getattr(client, "_imap", None)
            if low_level is not None and hasattr(low_level, "response"):
                try:
                    raw_appenduid = low_level.response("APPENDUID")
                    more_uidvalidity, more_uid = extract_append_uid(raw_appenduid)
                    if more_uid is not None:
                        append_uidvalidity, append_uid = more_uidvalidity, more_uid
                except Exception:
                    pass
        return append_uidvalidity, append_uid
    finally:
        try:
            client.logout()
        except Exception:
            pass


def command_send(
    smtp_config: SmtpConfig,
    defaults: MessageDefaults,
    sent_sync: SentSyncConfig,
    args: argparse.Namespace,
) -> int:
    try:
        to_addrs = parse_recipients(args.to, required=True)
        cc_addrs = parse_recipients(args.cc) if args.cc else []
        bcc_addrs = parse_recipients(args.bcc) if args.bcc else []
        recipients = to_addrs + cc_addrs + bcc_addrs

        max_attachment_bytes = parse_int_value(
            args.max_attachment_bytes,
            "--max-attachment-bytes",
            minimum=1,
        )
        attachment_paths = parse_attachment_paths(args.attach) if args.attach else []
        attachments = read_attachments(attachment_paths, max_attachment_bytes)

        from_addr = (args.from_addr or smtp_config.from_addr).strip() or smtp_config.from_addr
        subject = args.subject if args.subject is not None else defaults.subject
        body = args.body if args.body is not None else defaults.body
        content_type = parse_content_type(args.content_type, "--content-type")

        sync_enabled = sent_sync.enabled
        if args.sync_sent is not None:
            sync_enabled = bool(args.sync_sent)
        sync_required = sent_sync.required or bool(args.sent_sync_required)
        if sync_required and not sync_enabled:
            raise ValueError("Sent sync is required but disabled")

        sent_mailbox = (args.sent_mailbox or sent_sync.mailbox).strip() or sent_sync.mailbox
        sent_flags = (
            parse_flags(args.sent_flags, "--sent-flags")
            if args.sent_flags is not None
            else list(sent_sync.flags)
        )
        if not sent_flags:
            sent_flags = ["\\Seen"]
        if sync_enabled:
            validate_sent_sync_config(sent_sync, mailbox_override=sent_mailbox)

        message = build_message(
            from_addr=from_addr,
            to_addrs=to_addrs,
            cc_addrs=cc_addrs,
            subject=subject,
            body=body,
            content_type=content_type,
            message_id=args.message_id,
            in_reply_to=args.in_reply_to,
            references=args.references,
            attachments=attachments,
        )
    except ValueError as exc:
        print(
            json.dumps(
                {
                    "type": "error",
                    "at": utc_now_iso(),
                    "event": "smtp_send_invalid_args",
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2

    try:
        send_via_smtp(smtp_config, message, recipients)
    except Exception as exc:
        print(
            json.dumps(
                {
                    "type": "error",
                    "at": utc_now_iso(),
                    "event": "smtp_send_failed",
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1

    sent_sync_payload: dict[str, Any] = {
        "enabled": sync_enabled,
        "required": sync_required,
    }
    if sync_enabled:
        sent_sync_payload["mailbox"] = sent_mailbox
        sent_sync_payload["flags"] = sent_flags
        try:
            append_uidvalidity, append_uid = append_to_sent_mailbox(
                sent_sync,
                mailbox=sent_mailbox,
                flags=sent_flags,
                message_bytes=message.as_bytes(),
            )
            sent_sync_payload["appended"] = True
            sent_sync_payload["append_uidvalidity"] = append_uidvalidity
            sent_sync_payload["append_uid"] = append_uid
        except Exception as exc:
            sent_sync_payload["appended"] = False
            sent_sync_payload["error"] = str(exc)
            if sync_required:
                print(
                    json.dumps(
                        {
                            "type": "error",
                            "at": utc_now_iso(),
                            "event": "smtp_sent_sync_failed",
                            "error": str(exc),
                            "smtp_sent": True,
                            "mailbox": sent_mailbox,
                        },
                        ensure_ascii=False,
                    ),
                    file=sys.stderr,
                )
                return 1
    else:
        sent_sync_payload["appended"] = False

    print(
        json.dumps(
            {
                "type": "status",
                "at": utc_now_iso(),
                "event": "smtp_sent",
                "from": message["From"],
                "to_count": len(recipients),
                "to": to_addrs,
                "cc": cc_addrs,
                "bcc_count": len(bcc_addrs),
                "subject": subject,
                "message_id": message.get("Message-ID"),
                "smtp_host": smtp_config.host,
                "smtp_port": smtp_config.port,
                "attachment_count": len(attachments),
                "attachments": [
                    {
                        "filename": item.filename,
                        "bytes": item.bytes_size,
                        "content_type": item.content_type,
                        "source_path": str(item.source_path),
                    }
                    for item in attachments
                ],
                "sent_sync": sent_sync_payload,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        smtp_config = load_smtp_config_from_env()
        defaults = load_message_defaults_from_env()
        sent_sync = load_sent_sync_config_from_env()
    except ValueError as exc:
        print(f"SMTP_SEND_ERR reason=config_error message={exc}", file=sys.stderr)
        return 2

    parser = build_parser(defaults)
    args = parser.parse_args(argv)

    if args.command == "check-config":
        return command_check_config(smtp_config, defaults, sent_sync)
    if args.command == "send":
        return command_send(smtp_config, defaults, sent_sync, args)

    print(f"SMTP_SEND_ERR reason=unknown_command command={args.command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
