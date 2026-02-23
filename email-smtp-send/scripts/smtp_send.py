#!/usr/bin/env python3
"""Send email via SMTP using environment configuration."""

from __future__ import annotations

import argparse
import json
import os
import smtplib
import ssl
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage
from typing import Sequence


TRUE_VALUES = {"1", "true", "yes", "on", "y"}
FALSE_VALUES = {"0", "false", "no", "off", "n"}
CONTENT_TYPES = {"plain", "html"}


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


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_bool_value(raw: str | bool, label: str) -> bool:
    if isinstance(raw, bool):
        return raw
    text = str(raw).strip().lower()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    raise ValueError(f"{label} must be true/false, got {raw!r}")


def parse_int_value(raw: str | int, label: str, minimum: int = 1) -> int:
    try:
        value = int(str(raw).strip())
    except Exception as exc:  # pragma: no cover
        raise ValueError(f"{label} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ValueError(f"{label} must be >= {minimum}, got {value}")
    return value


def parse_env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return parse_int_value(raw, name, minimum=minimum)


def parse_recipients(values: Sequence[str]) -> list[str]:
    recipients: list[str] = []
    for item in values:
        parts = [value.strip() for value in item.split(",")]
        for part in parts:
            if part:
                recipients.append(part)
    if not recipients:
        raise ValueError("At least one recipient is required")
    return recipients


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
        port=parse_env_int("SMTP_PORT", default=465, minimum=1),
        use_ssl=parse_bool_value(os.environ.get("SMTP_SSL", "true"), "SMTP_SSL"),
        starttls=parse_bool_value(os.environ.get("SMTP_STARTTLS", "false"), "SMTP_STARTTLS"),
        username=username,
        password=password,
        from_addr=(os.environ.get("SMTP_FROM") or username).strip() or username,
        timeout=parse_env_int("SMTP_CONNECT_TIMEOUT", default=20, minimum=1),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Send email via SMTP server configured by env vars.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("check-config", help="Validate SMTP env config and print sanitized summary.")

    send_parser = subparsers.add_parser("send", help="Send one email.")
    send_parser.add_argument("--to", required=True, action="append", help="Recipient email. Repeat or use comma-separated values.")
    send_parser.add_argument("--cc", action="append", default=[], help="CC recipient email. Repeat or use comma-separated values.")
    send_parser.add_argument("--bcc", action="append", default=[], help="BCC recipient email. Repeat or use comma-separated values.")
    send_parser.add_argument("--subject", default=os.environ.get("SMTP_SUBJECT", "[smtp-test] email-smtp-send"), help="Email subject.")
    send_parser.add_argument("--body", default=os.environ.get("SMTP_BODY", "SMTP test message from email-smtp-send."), help="Email body text.")
    send_parser.add_argument("--content-type", choices=sorted(CONTENT_TYPES), default=(os.environ.get("SMTP_CONTENT_TYPE", "plain").strip().lower() or "plain"), help="Email body content type.")
    send_parser.add_argument("--from", dest="from_addr", default=None, help="Override sender address.")

    return parser


def command_check_config(config: SmtpConfig) -> int:
    payload = {
        "smtp": {
            "host": config.host,
            "port": config.port,
            "ssl": config.use_ssl,
            "starttls": config.starttls,
            "username": config.username,
            "from": config.from_addr,
            "timeout": config.timeout,
        }
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0


def send_via_smtp(config: SmtpConfig, message: EmailMessage) -> None:
    if config.use_ssl:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(config.host, config.port, timeout=config.timeout, context=context) as client:
            client.login(config.username, config.password)
            client.send_message(message)
        return

    with smtplib.SMTP(config.host, config.port, timeout=config.timeout) as client:
        client.ehlo()
        if config.starttls:
            context = ssl.create_default_context()
            client.starttls(context=context)
            client.ehlo()
        client.login(config.username, config.password)
        client.send_message(message)


def command_send(config: SmtpConfig, args: argparse.Namespace) -> int:
    to_addrs = parse_recipients(args.to)
    cc_addrs = parse_recipients(args.cc) if args.cc else []
    bcc_addrs = parse_recipients(args.bcc) if args.bcc else []

    message = EmailMessage()
    message["From"] = (args.from_addr or config.from_addr).strip() or config.from_addr
    message["To"] = ", ".join(to_addrs)
    if cc_addrs:
        message["Cc"] = ", ".join(cc_addrs)
    message["Subject"] = args.subject
    message.set_content(args.body, subtype=args.content_type)

    recipients = to_addrs + cc_addrs + bcc_addrs

    try:
        send_via_smtp(config, message)
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
                "subject": args.subject,
                "smtp_host": config.host,
                "smtp_port": config.port,
            },
            ensure_ascii=False,
        )
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        config = load_smtp_config_from_env()
    except ValueError as exc:
        print(f"SMTP_SEND_ERR reason=config_error message={exc}", file=sys.stderr)
        return 2

    if args.command == "check-config":
        return command_check_config(config)
    if args.command == "send":
        return command_send(config, args)

    print(f"SMTP_SEND_ERR reason=unknown_command command={args.command!r}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
