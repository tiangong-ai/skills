#!/usr/bin/env python3
"""Fetch full MIME email and attachment files by Message-Id (preferred) or UID."""

from __future__ import annotations

import argparse
import email
import hashlib
import imaplib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from email import policy
from pathlib import Path
from typing import Any, Mapping, Sequence


TRUE_VALUES = {"1", "true", "yes", "on", "y"}
FALSE_VALUES = {"0", "false", "no", "off", "n"}
UID_RE = re.compile(rb"UID (\d+)")
SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


@dataclass(frozen=True)
class AccountConfig:
    name: str
    host: str
    username: str
    password: str
    mailbox: str = "INBOX"
    port: int = 993
    use_ssl: bool = True


@dataclass(frozen=True)
class FetchOptions:
    message_id: str | None
    uid: str | None
    save_eml_dir: Path
    index_dir: Path | None
    save_attachments_dir: Path
    max_attachment_bytes: int
    allow_ext: set[str]
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


def parse_env_int(name: str, default: int, minimum: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return parse_int_value(raw, name, minimum=minimum)


def normalize_message_id(raw_message_id: str | None) -> str:
    value = str(raw_message_id or "").strip()
    if not value:
        return ""
    return value.replace("<", "").replace(">", "").strip().lower()


def normalize_uid(raw_uid: str | None) -> str:
    value = str(raw_uid or "").strip()
    if not value:
        return ""
    if not value.isdigit():
        raise ValueError(f"uid must be digits, got {raw_uid!r}")
    number = int(value)
    if number <= 0:
        raise ValueError(f"uid must be >= 1, got {raw_uid!r}")
    return str(number)


def normalize_allow_ext(raw: str | None) -> set[str]:
    if raw is None:
        return set()
    parts = [item.strip().lower().lstrip(".") for item in raw.split(",") if item.strip()]
    return {item for item in parts if item}


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


def sanitize_filename(filename: str) -> str:
    text = str(filename or "").replace("\\", "/")
    text = text.split("/")[-1]
    text = text.strip()
    text = SAFE_FILENAME_RE.sub("_", text)
    text = text.strip("._")
    return text


def make_index_key(message_id_norm: str) -> str:
    normalized = normalize_message_id(message_id_norm)
    if not normalized:
        raise ValueError("message_id_norm is required for index key")
    # Use exact UTF-8 hex encoding to keep a one-to-one mapping with message_id_norm.
    return normalized.encode("utf-8", errors="strict").hex()


def json_dumps(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def parse_fetch_payload(data: Any) -> tuple[str | None, bytes | None]:
    uid: str | None = None
    payload: bytes | None = None
    if not isinstance(data, list):
        return uid, payload

    for item in data:
        if not isinstance(item, tuple):
            continue
        metadata = item[0] if len(item) >= 1 and isinstance(item[0], (bytes, bytearray)) else b""
        matched = UID_RE.search(bytes(metadata))
        if matched:
            uid = matched.group(1).decode("ascii", errors="ignore")
        if len(item) >= 2 and isinstance(item[1], (bytes, bytearray)):
            payload = bytes(item[1])
    return uid, payload


def parse_headers(message: email.message.Message) -> dict[str, Any]:
    headers: dict[str, Any] = {}
    for key, value in message.items():
        text = str(value)
        current = headers.get(key)
        if current is None:
            headers[key] = text
        elif isinstance(current, list):
            current.append(text)
        else:
            headers[key] = [current, text]
    return headers


def extract_text_bodies(message: email.message.Message) -> tuple[str, str]:
    plain_parts: list[str] = []
    html_parts: list[str] = []

    if message.is_multipart():
        for part in message.walk():
            if part.get_content_maintype() == "multipart":
                continue
            disposition = (part.get_content_disposition() or "").lower()
            filename = part.get_filename()
            if disposition == "attachment":
                continue
            if filename and disposition in {"inline", "attachment"} and part.get_content_type() not in {
                "text/plain",
                "text/html",
            }:
                continue
            content_type = part.get_content_type()
            if content_type == "text/plain":
                plain_parts.append(decode_part_text(part))
            elif content_type == "text/html":
                html_parts.append(decode_part_text(part))
    else:
        content_type = message.get_content_type()
        if content_type == "text/plain":
            plain_parts.append(decode_part_text(message))
        elif content_type == "text/html":
            html_parts.append(decode_part_text(message))

    text_plain = "\n\n".join(item for item in plain_parts if item).strip()
    text_html = "\n\n".join(item for item in html_parts if item).strip()
    return text_plain, text_html


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


def quote_imap_value(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', "") + '"'


def search_uids_by_message_id(client: imaplib.IMAP4, message_id_value: str) -> list[str]:
    criteria_value = quote_imap_value(message_id_value)
    status, data = client.uid("SEARCH", None, "HEADER", "Message-Id", criteria_value)
    if status != "OK":
        raise RuntimeError(f"UID SEARCH by Message-Id failed, status={status}")
    if not data or not data[0]:
        return []
    return [chunk.decode("ascii", errors="ignore") for chunk in data[0].split() if chunk]


def fetch_message_id_for_uid(client: imaplib.IMAP4, uid: str) -> str:
    status, data = client.uid("FETCH", uid, "(UID BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])")
    if status != "OK":
        raise RuntimeError(f"UID FETCH header failed for uid={uid}, status={status}")
    _, raw_payload = parse_fetch_payload(data)
    if raw_payload is None:
        return ""
    parsed = email.message_from_bytes(raw_payload, policy=policy.default)
    return str(parsed.get("Message-Id", ""))


def find_uid_by_message_id_exact(
    client: imaplib.IMAP4,
    message_id_raw: str,
    message_id_norm: str,
) -> str | None:
    search_candidates: list[str] = []
    raw_value = str(message_id_raw or "").strip()
    if raw_value:
        search_candidates.append(raw_value)
    if message_id_norm:
        search_candidates.append(f"<{message_id_norm}>")
        search_candidates.append(message_id_norm)

    seen: set[str] = set()
    for candidate in search_candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        for uid in reversed(search_uids_by_message_id(client, candidate)):
            fetched_message_id = fetch_message_id_for_uid(client, uid)
            if normalize_message_id(fetched_message_id) == message_id_norm:
                return uid
    return None


def resolve_uid(
    client: imaplib.IMAP4,
    message_id: str | None,
    uid: str | None,
) -> tuple[str, str]:
    message_id_raw = str(message_id or "").strip()
    message_id_norm = normalize_message_id(message_id_raw)

    if message_id_norm:
        resolved_uid = find_uid_by_message_id_exact(client, message_id_raw, message_id_norm)
        if resolved_uid:
            return resolved_uid, message_id_norm
        fallback_uid = normalize_uid(uid)
        if fallback_uid:
            return fallback_uid, message_id_norm
        raise RuntimeError("Message-Id not found and uid fallback not provided")

    fallback_uid = normalize_uid(uid)
    if fallback_uid:
        return fallback_uid, ""
    raise ValueError("fetch requires --message-id or --uid")


def fetch_message_by_uid(client: imaplib.IMAP4, uid: str) -> bytes:
    status, data = client.uid("FETCH", uid, "(UID BODY.PEEK[])")
    if status != "OK":
        raise RuntimeError(f"UID FETCH full message failed for uid={uid}, status={status}")
    _, raw_payload = parse_fetch_payload(data)
    if raw_payload is None:
        raise RuntimeError(f"UID FETCH returned no payload for uid={uid}")
    return raw_payload


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def resolve_index_dir(save_eml_dir: Path, index_dir: Path | None) -> Path:
    if index_dir is not None:
        return ensure_dir(index_dir)
    return ensure_dir(save_eml_dir / ".index")


def index_file_path(save_eml_dir: Path, index_dir: Path | None, message_id_norm: str) -> Path:
    key = make_index_key(message_id_norm)
    index_root = resolve_index_dir(save_eml_dir, index_dir)
    shard_1 = key[:2] or "00"
    shard_2 = key[2:4] or "00"
    shard_dir = ensure_dir(ensure_dir(index_root / shard_1) / shard_2)
    return shard_dir / f"{key}.json"


def extract_index_message_id_norm(payload: Mapping[str, Any]) -> str:
    mail_ref = payload.get("mail_ref")
    if not isinstance(mail_ref, Mapping):
        return ""
    norm_value = normalize_message_id(str(mail_ref.get("message_id_norm") or ""))
    if norm_value:
        return norm_value
    return normalize_message_id(str(mail_ref.get("message_id_raw") or ""))


def load_index_record(
    save_eml_dir: Path,
    index_dir: Path | None,
    message_id_norm: str,
) -> dict[str, Any] | None:
    normalized = normalize_message_id(message_id_norm)
    if not normalized:
        return None
    path = index_file_path(save_eml_dir, index_dir, normalized)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    indexed_norm = extract_index_message_id_norm(payload)
    if not indexed_norm or indexed_norm != normalized:
        return None
    return payload


def record_paths_exist(payload: Mapping[str, Any]) -> bool:
    eml_path = str(payload.get("saved_eml_path") or "").strip()
    if not eml_path or not Path(eml_path).exists():
        return False
    attachments = payload.get("attachments")
    if isinstance(attachments, list):
        for item in attachments:
            if not isinstance(item, Mapping):
                continue
            saved_path = str(item.get("saved_path") or "").strip()
            if saved_path and not Path(saved_path).exists():
                return False
    return True


def write_index_record(
    save_eml_dir: Path,
    index_dir: Path | None,
    message_id_norm: str,
    payload: Mapping[str, Any],
) -> None:
    normalized = normalize_message_id(message_id_norm)
    if not normalized:
        return
    path = index_file_path(save_eml_dir, index_dir, normalized)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def build_eml_filename(message_id_norm: str, uid: str) -> str:
    base = sanitize_filename(message_id_norm) if message_id_norm else ""
    if not base:
        base = f"uid-{uid}"
    return f"{base}.eml"


def build_attachment_filename(original_name: str, index: int) -> str:
    safe = sanitize_filename(original_name)
    if safe:
        return safe
    return f"attachment-{index}"


def extension_allowed(filename: str, allow_ext: set[str]) -> bool:
    if not allow_ext:
        return True
    suffix = Path(filename).suffix.lower().lstrip(".")
    if not suffix:
        return False
    return suffix in allow_ext


def dedupe_filename(directory: Path, filename: str, payload: bytes, used_names: set[str]) -> str:
    candidate = filename
    if candidate not in used_names and not (directory / candidate).exists():
        used_names.add(candidate)
        return candidate

    stem = Path(filename).stem or "file"
    suffix = Path(filename).suffix
    digest = hashlib.sha256(payload).hexdigest()[:10]
    candidate = f"{stem}-{digest}{suffix}"
    counter = 2
    while candidate in used_names or (directory / candidate).exists():
        candidate = f"{stem}-{digest}-{counter}{suffix}"
        counter += 1
    used_names.add(candidate)
    return candidate


def save_full_message(raw_message: bytes, save_eml_dir: Path, message_id_norm: str, uid: str) -> Path:
    ensure_dir(save_eml_dir)
    filename = build_eml_filename(message_id_norm, uid)
    path = save_eml_dir / filename
    if not path.exists():
        path.write_bytes(raw_message)
    return path.resolve()


def extract_and_save_attachments(
    message: email.message.Message,
    save_attachments_dir: Path,
    max_attachment_bytes: int,
    allow_ext: set[str],
) -> list[dict[str, Any]]:
    ensure_dir(save_attachments_dir)
    used_names: set[str] = set()
    attachments: list[dict[str, Any]] = []

    index = 0
    for part in message.walk():
        if part.get_content_maintype() == "multipart":
            continue
        disposition = (part.get_content_disposition() or "").lower()
        filename = part.get_filename()
        if disposition not in {"attachment", "inline"} and not filename:
            continue

        index += 1
        payload = part.get_payload(decode=True)
        content = payload if isinstance(payload, (bytes, bytearray)) else b""
        original_name = str(filename or "")
        safe_name = build_attachment_filename(original_name, index)
        content_type = part.get_content_type()
        size = len(content)

        item: dict[str, Any] = {
            "filename": safe_name,
            "content_type": content_type,
            "bytes": size,
            "disposition": disposition,
            "saved_path": None,
            "skipped_reason": None,
        }

        if size > max_attachment_bytes:
            item["skipped_reason"] = "max_attachment_bytes_exceeded"
            attachments.append(item)
            continue

        if not extension_allowed(safe_name, allow_ext):
            item["skipped_reason"] = "extension_not_allowed"
            attachments.append(item)
            continue

        final_name = dedupe_filename(save_attachments_dir, safe_name, content, used_names)
        final_path = save_attachments_dir / final_name
        final_path.write_bytes(content)
        item["filename"] = final_name
        item["saved_path"] = str(final_path.resolve())
        attachments.append(item)

    return attachments


def build_mail_ref(
    account: str,
    mailbox: str,
    uid: str,
    message_id_raw: str,
    message_id_norm: str,
    date_value: str,
) -> dict[str, str]:
    return {
        "account": account,
        "mailbox": mailbox,
        "uid": uid,
        "message_id_raw": message_id_raw,
        "message_id_norm": message_id_norm,
        "date": date_value,
    }


def emit_json(payload: Mapping[str, Any]) -> None:
    print(json_dumps(payload), flush=True)


def build_parser() -> argparse.ArgumentParser:
    default_save_eml_dir = os.environ.get("IMAP_FULL_SAVE_EML_DIR", "./.email-imap-full-fetch/eml")
    default_index_dir = os.environ.get("IMAP_FULL_INDEX_DIR", "")
    default_save_attachments_dir = os.environ.get(
        "IMAP_FULL_SAVE_ATTACHMENTS_DIR",
        "./.email-imap-full-fetch/attachments",
    )
    default_max_attachment_bytes = parse_env_int(
        "IMAP_FULL_MAX_ATTACHMENT_BYTES",
        default=25 * 1024 * 1024,
        minimum=1,
    )
    default_allow_ext = os.environ.get("IMAP_FULL_ALLOW_EXT", "")
    default_connect_timeout = parse_env_int("IMAP_CONNECT_TIMEOUT", default=20, minimum=1)
    default_mailbox = os.environ.get("IMAP_MAILBOX", "INBOX")

    parser = argparse.ArgumentParser(
        description="Fetch full MIME email and attachments by message-id (preferred) or uid.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch one email by message-id or uid.")
    fetch_parser.add_argument(
        "--message-id",
        default=None,
        help="Message-Id value (preferred lookup key).",
    )
    fetch_parser.add_argument(
        "--uid",
        default=None,
        help="UID fallback when message-id lookup misses.",
    )
    fetch_parser.add_argument(
        "--mailbox",
        default=default_mailbox,
        help="Mailbox name (default from IMAP_MAILBOX or INBOX).",
    )
    fetch_parser.add_argument(
        "--save-eml-dir",
        default=default_save_eml_dir,
        help="Directory for raw .eml files (default from IMAP_FULL_SAVE_EML_DIR).",
    )
    fetch_parser.add_argument(
        "--index-dir",
        default=default_index_dir,
        help="Directory for idempotency index JSON files (env IMAP_FULL_INDEX_DIR). Defaults to <save-eml-dir>/.index.",
    )
    fetch_parser.add_argument(
        "--save-attachments-dir",
        default=default_save_attachments_dir,
        help="Directory for extracted attachments (default from IMAP_FULL_SAVE_ATTACHMENTS_DIR).",
    )
    fetch_parser.add_argument(
        "--max-attachment-bytes",
        type=lambda value: parse_int_value(value, "--max-attachment-bytes", minimum=1),
        default=default_max_attachment_bytes,
        help="Skip attachments larger than this limit (env IMAP_FULL_MAX_ATTACHMENT_BYTES).",
    )
    fetch_parser.add_argument(
        "--allow-ext",
        default=default_allow_ext,
        help="Comma-separated allowed attachment extensions (env IMAP_FULL_ALLOW_EXT). Empty means allow all.",
    )
    fetch_parser.add_argument(
        "--connect-timeout",
        type=lambda value: parse_int_value(value, "--connect-timeout", minimum=1),
        default=default_connect_timeout,
        help="IMAP connection timeout seconds (default from IMAP_CONNECT_TIMEOUT).",
    )
    return parser


def command_fetch(account: AccountConfig, args: argparse.Namespace) -> int:
    index_dir = Path(args.index_dir).expanduser() if str(args.index_dir or "").strip() else None
    options = FetchOptions(
        message_id=args.message_id,
        uid=args.uid,
        save_eml_dir=Path(args.save_eml_dir).expanduser(),
        index_dir=index_dir,
        save_attachments_dir=Path(args.save_attachments_dir).expanduser(),
        max_attachment_bytes=args.max_attachment_bytes,
        allow_ext=normalize_allow_ext(args.allow_ext),
        connect_timeout=args.connect_timeout,
    )
    mailbox = str(args.mailbox or account.mailbox).strip() or account.mailbox

    requested_message_id_norm = normalize_message_id(options.message_id)
    if requested_message_id_norm:
        cached = load_index_record(options.save_eml_dir, options.index_dir, requested_message_id_norm)
        if cached and record_paths_exist(cached):
            replay = dict(cached)
            replay["idempotent_hit"] = True
            emit_json(replay)
            return 0

    client: imaplib.IMAP4 | None = None
    try:
        client = open_imap_connection(account, options.connect_timeout)
        status, _ = client.select(mailbox, readonly=True)
        if status != "OK":
            raise RuntimeError(f"SELECT failed for mailbox={mailbox!r}")

        uid, _ = resolve_uid(client, options.message_id, options.uid)
        raw_payload = fetch_message_by_uid(client, uid)
        message = email.message_from_bytes(raw_payload, policy=policy.default)

        message_id_raw = str(message.get("Message-Id", "")).strip()
        message_id_norm = normalize_message_id(message_id_raw) or requested_message_id_norm
        if not message_id_norm:
            message_id_norm = f"uid-{uid}"

        cached_after_fetch = load_index_record(
            options.save_eml_dir,
            options.index_dir,
            message_id_norm,
        )
        if cached_after_fetch and record_paths_exist(cached_after_fetch):
            replay = dict(cached_after_fetch)
            replay["idempotent_hit"] = True
            emit_json(replay)
            return 0

        date_value = str(message.get("Date", ""))
        headers = parse_headers(message)
        text_plain, text_html = extract_text_bodies(message)
        saved_eml_path = save_full_message(raw_payload, options.save_eml_dir, message_id_norm, uid)
        attachments = extract_and_save_attachments(
            message=message,
            save_attachments_dir=options.save_attachments_dir,
            max_attachment_bytes=options.max_attachment_bytes,
            allow_ext=options.allow_ext,
        )
        mail_ref = build_mail_ref(
            account=account.name,
            mailbox=mailbox,
            uid=uid,
            message_id_raw=message_id_raw,
            message_id_norm=message_id_norm,
            date_value=date_value,
        )

        result: dict[str, Any] = {
            "mail_ref": mail_ref,
            "headers": headers,
            "text_plain": text_plain,
            "text_html": text_html,
            "attachments": attachments,
            "saved_eml_path": str(saved_eml_path),
            "idempotent_hit": False,
            "fetched_at": utc_now_iso(),
        }
        write_index_record(
            options.save_eml_dir,
            options.index_dir,
            message_id_norm,
            result,
        )
        emit_json(result)
        return 0
    finally:
        safe_logout(client)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        account = load_account_from_env()
        if args.command == "fetch":
            return command_fetch(account, args)
    except ValueError as exc:
        print(f"IMAP_FULL_FETCH_ERR reason=config_error message={exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        payload = {
            "type": "error",
            "at": utc_now_iso(),
            "event": "fetch_failed",
            "error": str(exc),
        }
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        return 1

    print("IMAP_FULL_FETCH_ERR reason=unknown_command", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
