from __future__ import annotations

import re
import urllib.parse
from collections.abc import Mapping, Sequence
from typing import Any


REDACTED = "[REDACTED]"
SENSITIVE_NAMES = {
    "access_token",
    "auth",
    "email",
    "mailto",
    "api_key",
    "authorization",
    "cookie",
    "credential",
    "key",
    "license_key",
    "password",
    "proxy_password",
    "refresh_token",
    "secret",
    "session",
    "session_id",
    "session_token",
    "set_cookie",
    "signature",
    "sig",
    "token",
    "x-api-key",
}
_EMAIL = re.compile(r"(?<![\w.+-])[\w.+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?![\w.-])")
_URL = re.compile(r"https?://[^\s<>\"']+")
_ASSIGNMENT = re.compile(
    r"(?i)\b(access[_-]?token|auth|authorization|cookie|credential|email|mailto|"
    r"api[_-]?key|key|license[_-]?key|password|proxy[_-]?password|refresh[_-]?token|"
    r"secret|session(?:[_-]?(?:id|token))?|set[_-]?cookie|signature|sig|token|x-api-key)"
    r"\s*([:=])\s*([^\s,;&]+)"
)
_BEARER = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")


def _sensitive_name(value: object) -> bool:
    return str(value).strip().casefold().replace("-", "_") in {
        name.replace("-", "_") for name in SENSITIVE_NAMES
    }


def sanitize_url(url: str) -> str:
    try:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {"http", "https"}:
            return sanitize_text_without_urls(url)
        hostname = parsed.hostname or ""
        port = f":{parsed.port}" if parsed.port is not None else ""
        if parsed.username is not None or parsed.password is not None:
            netloc = f"{REDACTED}@{hostname}{port}"
        else:
            netloc = parsed.netloc
        query = urllib.parse.urlencode(
            [
                (key, REDACTED if _sensitive_name(key) else value)
                for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
            ],
            doseq=True,
        )
        fragment_items = urllib.parse.parse_qsl(parsed.fragment, keep_blank_values=True)
        if fragment_items:
            fragment = urllib.parse.urlencode(
                [
                    (key, REDACTED if _sensitive_name(key) else value)
                    for key, value in fragment_items
                ],
                doseq=True,
            )
        else:
            fragment = sanitize_text_without_urls(parsed.fragment)
        return urllib.parse.urlunsplit((parsed.scheme, netloc, parsed.path, query, fragment))
    except (TypeError, ValueError):
        return sanitize_text_without_urls(str(url))


def sanitize_text_without_urls(value: str) -> str:
    value = _BEARER.sub(f"Bearer {REDACTED}", value)
    value = _ASSIGNMENT.sub(lambda match: f"{match.group(1)}{match.group(2)}{REDACTED}", value)
    return _EMAIL.sub(REDACTED, value)


def sanitize_text(value: str) -> str:
    redacted = _URL.sub(lambda match: sanitize_url(match.group(0)), value)
    return sanitize_text_without_urls(redacted)


def sanitize_data(value: Any, *, key: str | None = None) -> Any:
    if key is not None and _sensitive_name(key):
        return REDACTED
    if isinstance(value, str):
        return sanitize_text(value)
    if isinstance(value, Mapping):
        return {str(item_key): sanitize_data(item_value, key=str(item_key)) for item_key, item_value in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [sanitize_data(item) for item in value]
    return value
