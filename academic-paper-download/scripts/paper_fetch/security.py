from __future__ import annotations

import ipaddress
import socket
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from typing import Any

from .errors import PaperFetchError


Resolver = Callable[..., list[tuple[Any, ...]]]
BLOCKED_HOSTS = {
    "localhost",
    "localhost.localdomain",
    "ip6-localhost",
    "ip6-loopback",
    "metadata",
    "metadata.google.internal",
    "metadata.aws.internal",
}


def _public_address(address: str) -> bool:
    ip = ipaddress.ip_address(address)
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def validate_public_url(url: str, resolver: Resolver = socket.getaddrinfo) -> None:
    try:
        parsed = urllib.parse.urlparse(url)
        port = parsed.port
    except (TypeError, ValueError) as exc:
        raise PaperFetchError("unsafe_url", "Malformed URL", url=url) from exc
    if parsed.scheme not in {"http", "https"}:
        raise PaperFetchError("unsafe_url", "Only HTTP(S) URLs are allowed", url=url)
    if port is not None and port not in {80, 443}:
        raise PaperFetchError("unsafe_url", "Non-standard network port is blocked", url=url)
    host = (parsed.hostname or "").rstrip(".").lower()
    if not host or host in BLOCKED_HOSTS:
        raise PaperFetchError("unsafe_url", "Blocked or empty hostname", url=url)
    try:
        literal = ipaddress.ip_address(host)
    except ValueError:
        literal = None
    if literal is not None and not _public_address(str(literal)):
        raise PaperFetchError("unsafe_url", "Private network address is blocked", url=url)
    try:
        answers = resolver(host, port or (443 if parsed.scheme == "https" else 80))
    except OSError as exc:
        raise PaperFetchError(
            "dns_error",
            f"Could not resolve host: {host}",
            retryable=True,
            url=url,
        ) from exc
    addresses = {answer[4][0] for answer in answers if len(answer) > 4 and answer[4]}
    if not addresses:
        raise PaperFetchError("dns_error", f"Host resolved to no addresses: {host}", retryable=True, url=url)
    if any(not _public_address(address) for address in addresses):
        raise PaperFetchError("unsafe_url", "Hostname resolves to a private network address", url=url)


class SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    def __init__(self, resolver: Resolver = socket.getaddrinfo) -> None:
        super().__init__()
        self.resolver = resolver

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        try:
            validate_public_url(newurl, self.resolver)
        except PaperFetchError as exc:
            raise urllib.error.HTTPError(
                newurl,
                code,
                f"unsafe_redirect:{exc.code}",
                headers,
                fp,
            ) from exc
        return super().redirect_request(req, fp, code, msg, headers, newurl)
