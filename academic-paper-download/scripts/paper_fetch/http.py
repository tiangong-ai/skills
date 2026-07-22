from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from .errors import PaperFetchError
from .sanitize import sanitize_text, sanitize_url
from .security import Resolver, SafeRedirectHandler, validate_public_url


@runtime_checkable
class PaperTransport(Protocol):
    """Injectable transport used by every resolver and artifact download."""

    def get_json(
        self,
        url: str,
        *,
        timeout: float,
        headers: dict[str, str] | None = None,
        max_bytes: int = 5 * 1024 * 1024,
    ) -> dict[str, Any]: ...

    def get_text(
        self,
        url: str,
        *,
        timeout: float,
        headers: dict[str, str] | None = None,
        max_bytes: int = 5 * 1024 * 1024,
    ) -> str: ...

    def download_to(
        self,
        url: str,
        destination: Path,
        *,
        timeout: float,
        max_bytes: int,
        headers: dict[str, str] | None = None,
    ) -> int: ...


class HttpClient:
    """Default urllib implementation of :class:`PaperTransport`."""

    def __init__(
        self,
        *,
        user_agent: str,
        resolver: Resolver = socket.getaddrinfo,
        opener: Any | None = None,
    ) -> None:
        self.user_agent = user_agent
        self.resolver = resolver
        self.opener = opener or urllib.request.build_opener(SafeRedirectHandler(resolver))

    def _open(
        self,
        url: str,
        *,
        timeout: float,
        accept: str,
        headers: dict[str, str] | None = None,
    ):
        validate_public_url(url, self.resolver)
        request_headers = {"User-Agent": self.user_agent, "Accept": accept}
        if headers:
            request_headers.update(headers)
        request = urllib.request.Request(url, headers=request_headers)
        try:
            return self.opener.open(request, timeout=timeout)
        except PaperFetchError:
            raise
        except urllib.error.HTTPError as exc:
            retryable = exc.code not in {400, 404, 410}
            safe_url = sanitize_url(url)
            raise PaperFetchError(
                "http_error",
                f"HTTP {exc.code} while fetching {safe_url}",
                retryable=retryable,
                http_status=exc.code,
                url=safe_url,
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            safe_url = sanitize_url(url)
            raise PaperFetchError(
                "network_error",
                f"Network error while fetching {safe_url}: {sanitize_text(str(exc))}",
                retryable=True,
                url=safe_url,
            ) from exc

    def get_json(
        self,
        url: str,
        *,
        timeout: float,
        headers: dict[str, str] | None = None,
        max_bytes: int = 5 * 1024 * 1024,
    ) -> dict[str, Any]:
        with self._open(
            url,
            timeout=timeout,
            accept="application/json",
            headers=headers,
        ) as response:
            payload = response.read(max_bytes + 1)
        if len(payload) > max_bytes:
            raise PaperFetchError("response_too_large", "JSON response exceeds size limit", retryable=False, url=url)
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PaperFetchError("invalid_json", "Remote service returned invalid JSON", retryable=True, url=url) from exc
        if not isinstance(value, dict):
            raise PaperFetchError("invalid_json", "Remote JSON response must be an object", retryable=True, url=url)
        return value

    def get_text(
        self,
        url: str,
        *,
        timeout: float,
        headers: dict[str, str] | None = None,
        max_bytes: int = 5 * 1024 * 1024,
    ) -> str:
        with self._open(
            url,
            timeout=timeout,
            accept="text/html,application/xhtml+xml",
            headers=headers,
        ) as response:
            payload = response.read(max_bytes + 1)
        if len(payload) > max_bytes:
            raise PaperFetchError("response_too_large", "HTML response exceeds size limit", retryable=False, url=url)
        return payload.decode("utf-8", "replace")

    def download_to(
        self,
        url: str,
        destination: Path,
        *,
        timeout: float,
        max_bytes: int,
        headers: dict[str, str] | None = None,
    ) -> int:
        total = 0
        try:
            with self._open(
                url,
                timeout=timeout,
                accept="application/pdf,*/*;q=0.8",
                headers=headers,
            ) as response, destination.open("wb") as handle:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_bytes:
                        raise PaperFetchError(
                            "download_size_exceeded",
                            f"PDF exceeds size limit of {max_bytes} bytes",
                            retryable=False,
                            url=url,
                        )
                    handle.write(chunk)
                handle.flush()
                os.fsync(handle.fileno())
        except PaperFetchError:
            raise
        except OSError as exc:
            raise PaperFetchError(
                "download_io_error",
                f"Could not write temporary download: {exc}",
                retryable=True,
                url=url,
            ) from exc
        return total
