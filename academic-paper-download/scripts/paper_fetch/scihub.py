from __future__ import annotations

import html.parser
import os
import re
import urllib.parse

from .errors import PaperFetchError
from .http import HttpClient
from .models import Candidate, ChannelResolution


DEFAULT_MIRRORS = (
    "sci-hub.ru",
    "sci-hub.st",
    "sci-hub.su",
    "sci-hub.box",
    "sci-hub.red",
    "sci-hub.al",
    "sci-hub.mk",
    "sci-hub.ee",
)
DISCOVERY_URL = "https://www.sci-hub.pub/"
SCIHUB_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 "
    "Mobile/15E148 Safari/604.1"
)
NOT_FOUND_PATTERNS = (
    re.compile(r"please\s+try\s+to\s+search\s+again\s+using\s+doi", re.IGNORECASE),
    re.compile(r"article\s+not\s+found\s+in\s+(?:the\s+)?database", re.IGNORECASE),
    re.compile(r"статья\s+не\s+найдена\s+в\s+базе", re.IGNORECASE),
)
DISCOVERY_RE = re.compile(
    r'href=["\']https?://(?:www\.)?(sci-hub\.[a-z0-9.-]+)/?["\']',
    re.IGNORECASE,
)


class _EmbedFinder(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.candidates: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() not in {"iframe", "embed"}:
            return
        values = {(key or "").casefold(): value or "" for key, value in attrs}
        if values.get("src"):
            self.candidates.append((values.get("id", "").casefold(), values["src"].strip()))

    handle_startendtag = handle_starttag


def extract_pdf_url(page: str, mirror: str) -> str | None:
    finder = _EmbedFinder()
    try:
        finder.feed(page)
    except Exception:
        return None
    ordered = sorted(
        finder.candidates,
        key=lambda item: (item[0] != "pdf", ".pdf" not in item[1].casefold()),
    )
    for element_id, source in ordered:
        if element_id != "pdf" and ".pdf" not in source.casefold():
            continue
        if source.startswith("//"):
            return "https:" + source
        if source.startswith("/"):
            return f"https://{mirror}{source}"
        if source.startswith("http://"):
            return "https://" + source[len("http://") :]
        if source.startswith("https://"):
            return source
    return None


def configured_mirrors() -> list[str]:
    raw = os.environ.get("PAPER_FETCH_SCIHUB_MIRRORS", "").strip()
    entries = raw.split(",") if raw else list(DEFAULT_MIRRORS)
    mirrors: list[str] = []
    for entry in entries:
        candidate = entry.strip()
        if not candidate:
            continue
        parsed = urllib.parse.urlparse(
            candidate if "://" in candidate else "https://" + candidate
        )
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            continue
        host = parsed.hostname.rstrip(".").casefold()
        if host not in mirrors:
            mirrors.append(host)
    return mirrors


class SciHubResolver:
    def __init__(self, http: HttpClient, mirrors: list[str] | None = None) -> None:
        self.http = http
        self.mirrors = mirrors

    def _discover(self, timeout: float) -> list[str]:
        try:
            page = self.http.get_text(DISCOVERY_URL, timeout=timeout, headers={"User-Agent": SCIHUB_UA})
        except PaperFetchError:
            return []
        return list(dict.fromkeys(match.group(1).casefold() for match in DISCOVERY_RE.finditer(page)))

    def resolve(self, doi: str, *, timeout: float) -> ChannelResolution:
        tried: set[str] = set()
        mirrors = list(self.mirrors if self.mirrors is not None else configured_mirrors())
        discovered = False
        while True:
            for mirror in mirrors:
                if mirror in tried:
                    continue
                tried.add(mirror)
                page_url = f"https://{mirror}/{urllib.parse.quote(doi, safe='/')}"
                try:
                    page = self.http.get_text(
                        page_url,
                        timeout=timeout,
                        headers={"User-Agent": SCIHUB_UA},
                    )
                except PaperFetchError:
                    continue
                pdf_url = extract_pdf_url(page, mirror)
                if pdf_url:
                    return ChannelResolution(
                        candidate=Candidate(
                            "scihub",
                            pdf_url,
                            detail={"mirror": mirror},
                            headers={"User-Agent": SCIHUB_UA, "Referer": f"https://{mirror}/"},
                        )
                    )
                if any(pattern.search(page) for pattern in NOT_FOUND_PATTERNS):
                    return ChannelResolution()
            if discovered:
                return ChannelResolution()
            mirrors = self._discover(timeout)
            discovered = True
