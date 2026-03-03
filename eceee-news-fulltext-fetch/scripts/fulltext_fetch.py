#!/usr/bin/env python3
"""Discover and fetch full text from eceee news pages into SQLite."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen

try:
    import trafilatura  # type: ignore
except ImportError:
    trafilatura = None


DEFAULT_DB_FILENAME = "eceee_news.db"
DEFAULT_DB_PATH = os.environ.get("ECEEE_NEWS_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_INDEX_URL = "https://www.eceee.org/all-news/"
DEFAULT_USER_AGENT = "eceee-news-fulltext-fetch/1.0 (+https://github.com/tiangong-ai/skills)"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_MINUTES = 30
MAX_RETRY_BACKOFF_MINUTES = 24 * 60
BINARY_CONTENT_PREFIXES = (
    "application/pdf",
    "application/zip",
    "application/octet-stream",
    "image/",
    "audio/",
    "video/",
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE,
    title TEXT,
    published_at TEXT,
    discovered_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entry_content (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id INTEGER NOT NULL UNIQUE,
    source_url TEXT NOT NULL,
    final_url TEXT,
    http_status INTEGER,
    extractor TEXT NOT NULL,
    content_text TEXT,
    content_hash TEXT,
    content_length INTEGER NOT NULL DEFAULT 0,
    fetched_at TEXT NOT NULL,
    last_error TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY(entry_id) REFERENCES entries(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_entries_published_at ON entries(published_at);
CREATE INDEX IF NOT EXISTS idx_entries_last_seen_at ON entries(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_entry_content_status ON entry_content(status);
CREATE INDEX IF NOT EXISTS idx_entry_content_updated_at ON entry_content(updated_at);
CREATE INDEX IF NOT EXISTS idx_entry_content_retry_count ON entry_content(retry_count);
CREATE INDEX IF NOT EXISTS idx_entry_content_status_updated_entry
    ON entry_content(status, updated_at DESC, entry_id DESC);
CREATE INDEX IF NOT EXISTS idx_entry_content_failed_retry
    ON entry_content(status, next_retry_at, retry_count);
"""


class NewsIndexParser(HTMLParser):
    """Parse links from eceee all-news index page."""

    def __init__(self, base_url: str) -> None:
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self._capturing = False
        self._capture_href = ""
        self._capture_title = ""
        self._capture_pubdate = ""
        self._capture_text: list[str] = []
        self.records: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return

        attr_map = {k.lower(): (v or "") for k, v in attrs}
        classes = set((attr_map.get("class") or "").split())
        href = normalize_space(attr_map.get("href") or "")

        if "newslink" not in classes:
            return
        if not is_article_url(href):
            return

        self._capturing = True
        self._capture_href = canonicalize_url(href, self.base_url)
        self._capture_title = normalize_space(attr_map.get("title") or "")
        self._capture_pubdate = normalize_space(attr_map.get("data-pubdate") or "")
        self._capture_text = []

    def handle_data(self, data: str) -> None:
        if not self._capturing:
            return
        text = normalize_space(data)
        if text:
            self._capture_text.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or not self._capturing:
            return

        merged_text = normalize_space(" ".join(self._capture_text))
        title = self._capture_title or merged_text
        pubdate = normalize_pubdate(self._capture_pubdate)

        if self._capture_href and title:
            record: dict[str, str] = {
                "url": self._capture_href,
                "title": title,
            }
            if pubdate:
                record["published_at"] = pubdate
            self.records.append(record)

        self._capturing = False
        self._capture_href = ""
        self._capture_title = ""
        self._capture_pubdate = ""
        self._capture_text = []


class ReadableTextParser(HTMLParser):
    """Fallback text extractor when trafilatura is unavailable."""

    SKIP_TAGS = {"script", "style", "noscript", "svg", "canvas", "iframe"}
    BLOCK_TAGS = {
        "article",
        "aside",
        "blockquote",
        "br",
        "dd",
        "div",
        "dl",
        "dt",
        "figcaption",
        "figure",
        "footer",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "header",
        "hr",
        "li",
        "main",
        "nav",
        "ol",
        "p",
        "pre",
        "section",
        "table",
        "td",
        "th",
        "tr",
        "ul",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._skip_depth = 0
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        tag_name = tag.lower()
        if tag_name in self.SKIP_TAGS:
            self._skip_depth += 1
            return
        if self._skip_depth == 0 and tag_name in self.BLOCK_TAGS:
            self._chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        tag_name = tag.lower()
        if tag_name in self.SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1
            return
        if self._skip_depth == 0 and tag_name in self.BLOCK_TAGS:
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth > 0:
            return
        text = normalize_space(data)
        if text:
            self._chunks.append(text + " ")

    def get_text(self) -> str:
        return "".join(self._chunks)


@dataclass
class FetchResponse:
    ok: bool
    source_url: str
    final_url: str | None
    http_status: int | None
    html: str | None
    error: str | None


@dataclass
class ExtractResult:
    status: str
    source_url: str
    final_url: str | None
    http_status: int | None
    extractor: str
    content_text: str | None
    content_hash: str | None
    content_length: int
    last_error: str | None


@dataclass
class DiscoverSummary:
    discovered: int
    inserted: int
    updated: int


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_utc_iso(value: str | None) -> datetime:
    text = normalize_space(str(value or ""))
    if not text:
        return datetime.now(timezone.utc)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_next_retry_at(
    fetched_at: str,
    retry_count: int,
    max_retries: int,
    retry_backoff_minutes: int,
) -> str | None:
    if max_retries > 0 and retry_count >= max_retries:
        return None
    base_minutes = max(int(retry_backoff_minutes), 0)
    if base_minutes == 0:
        wait_minutes = 0
    else:
        exponent = max(retry_count - 1, 0)
        wait_minutes = min(base_minutes * (2**exponent), MAX_RETRY_BACKOFF_MINUTES)
    next_retry = parse_utc_iso(fetched_at) + timedelta(minutes=wait_minutes)
    return next_retry.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_space(value: str) -> str:
    return " ".join(value.split())


def normalize_pubdate(value: str) -> str:
    text = normalize_space(value)
    if not text:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        return text
    return ""


def canonicalize_url(raw_url: str, base_url: str = DEFAULT_INDEX_URL) -> str:
    merged = urljoin(base_url, normalize_space(raw_url))
    try:
        parts = urlsplit(merged)
    except Exception:
        return merged

    scheme = parts.scheme.lower() if parts.scheme else "https"
    netloc = parts.netloc.lower()
    path = re.sub(r"/{2,}", "/", parts.path or "/")
    if not path.startswith("/"):
        path = "/" + path
    if path != "/":
        path = path.rstrip("/") + "/"

    return urlunsplit((scheme, netloc, path, "", ""))


def is_article_url(url: str) -> bool:
    target = url.lower()
    if "/all-news/news/" not in target:
        return False
    if target.rstrip("/").endswith("/all-news/news"):
        return False
    if "news-subscribe" in target:
        return False
    return True


def sha256_hexdigest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def clean_text(value: str) -> str:
    text = value.replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    lines = [normalize_space(line) for line in text.split("\n")]

    cleaned: list[str] = []
    previous = ""
    for line in lines:
        if not line:
            if cleaned and cleaned[-1] != "":
                cleaned.append("")
            continue
        if line == previous:
            continue
        cleaned.append(line)
        previous = line

    while cleaned and cleaned[0] == "":
        cleaned.pop(0)
    while cleaned and cleaned[-1] == "":
        cleaned.pop()

    normalized = "\n".join(cleaned)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def resolve_db_path(db_path: str) -> Path:
    raw = str(db_path or "").strip()
    if not raw:
        raw = DEFAULT_DB_PATH

    path = Path(raw).expanduser()
    looks_like_directory = raw.endswith(("/", "\\")) or path.is_dir() or path.suffix == ""
    if looks_like_directory:
        path = path / DEFAULT_DB_FILENAME
    return path


def connect_db(db_path: str) -> sqlite3.Connection:
    db_file = resolve_db_path(db_path)
    if db_file.parent and str(db_file.parent) not in ("", "."):
        db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def fetch_html(url: str, timeout: int, max_bytes: int, user_agent: str) -> FetchResponse:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            final_url = str(response.geturl() or url)
            http_status_raw = response.getcode()
            http_status = int(http_status_raw) if http_status_raw is not None else 200
            content_type = str(response.headers.get("Content-Type") or "").lower()
            if content_type and any(content_type.startswith(prefix) for prefix in BINARY_CONTENT_PREFIXES):
                return FetchResponse(
                    ok=False,
                    source_url=url,
                    final_url=final_url,
                    http_status=http_status,
                    html=None,
                    error=f"unsupported_content_type:{content_type}",
                )

            payload = response.read(max_bytes + 1)
            if len(payload) > max_bytes:
                payload = payload[:max_bytes]

            charset: str | None = None
            if hasattr(response.headers, "get_content_charset"):
                charset = response.headers.get_content_charset()
            if not charset:
                match = re.search(r"charset=([a-zA-Z0-9._-]+)", content_type)
                if match:
                    charset = match.group(1)

            html = ""
            for encoding in (charset, "utf-8", "latin-1"):
                if not encoding:
                    continue
                try:
                    html = payload.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            if not html:
                html = payload.decode("utf-8", errors="replace")

            return FetchResponse(
                ok=True,
                source_url=url,
                final_url=final_url,
                http_status=http_status,
                html=html,
                error=None,
            )
    except HTTPError as exc:
        return FetchResponse(
            ok=False,
            source_url=url,
            final_url=str(exc.geturl() or url),
            http_status=int(exc.code) if exc.code is not None else None,
            html=None,
            error=f"http_error:{exc.code}",
        )
    except URLError as exc:
        return FetchResponse(
            ok=False,
            source_url=url,
            final_url=url,
            http_status=None,
            html=None,
            error=f"url_error:{exc.reason}",
        )
    except TimeoutError:
        return FetchResponse(
            ok=False,
            source_url=url,
            final_url=url,
            http_status=None,
            html=None,
            error="timeout",
        )
    except Exception as exc:
        return FetchResponse(
            ok=False,
            source_url=url,
            final_url=url,
            http_status=None,
            html=None,
            error=f"unexpected_fetch_error:{exc}",
        )


def parse_news_index(html: str, index_url: str) -> list[dict[str, str]]:
    parser = NewsIndexParser(base_url=index_url)
    parser.feed(html)
    parser.close()

    deduped: dict[str, dict[str, str]] = {}
    for row in parser.records:
        key = row["url"]
        existing = deduped.get(key)
        if not existing:
            deduped[key] = row
            continue

        # Prefer entries that include a valid published date.
        if not existing.get("published_at") and row.get("published_at"):
            deduped[key] = row
            continue

        # Prefer longer title when duplicates collide.
        if len(row.get("title", "")) > len(existing.get("title", "")):
            deduped[key] = row

    return list(deduped.values())


def upsert_discovered_entries(conn: sqlite3.Connection, rows: list[dict[str, str]]) -> DiscoverSummary:
    now = now_utc_iso()
    inserted = 0
    updated = 0

    for row in rows:
        url = row["url"]
        title = normalize_space(row.get("title", ""))
        published_at = normalize_pubdate(row.get("published_at", "")) or None

        existing = conn.execute(
            "SELECT id, title, published_at FROM entries WHERE url = ?",
            (url,),
        ).fetchone()

        if not existing:
            conn.execute(
                """
                INSERT INTO entries (
                    url, title, published_at, discovered_at, last_seen_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (url, title, published_at, now, now, now, now),
            )
            inserted += 1
            continue

        previous_title = normalize_space(str(existing["title"] or ""))
        previous_pub = normalize_pubdate(str(existing["published_at"] or ""))
        next_title = title or previous_title
        next_pub = published_at or previous_pub or None

        conn.execute(
            """
            UPDATE entries
            SET title = ?, published_at = ?, last_seen_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (next_title, next_pub, now, now, int(existing["id"])),
        )

        if next_title != previous_title or (next_pub or "") != (previous_pub or ""):
            updated += 1

    return DiscoverSummary(discovered=len(rows), inserted=inserted, updated=updated)


def discover_entries(
    conn: sqlite3.Connection,
    index_url: str,
    timeout: int,
    max_bytes: int,
    user_agent: str,
) -> DiscoverSummary:
    response = fetch_html(url=index_url, timeout=timeout, max_bytes=max_bytes, user_agent=user_agent)
    if not response.ok or not response.html:
        error = normalize_space(str(response.error or "index_fetch_failed"))
        raise ValueError(f"index_fetch_failed:{error}")

    discovered_rows = parse_news_index(response.html, index_url=index_url)
    if not discovered_rows:
        raise ValueError("index_parse_empty")

    return upsert_discovered_entries(conn, discovered_rows)


def extract_main_content_fragment(html: str) -> str:
    # Keep the article block and drop related-news tail.
    match = re.search(
        r'<div\s+class="mainContentColumn">(.*?)</div>\s*<hr\s*/?>',
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    fragment = match.group(1) if match else ""

    if not fragment:
        fallback = re.search(
            r'<div\s+class="mainContentColumn">(.*?)</div>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        fragment = fallback.group(1) if fallback else html

    fragment = re.sub(r"<!--.*?-->", " ", fragment, flags=re.DOTALL)
    fragment = re.sub(
        r'<p[^>]*class="share-on-social"[^>]*>.*?</p>',
        " ",
        fragment,
        flags=re.IGNORECASE | re.DOTALL,
    )

    split_markers = [
        r'<a\s+name="more_related_news"',
        r'<h2\s+class="related-news-h2"',
    ]
    for marker in split_markers:
        parts = re.split(marker, fragment, maxsplit=1, flags=re.IGNORECASE)
        fragment = parts[0]

    # External reference links are not part of the article body.
    external_split = re.split(r"<h3>\s*External\s+link\s*</h3>", fragment, maxsplit=1, flags=re.IGNORECASE)
    fragment = external_split[0]

    return fragment


def extract_with_trafilatura(html: str, url: str) -> str:
    if trafilatura is None:
        return ""

    attempts: list[dict[str, Any]] = [
        {"url": url, "output_format": "txt", "include_comments": False, "include_tables": False},
        {"url": url, "output_format": "txt"},
        {"url": url},
        {},
    ]
    for kwargs in attempts:
        try:
            result = trafilatura.extract(html, **kwargs)
        except TypeError:
            continue
        except Exception:
            return ""
        cleaned = clean_text(str(result or ""))
        if cleaned:
            return cleaned
    return ""


def extract_with_fallback_parser(html: str) -> str:
    parser = ReadableTextParser()
    try:
        parser.feed(html)
        parser.close()
    except Exception:
        return ""
    return clean_text(parser.get_text())


def build_extract_result(
    fetch_response: FetchResponse,
    min_chars: int,
    disable_trafilatura: bool,
) -> ExtractResult:
    if not fetch_response.ok:
        return ExtractResult(
            status="failed",
            source_url=fetch_response.source_url,
            final_url=fetch_response.final_url,
            http_status=fetch_response.http_status,
            extractor="none",
            content_text=None,
            content_hash=None,
            content_length=0,
            last_error=fetch_response.error or "fetch_failed",
        )

    html = fetch_response.html or ""
    target_url = fetch_response.final_url or fetch_response.source_url
    target_html = extract_main_content_fragment(html)

    extractor = "html-parser"
    text = ""
    if not disable_trafilatura:
        text = extract_with_trafilatura(target_html, target_url)
        if text:
            extractor = "trafilatura"
    if not text:
        text = extract_with_fallback_parser(target_html)
        extractor = "html-parser"

    if not text:
        return ExtractResult(
            status="failed",
            source_url=fetch_response.source_url,
            final_url=fetch_response.final_url,
            http_status=fetch_response.http_status,
            extractor=extractor,
            content_text=None,
            content_hash=None,
            content_length=0,
            last_error="empty_extracted_text",
        )

    content_length = len(text)
    if content_length < max(min_chars, 1):
        return ExtractResult(
            status="failed",
            source_url=fetch_response.source_url,
            final_url=fetch_response.final_url,
            http_status=fetch_response.http_status,
            extractor=extractor,
            content_text=None,
            content_hash=None,
            content_length=content_length,
            last_error=f"text_too_short:{content_length}",
        )

    return ExtractResult(
        status="ready",
        source_url=fetch_response.source_url,
        final_url=fetch_response.final_url,
        http_status=fetch_response.http_status,
        extractor=extractor,
        content_text=text,
        content_hash=sha256_hexdigest(text),
        content_length=content_length,
        last_error=None,
    )


def list_candidate_entries(
    conn: sqlite3.Connection,
    limit: int,
    force: bool,
    only_failed: bool,
    refetch_days: int,
    oldest_first: bool,
    max_retries: int,
    since_date: str,
) -> list[sqlite3.Row]:
    sql = """
    SELECT
        e.id AS entry_id,
        e.title,
        e.url,
        e.published_at,
        ec.status AS content_status,
        ec.fetched_at,
        ec.updated_at
    FROM entries e
    LEFT JOIN entry_content ec ON ec.entry_id = e.id
    WHERE e.url IS NOT NULL AND e.url != ''
    """
    params: list[Any] = []

    if since_date:
        sql += " AND (e.published_at IS NULL OR e.published_at >= ?)"
        params.append(since_date)

    now_iso = now_utc_iso()
    failed_retry_clause = "ec.status = 'failed' AND (ec.next_retry_at IS NULL OR ec.next_retry_at <= ?)"
    failed_retry_params: list[Any] = [now_iso]
    if max_retries > 0:
        failed_retry_clause += " AND ec.retry_count < ?"
        failed_retry_params.append(max_retries)

    if only_failed:
        sql += f" AND ({failed_retry_clause})"
        params.extend(failed_retry_params)
    elif not force:
        if refetch_days > 0:
            threshold = datetime.now(timezone.utc) - timedelta(days=refetch_days)
            threshold_iso = threshold.replace(microsecond=0).isoformat().replace("+00:00", "Z")
            sql += (
                " AND (ec.entry_id IS NULL "
                f"OR ({failed_retry_clause}) "
                "OR (ec.status = 'ready' AND ec.fetched_at < ?))"
            )
            params.extend(failed_retry_params)
            params.append(threshold_iso)
        else:
            sql += f" AND (ec.entry_id IS NULL OR ({failed_retry_clause}))"
            params.extend(failed_retry_params)

    if oldest_first:
        sql += " ORDER BY COALESCE(ec.updated_at, '') ASC, e.id ASC"
    else:
        sql += """
        ORDER BY
            CASE
                WHEN ec.entry_id IS NULL THEN 0
                WHEN ec.status = 'failed' THEN 1
                ELSE 2
            END ASC,
            CASE
                WHEN ec.status = 'failed' THEN ec.retry_count
                ELSE 0
            END ASC,
            COALESCE(e.published_at, e.last_seen_at, e.discovered_at, '') DESC,
            e.id DESC
        """

    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)

    return conn.execute(sql, tuple(params)).fetchall()


def persist_extract_result(
    conn: sqlite3.Connection,
    entry_id: int,
    result: ExtractResult,
    fetched_at: str,
    max_retries: int,
    retry_backoff_minutes: int,
) -> str:
    existing = conn.execute(
        """
        SELECT status, content_hash, content_text, content_length, retry_count, next_retry_at
        FROM entry_content
        WHERE entry_id = ?
        """,
        (entry_id,),
    ).fetchone()

    if result.status == "ready":
        status_value = "ready"
        content_text = result.content_text
        content_hash = result.content_hash
        content_length = int(result.content_length)
        retry_count = 0
        next_retry_at = None
        last_error = None
        if not existing:
            state = "ready_new"
        elif str(existing["content_hash"] or "") == str(content_hash or ""):
            state = "ready_unchanged"
        else:
            state = "ready_updated"
    else:
        previous_retry = int(existing["retry_count"]) if existing else 0
        retry_count = previous_retry + 1
        next_retry_at = compute_next_retry_at(
            fetched_at=fetched_at,
            retry_count=retry_count,
            max_retries=max_retries,
            retry_backoff_minutes=retry_backoff_minutes,
        )
        last_error = result.last_error
        keep_ready_content = (
            existing is not None
            and str(existing["status"] or "") == "ready"
            and str(existing["content_hash"] or "") != ""
        )
        if keep_ready_content:
            status_value = "ready"
            content_text = str(existing["content_text"] or "")
            content_hash = str(existing["content_hash"] or "")
            content_length = int(existing["content_length"] or len(content_text))
            next_retry_at = None
            state = "ready_retained"
        else:
            status_value = "failed"
            content_text = None
            content_hash = None
            content_length = 0
            state = "failed_new" if not existing else "failed_updated"

    if not existing:
        conn.execute(
            """
            INSERT INTO entry_content (
                entry_id, source_url, final_url, http_status, extractor, content_text,
                content_hash, content_length, fetched_at, last_error, retry_count, next_retry_at, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entry_id,
                result.source_url,
                result.final_url,
                result.http_status,
                result.extractor,
                content_text,
                content_hash,
                content_length,
                fetched_at,
                last_error,
                retry_count,
                next_retry_at,
                status_value,
                fetched_at,
                fetched_at,
            ),
        )
        return state

    conn.execute(
        """
        UPDATE entry_content
        SET source_url = ?,
            final_url = ?,
            http_status = ?,
            extractor = ?,
            content_text = ?,
            content_hash = ?,
            content_length = ?,
            fetched_at = ?,
            last_error = ?,
            retry_count = ?,
            next_retry_at = ?,
            status = ?,
            updated_at = ?
        WHERE entry_id = ?
        """,
        (
            result.source_url,
            result.final_url,
            result.http_status,
            result.extractor,
            content_text,
            content_hash,
            content_length,
            fetched_at,
            last_error,
            retry_count,
            next_retry_at,
            status_value,
            fetched_at,
            entry_id,
        ),
    )
    return state


def process_entry(conn: sqlite3.Connection, row: sqlite3.Row, args: argparse.Namespace) -> tuple[str, ExtractResult]:
    source_url = normalize_space(str(row["url"] or ""))
    if not source_url:
        result = ExtractResult(
            status="failed",
            source_url="",
            final_url=None,
            http_status=None,
            extractor="none",
            content_text=None,
            content_hash=None,
            content_length=0,
            last_error="missing_source_url",
        )
    else:
        fetch_response = fetch_html(
            url=source_url,
            timeout=args.timeout,
            max_bytes=args.max_bytes,
            user_agent=args.user_agent,
        )
        result = build_extract_result(
            fetch_response=fetch_response,
            min_chars=args.min_chars,
            disable_trafilatura=args.disable_trafilatura,
        )

    fetched_at = now_utc_iso()
    state = persist_extract_result(
        conn=conn,
        entry_id=int(row["entry_id"]),
        result=result,
        fetched_at=fetched_at,
        max_retries=args.max_retries,
        retry_backoff_minutes=args.retry_backoff_minutes,
    )
    return state, result


def validate_retry_args(args: argparse.Namespace) -> None:
    if int(args.max_retries) < 0:
        raise ValueError("invalid_max_retries")
    if int(args.retry_backoff_minutes) < 0:
        raise ValueError("invalid_retry_backoff_minutes")


def validate_since_date(value: str) -> str:
    text = normalize_space(str(value or ""))
    if not text:
        return ""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        raise ValueError("invalid_since_date")
    return text


def cmd_init_db(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        conn.commit()
    print(f"ECEEE_FT_INIT_OK path={resolve_db_path(args.db)}")
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    validate_retry_args(args)
    since_date = validate_since_date(args.since_date)

    totals = {
        "checked": 0,
        "ready_new": 0,
        "ready_updated": 0,
        "ready_unchanged": 0,
        "ready_retained": 0,
        "failed_new": 0,
        "failed_updated": 0,
    }

    with connect_db(args.db) as conn:
        init_db(conn)
        discover_summary = discover_entries(
            conn=conn,
            index_url=args.index_url,
            timeout=args.timeout,
            max_bytes=args.max_bytes,
            user_agent=args.user_agent,
        )

        if args.discover_only:
            conn.commit()
            print(
                "ECEEE_DISCOVER_OK "
                f"discovered={discover_summary.discovered} "
                f"inserted={discover_summary.inserted} "
                f"updated={discover_summary.updated}"
            )
            return 0

        rows = list_candidate_entries(
            conn=conn,
            limit=args.limit,
            force=args.force,
            only_failed=args.only_failed,
            refetch_days=args.refetch_days,
            oldest_first=args.oldest_first,
            max_retries=args.max_retries,
            since_date=since_date,
        )
        if not rows:
            conn.commit()
            print(
                "ECEEE_FT_SYNC_OK "
                f"discovered={discover_summary.discovered} "
                f"inserted={discover_summary.inserted} "
                f"updated={discover_summary.updated} "
                "checked=0 ready_new=0 ready_updated=0 ready_unchanged=0 "
                "ready_retained=0 failed_new=0 failed_updated=0"
            )
            return 0

        for row in rows:
            state, _ = process_entry(conn, row, args)
            totals["checked"] += 1
            totals[state] += 1
            if totals["checked"] % 20 == 0:
                conn.commit()

        conn.commit()

    print(
        "ECEEE_FT_SYNC_OK "
        f"discovered={discover_summary.discovered} "
        f"inserted={discover_summary.inserted} "
        f"updated={discover_summary.updated} "
        f"checked={totals['checked']} "
        f"ready_new={totals['ready_new']} "
        f"ready_updated={totals['ready_updated']} "
        f"ready_unchanged={totals['ready_unchanged']} "
        f"ready_retained={totals['ready_retained']} "
        f"failed_new={totals['failed_new']} "
        f"failed_updated={totals['failed_updated']}"
    )

    failed_count = totals["failed_new"] + totals["failed_updated"]
    if args.fail_on_errors and failed_count > 0:
        return 1
    return 0


def cmd_fetch_entry(args: argparse.Namespace) -> int:
    validate_retry_args(args)

    entry_id = int(args.entry_id) if args.entry_id is not None else None
    url_value = canonicalize_url(args.url, base_url=args.index_url) if args.url else ""

    with connect_db(args.db) as conn:
        init_db(conn)

        if entry_id is not None:
            row = conn.execute(
                """
                SELECT id AS entry_id, title, url, published_at
                FROM entries
                WHERE id = ?
                """,
                (entry_id,),
            ).fetchone()
        else:
            row = conn.execute(
                """
                SELECT id AS entry_id, title, url, published_at
                FROM entries
                WHERE url = ?
                """,
                (url_value,),
            ).fetchone()

        if not row:
            locator = f"entry_id={entry_id}" if entry_id is not None else f"url={url_value}"
            print(f"ECEEE_FT_ERR reason=entry_not_found {locator}", file=sys.stderr)
            return 1

        state, result = process_entry(conn, row, args)
        conn.commit()

    error_text = normalize_space(str(result.last_error or ""))
    print(
        "ECEEE_FT_FETCH_OK "
        f"entry_id={row['entry_id']} "
        f"state={state} "
        f"status={result.status} "
        f"http_status={result.http_status or ''} "
        f"extractor={result.extractor} "
        f"chars={result.content_length} "
        f"error={error_text}"
    )
    return 1 if state in {"failed_new", "failed_updated"} else 0


def cmd_list_entries(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT id, published_at, title, url, discovered_at, last_seen_at
            FROM entries
            ORDER BY COALESCE(published_at, last_seen_at, discovered_at) DESC, id DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()

    print("entry_id\tpublished_at\ttitle\turl\tdiscovered_at\tlast_seen_at")
    for row in rows:
        title = normalize_space(str(row["title"] or "")).replace("\t", " ")
        url = normalize_space(str(row["url"] or "")).replace("\t", " ")
        print(
            f"{row['id']}\t{row['published_at'] or ''}\t{title}\t{url}\t"
            f"{row['discovered_at']}\t{row['last_seen_at']}"
        )
    return 0


def cmd_list_content(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        sql = """
        SELECT
            ec.entry_id,
            ec.status,
            ec.content_length,
            ec.retry_count,
            ec.next_retry_at,
            ec.http_status,
            ec.extractor,
            ec.fetched_at,
            COALESCE(ec.final_url, ec.source_url) AS url,
            ec.last_error,
            e.title,
            e.published_at
        FROM entry_content ec
        JOIN entries e ON e.id = ec.entry_id
        """
        params: list[Any] = []
        if args.status != "all":
            sql += " WHERE ec.status = ?"
            params.append(args.status)
        sql += " ORDER BY ec.updated_at DESC, ec.entry_id DESC LIMIT ?"
        params.append(args.limit)
        rows = conn.execute(sql, tuple(params)).fetchall()

    print(
        "entry_id\tstatus\tchars\tretry\tnext_retry_at\thttp_status\textractor\t"
        "fetched_at\tpublished_at\ttitle\turl\tlast_error"
    )
    for row in rows:
        title = normalize_space(str(row["title"] or "")).replace("\t", " ")
        url = normalize_space(str(row["url"] or "")).replace("\t", " ")
        error_text = normalize_space(str(row["last_error"] or "")).replace("\t", " ")
        print(
            f"{row['entry_id']}\t{row['status']}\t{row['content_length']}\t{row['retry_count']}\t"
            f"{row['next_retry_at'] or ''}\t"
            f"{row['http_status'] or ''}\t{row['extractor']}\t{row['fetched_at']}\t"
            f"{row['published_at'] or ''}\t{title}\t{url}\t{error_text}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover eceee news links and fetch full text into SQLite.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Create entries and entry_content tables.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_init.set_defaults(func=cmd_init_db)

    parser_sync = subparsers.add_parser("sync", help="Discover news links then fetch pending full text.")
    parser_sync.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_sync.add_argument("--index-url", default=DEFAULT_INDEX_URL, help=f"Index URL (default: {DEFAULT_INDEX_URL})")
    parser_sync.add_argument("--discover-only", action="store_true", help="Update entries table only, skip fulltext fetch.")
    parser_sync.add_argument("--limit", type=int, default=50, help="Max entries per run. 0 means no limit.")
    parser_sync.add_argument("--force", action="store_true", help="Refetch entries even when status is ready.")
    parser_sync.add_argument("--only-failed", action="store_true", help="Fetch only rows currently marked failed.")
    parser_sync.add_argument(
        "--since-date",
        default="",
        help="Optional lower bound on entries.published_at (YYYY-MM-DD).",
    )
    parser_sync.add_argument(
        "--refetch-days",
        type=int,
        default=0,
        help="When > 0, also refetch ready rows older than this number of days.",
    )
    parser_sync.add_argument(
        "--oldest-first",
        action="store_true",
        help="Use historical queue order (oldest first). Default prioritizes freshest rows.",
    )
    parser_sync.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser_sync.add_argument("--max-bytes", type=int, default=2_000_000, help="Max bytes to read per response.")
    parser_sync.add_argument("--min-chars", type=int, default=180, help="Minimum extracted characters for ready.")
    parser_sync.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Max failed retries per entry (default: {DEFAULT_MAX_RETRIES}, 0 means unlimited).",
    )
    parser_sync.add_argument(
        "--retry-backoff-minutes",
        type=int,
        default=DEFAULT_RETRY_BACKOFF_MINUTES,
        help=(
            f"Base minutes for exponential retry backoff "
            f"(default: {DEFAULT_RETRY_BACKOFF_MINUTES}, 0 disables waiting)."
        ),
    )
    parser_sync.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help=f"HTTP User-Agent (default: {DEFAULT_USER_AGENT})",
    )
    parser_sync.add_argument(
        "--disable-trafilatura",
        action="store_true",
        help="Force fallback parser instead of trafilatura.",
    )
    parser_sync.add_argument(
        "--fail-on-errors",
        action="store_true",
        help="Exit with code 1 when new failed rows are produced.",
    )
    parser_sync.set_defaults(func=cmd_sync)

    parser_fetch_entry = subparsers.add_parser("fetch-entry", help="Fetch one entry by id or URL.")
    parser_fetch_entry.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_fetch_entry.add_argument("--index-url", default=DEFAULT_INDEX_URL, help=f"Index URL (default: {DEFAULT_INDEX_URL})")
    locator_group = parser_fetch_entry.add_mutually_exclusive_group(required=True)
    locator_group.add_argument("--entry-id", type=int, help="Entry id from entries table.")
    locator_group.add_argument("--url", help="Entry URL from entries table.")
    parser_fetch_entry.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser_fetch_entry.add_argument("--max-bytes", type=int, default=2_000_000, help="Max bytes to read per response.")
    parser_fetch_entry.add_argument("--min-chars", type=int, default=180, help="Minimum extracted characters for ready.")
    parser_fetch_entry.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help=f"Max failed retries per entry (default: {DEFAULT_MAX_RETRIES}, 0 means unlimited).",
    )
    parser_fetch_entry.add_argument(
        "--retry-backoff-minutes",
        type=int,
        default=DEFAULT_RETRY_BACKOFF_MINUTES,
        help=(
            f"Base minutes for exponential retry backoff "
            f"(default: {DEFAULT_RETRY_BACKOFF_MINUTES}, 0 disables waiting)."
        ),
    )
    parser_fetch_entry.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help=f"HTTP User-Agent (default: {DEFAULT_USER_AGENT})",
    )
    parser_fetch_entry.add_argument(
        "--disable-trafilatura",
        action="store_true",
        help="Force fallback parser instead of trafilatura.",
    )
    parser_fetch_entry.set_defaults(func=cmd_fetch_entry)

    parser_list_entries = subparsers.add_parser("list-entries", help="List discovered entries.")
    parser_list_entries.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_list_entries.add_argument("--limit", type=int, default=100, help="Max rows to print.")
    parser_list_entries.set_defaults(func=cmd_list_entries)

    parser_list_content = subparsers.add_parser("list-content", help="List stored fulltext rows.")
    parser_list_content.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_list_content.add_argument("--status", choices=["all", "ready", "failed"], default="all", help="Status filter.")
    parser_list_content.add_argument("--limit", type=int, default=100, help="Max rows to print.")
    parser_list_content.set_defaults(func=cmd_list_content)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        return int(args.func(args))
    except sqlite3.Error as exc:
        print(f"ECEEE_FT_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        reason = str(exc)
        if reason.startswith("index_fetch_failed"):
            print(f"ECEEE_FT_ERR reason={reason}", file=sys.stderr)
            return 2
        if reason == "index_parse_empty":
            print("ECEEE_FT_ERR reason=index_parse_empty", file=sys.stderr)
            return 2
        if reason == "invalid_max_retries":
            print("ECEEE_FT_ERR reason=invalid_max_retries detail='--max-retries must be >= 0.'", file=sys.stderr)
            return 1
        if reason == "invalid_retry_backoff_minutes":
            print(
                "ECEEE_FT_ERR reason=invalid_retry_backoff_minutes "
                "detail='--retry-backoff-minutes must be >= 0.'",
                file=sys.stderr,
            )
            return 1
        if reason == "invalid_since_date":
            print("ECEEE_FT_ERR reason=invalid_since_date detail='Use YYYY-MM-DD.'", file=sys.stderr)
            return 1
        print(f"ECEEE_FT_ERR reason=value_error detail={reason}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ECEEE_FT_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
