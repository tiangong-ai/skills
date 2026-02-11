#!/usr/bin/env python3
"""Fetch article full text for DOI-keyed RSS entries already stored in SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
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
from urllib.parse import quote
from urllib.request import Request, urlopen

try:
    import trafilatura  # type: ignore
except ImportError:
    trafilatura = None


DEFAULT_DB_FILENAME = "sustainability_rss.db"
DEFAULT_DB_PATH = os.environ.get("SUSTAIN_RSS_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_USER_AGENT = "sustainability-fulltext-fetch/1.0 (+https://github.com/tiangong-ai/skills)"
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_MINUTES = 30
DEFAULT_API_TIMEOUT_SECONDS = 12.0
MAX_RETRY_BACKOFF_MINUTES = 24 * 60
OPENALEX_BASE = "https://api.openalex.org/works/https://doi.org/"
SEMANTIC_SCHOLAR_BASE = "https://api.semanticscholar.org/graph/v1/paper/"
DOI_PATTERN = re.compile(r"10\.\d{4,9}/[-._;()/:A-Z0-9]+", re.IGNORECASE)
BINARY_CONTENT_PREFIXES = (
    "application/pdf",
    "application/zip",
    "application/octet-stream",
    "image/",
    "audio/",
    "video/",
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS entry_content (
    doi TEXT PRIMARY KEY,
    source_url TEXT,
    final_url TEXT,
    http_status INTEGER,
    extractor TEXT NOT NULL,
    content_kind TEXT NOT NULL DEFAULT 'fulltext',
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
    FOREIGN KEY(doi) REFERENCES entries(doi) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_entry_content_status ON entry_content(status);
CREATE INDEX IF NOT EXISTS idx_entry_content_updated_at ON entry_content(updated_at);
CREATE INDEX IF NOT EXISTS idx_entry_content_retry_count ON entry_content(retry_count);
CREATE INDEX IF NOT EXISTS idx_entry_content_status_updated_doi
    ON entry_content(status, updated_at DESC, doi DESC);
CREATE INDEX IF NOT EXISTS idx_entry_content_failed_retry
    ON entry_content(status, next_retry_at, retry_count);
CREATE INDEX IF NOT EXISTS idx_entry_content_failed_retry_doi
    ON entry_content(status, next_retry_at, retry_count, doi);
CREATE INDEX IF NOT EXISTS idx_entry_content_status_fetched_doi
    ON entry_content(status, fetched_at, doi);
"""

EVENT_TS_SQL_EXPR = (
    "COALESCE("
    "CASE WHEN e.published_at GLOB '????-??-??T*Z' THEN e.published_at END, "
    "e.first_seen_at, "
    "e.last_seen_at"
    ")"
)
ENTRY_ELIGIBILITY_SQL_EXPR = (
    "("
    "(e.doi_is_surrogate = 0 AND e.doi NOT LIKE 'rss-hash:%') "
    "OR COALESCE(NULLIF(e.canonical_url, ''), NULLIF(e.url, '')) IS NOT NULL"
    ")"
)


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
class ApiFetchResult:
    ok: bool
    source: str | None
    text: str | None
    error: str | None


@dataclass
class ExtractResult:
    status: str
    source_url: str
    final_url: str | None
    http_status: int | None
    extractor: str
    content_kind: str
    content_text: str | None
    content_hash: str | None
    content_length: int
    last_error: str | None


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


def normalize_doi(raw: Any) -> str:
    text = normalize_space(str(raw or ""))
    if not text:
        return ""
    if text.lower().startswith("rss-hash:"):
        return ""
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^doi:\s*", "", text, flags=re.IGNORECASE)
    text = text.strip().strip("<>")
    text = text.rstrip(".,;:!?)]}>'\"")
    match = DOI_PATTERN.search(text)
    if not match:
        return ""
    doi = match.group(0).rstrip(".,;:!?)]}>'\"")
    return doi.lower()


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

    return Path(raw).expanduser()


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


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return bool(row)


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    if not table_exists(conn, table_name):
        return set()
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {str(row["name"]) for row in rows}


def ensure_entries_table(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "entries"):
        raise ValueError("entries_table_missing")
    columns = table_columns(conn, "entries")
    required = {"doi", "is_relevant", "doi_is_surrogate"}
    if not required.issubset(columns):
        raise ValueError("entries_table_missing_doi")


def migrate_legacy_entry_content_if_needed(conn: sqlite3.Connection) -> None:
    if not table_exists(conn, "entry_content"):
        return
    columns = table_columns(conn, "entry_content")
    if "doi" in columns:
        return

    legacy_name = f"entry_content_legacy_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
    conn.execute(f"ALTER TABLE entry_content RENAME TO {legacy_name}")
    conn.executescript(SCHEMA_SQL)

    entries_columns = table_columns(conn, "entries")
    if "id" not in entries_columns:
        # Old entry_content relied on entries.id. If the id column is gone after schema migration,
        # keep legacy rows untouched in backup table and continue with fresh DOI-based table.
        conn.commit()
        return

    try:
        rows = conn.execute(
            f"""
            SELECT
                e.doi,
                ec.source_url,
                ec.final_url,
                ec.http_status,
                ec.extractor,
                ec.content_text,
                ec.content_hash,
                ec.content_length,
                ec.fetched_at,
                ec.last_error,
                ec.retry_count,
                ec.next_retry_at,
                ec.status,
                ec.created_at,
                ec.updated_at
            FROM {legacy_name} ec
            JOIN entries e ON e.id = ec.entry_id
            WHERE e.doi IS NOT NULL AND e.doi <> ''
            """
        ).fetchall()
    except sqlite3.Error:
        conn.commit()
        return

    for row in rows:
        conn.execute(
            """
            INSERT INTO entry_content (
                doi, source_url, final_url, http_status, extractor, content_kind,
                content_text, content_hash, content_length, fetched_at, last_error,
                retry_count, next_retry_at, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(doi) DO UPDATE SET
                source_url = excluded.source_url,
                final_url = excluded.final_url,
                http_status = excluded.http_status,
                extractor = excluded.extractor,
                content_kind = excluded.content_kind,
                content_text = excluded.content_text,
                content_hash = excluded.content_hash,
                content_length = excluded.content_length,
                fetched_at = excluded.fetched_at,
                last_error = excluded.last_error,
                retry_count = excluded.retry_count,
                next_retry_at = excluded.next_retry_at,
                status = excluded.status,
                updated_at = excluded.updated_at
            """,
            (
                row["doi"],
                row["source_url"],
                row["final_url"],
                row["http_status"],
                row["extractor"],
                "fulltext",
                row["content_text"],
                row["content_hash"],
                row["content_length"],
                row["fetched_at"],
                row["last_error"],
                row["retry_count"],
                row["next_retry_at"],
                row["status"],
                row["created_at"],
                row["updated_at"],
            ),
        )

    conn.commit()


def init_db(conn: sqlite3.Connection) -> None:
    ensure_entries_table(conn)
    migrate_legacy_entry_content_if_needed(conn)
    conn.executescript(SCHEMA_SQL)
    columns = table_columns(conn, "entry_content")
    if "next_retry_at" not in columns:
        conn.execute("ALTER TABLE entry_content ADD COLUMN next_retry_at TEXT")
    if "content_kind" not in columns:
        conn.execute("ALTER TABLE entry_content ADD COLUMN content_kind TEXT NOT NULL DEFAULT 'fulltext'")


def choose_entry_url(row: sqlite3.Row) -> str:
    canonical_url = normalize_space(str(row["canonical_url"] or ""))
    raw_url = normalize_space(str(row["url"] or ""))
    return canonical_url or raw_url


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


def build_extract_result_from_html(
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
            content_kind="fulltext",
            content_text=None,
            content_hash=None,
            content_length=0,
            last_error=fetch_response.error or "fetch_failed",
        )

    html = fetch_response.html or ""
    target_url = fetch_response.final_url or fetch_response.source_url

    extractor = "html-parser"
    text = ""
    if not disable_trafilatura:
        text = extract_with_trafilatura(html, target_url)
        if text:
            extractor = "trafilatura"
    if not text:
        text = extract_with_fallback_parser(html)
        extractor = "html-parser"

    if not text:
        return ExtractResult(
            status="failed",
            source_url=fetch_response.source_url,
            final_url=fetch_response.final_url,
            http_status=fetch_response.http_status,
            extractor=extractor,
            content_kind="fulltext",
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
            content_kind="fulltext",
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
        content_kind="fulltext",
        content_text=text,
        content_hash=sha256_hexdigest(text),
        content_length=content_length,
        last_error=None,
    )


def http_get_json(url: str, headers: dict[str, str], timeout: float) -> tuple[int | None, dict[str, Any]]:
    request = Request(url=url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            payload = json.loads(body) if body else {}
            return int(getattr(response, "status", 200)), payload
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        payload: dict[str, Any] = {}
        if body:
            try:
                payload = json.loads(body)
            except json.JSONDecodeError:
                payload = {"raw": body[:500]}
        return exc.code, payload
    except URLError as exc:
        return None, {"error": str(exc.reason)}


def reconstruct_abstract(inverted_index: dict[str, list[int]] | None) -> str | None:
    if not inverted_index:
        return None

    indexed_tokens: list[tuple[int, str]] = []
    for token, positions in inverted_index.items():
        if not isinstance(positions, list):
            continue
        for pos in positions:
            if isinstance(pos, int):
                indexed_tokens.append((pos, token))

    if not indexed_tokens:
        return None

    indexed_tokens.sort(key=lambda item: item[0])
    text = " ".join(token for _, token in indexed_tokens)
    text = re.sub(r"\s+([,.;:!?%)\]}])", r"\1", text)
    text = re.sub(r"([(\[{])\s+", r"\1", text)
    text = re.sub(r"\s{2,}", " ", text).strip()
    return text or None


def fetch_openalex(doi: str, email: str | None, timeout: float) -> tuple[str | None, str | None]:
    normalized = normalize_doi(doi)
    if not normalized:
        return None, "invalid_doi"

    url = OPENALEX_BASE + quote(normalized, safe="")
    headers = {"Accept": "application/json"}
    if email:
        ua = email if email.lower().startswith("mailto:") else f"mailto:{email}"
        headers["User-Agent"] = ua
    else:
        headers["User-Agent"] = "sustainability-fulltext-fetch/1.0"

    status, payload = http_get_json(url, headers, timeout)
    if status is None:
        return None, "openalex_network_error"
    if status == 404:
        return None, "openalex_not_found"
    if 500 <= status < 600:
        return None, "openalex_server_error"
    if status != 200:
        return None, f"openalex_http_{status}"

    abstract = reconstruct_abstract(payload.get("abstract_inverted_index"))
    if abstract:
        return abstract, None
    return None, "openalex_no_abstract"


def fetch_semantic_scholar(doi: str, api_key: str | None, timeout: float) -> tuple[str | None, str | None]:
    normalized = normalize_doi(doi)
    if not normalized:
        return None, "invalid_doi"

    paper_id = quote(f"DOI:{normalized}", safe="")
    url = f"{SEMANTIC_SCHOLAR_BASE}{paper_id}?fields=abstract"
    headers = {"Accept": "application/json"}
    if api_key:
        headers["x-api-key"] = api_key

    status, payload = http_get_json(url, headers, timeout)
    if status is None:
        return None, "s2_network_error"
    if status == 404:
        return None, "s2_not_found"
    if status == 429:
        return None, "s2_rate_limited"
    if 500 <= status < 600:
        return None, "s2_server_error"
    if status != 200:
        return None, f"s2_http_{status}"

    abstract = payload.get("abstract")
    if isinstance(abstract, str) and abstract.strip():
        return abstract.strip(), None
    return None, "s2_no_abstract"


def fetch_api_metadata(doi: str, openalex_email: str | None, s2_api_key: str | None, timeout: float) -> ApiFetchResult:
    normalized = normalize_doi(doi)
    if not normalized:
        return ApiFetchResult(ok=False, source=None, text=None, error="invalid_or_surrogate_doi")

    openalex_text, openalex_error = fetch_openalex(normalized, openalex_email, timeout)
    if openalex_text:
        return ApiFetchResult(ok=True, source="openalex", text=openalex_text, error=None)

    s2_text, s2_error = fetch_semantic_scholar(normalized, s2_api_key, timeout)
    if s2_text:
        return ApiFetchResult(ok=True, source="semanticscholar", text=s2_text, error=None)

    errors = [err for err in [openalex_error, s2_error] if err]
    return ApiFetchResult(ok=False, source=None, text=None, error="|".join(errors) if errors else "api_missing")


def build_extract_result_from_api(doi: str, api_result: ApiFetchResult, min_chars: int) -> ExtractResult:
    if not api_result.ok or not api_result.text:
        return ExtractResult(
            status="failed",
            source_url=f"https://doi.org/{doi}",
            final_url=f"https://doi.org/{doi}",
            http_status=None,
            extractor=api_result.source or "api",
            content_kind="abstract",
            content_text=None,
            content_hash=None,
            content_length=0,
            last_error=api_result.error or "api_missing",
        )

    text = clean_text(api_result.text)
    content_length = len(text)
    if content_length < max(min_chars, 1):
        return ExtractResult(
            status="failed",
            source_url=f"https://doi.org/{doi}",
            final_url=f"https://doi.org/{doi}",
            http_status=None,
            extractor=api_result.source or "api",
            content_kind="abstract",
            content_text=None,
            content_hash=None,
            content_length=content_length,
            last_error=f"api_text_too_short:{content_length}",
        )

    return ExtractResult(
        status="ready",
        source_url=f"https://doi.org/{doi}",
        final_url=f"https://doi.org/{doi}",
        http_status=200,
        extractor=api_result.source or "api",
        content_kind="abstract",
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
) -> list[sqlite3.Row]:
    def _fetch_rows(sql: str, params: list[Any], query_limit: int) -> list[sqlite3.Row]:
        local_sql = sql
        local_params = list(params)
        if query_limit > 0:
            local_sql += " LIMIT ?"
            local_params.append(query_limit)
        return conn.execute(local_sql, tuple(local_params)).fetchall()

    def _merge_groups(groups: list[list[sqlite3.Row]], merged_limit: int) -> list[sqlite3.Row]:
        merged: list[sqlite3.Row] = []
        seen: set[str] = set()
        for rows in groups:
            for row in rows:
                doi = str(row["doi"] or "")
                if not doi or doi in seen:
                    continue
                seen.add(doi)
                merged.append(row)
                if merged_limit > 0 and len(merged) >= merged_limit:
                    return merged
        return merged

    def _load_missing_rows(query_limit: int) -> list[sqlite3.Row]:
        sql = f"""
        SELECT
            e.doi,
            e.title,
            e.canonical_url,
            e.url
        FROM entries e
        LEFT JOIN entry_content ec ON ec.doi = e.doi
        WHERE e.is_relevant = 1
          AND {ENTRY_ELIGIBILITY_SQL_EXPR}
          AND ec.doi IS NULL
        ORDER BY {EVENT_TS_SQL_EXPR} DESC, e.doi DESC
        """
        return _fetch_rows(sql, [], query_limit)

    def _load_failed_rows_due(query_limit: int) -> list[sqlite3.Row]:
        now_iso = now_utc_iso()
        retry_clause = "ec.retry_count < ?" if max_retries > 0 else "1=1"
        retry_params = [max_retries] if max_retries > 0 else []

        sql_null = f"""
        SELECT
            e.doi,
            e.title,
            e.canonical_url,
            e.url
        FROM entry_content ec
        JOIN entries e ON e.doi = ec.doi
        WHERE ec.status = 'failed'
          AND ec.next_retry_at IS NULL
          AND {retry_clause}
          AND e.is_relevant = 1
          AND {ENTRY_ELIGIBILITY_SQL_EXPR}
        ORDER BY ec.retry_count ASC
        """
        rows_null = _fetch_rows(sql_null, retry_params, query_limit)
        if query_limit > 0 and len(rows_null) >= query_limit:
            return rows_null

        remain = 0 if query_limit <= 0 else max(query_limit - len(rows_null), 0)
        sql_due = f"""
        SELECT
            e.doi,
            e.title,
            e.canonical_url,
            e.url
        FROM entry_content ec
        JOIN entries e ON e.doi = ec.doi
        WHERE ec.status = 'failed'
          AND ec.next_retry_at <= ?
          AND {retry_clause}
          AND e.is_relevant = 1
          AND {ENTRY_ELIGIBILITY_SQL_EXPR}
        ORDER BY ec.next_retry_at ASC, ec.retry_count ASC
        """
        rows_due = _fetch_rows(sql_due, [now_iso, *retry_params], remain if query_limit > 0 else query_limit)
        return _merge_groups([rows_null, rows_due], query_limit)

    def _load_ready_stale_rows(query_limit: int, threshold_iso: str) -> list[sqlite3.Row]:
        sql = f"""
        SELECT
            e.doi,
            e.title,
            e.canonical_url,
            e.url
        FROM entry_content ec
        JOIN entries e ON e.doi = ec.doi
        WHERE ec.status = 'ready'
          AND ec.fetched_at < ?
          AND e.is_relevant = 1
          AND {ENTRY_ELIGIBILITY_SQL_EXPR}
        ORDER BY ec.fetched_at ASC
        """
        return _fetch_rows(sql, [threshold_iso], query_limit)

    if oldest_first:
        sql = f"""
        SELECT
            e.doi,
            e.title,
            e.canonical_url,
            e.url
        FROM entries e
        LEFT JOIN entry_content ec ON ec.doi = e.doi
        WHERE e.is_relevant = 1
          AND {ENTRY_ELIGIBILITY_SQL_EXPR}
        """
        params: list[Any] = []
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
                    " AND (ec.doi IS NULL "
                    f"OR ({failed_retry_clause}) "
                    "OR (ec.status = 'ready' AND ec.fetched_at < ?))"
                )
                params.extend(failed_retry_params)
                params.append(threshold_iso)
            else:
                sql += f" AND (ec.doi IS NULL OR ({failed_retry_clause}))"
                params.extend(failed_retry_params)

        sql += " ORDER BY COALESCE(ec.updated_at, '') ASC, e.doi ASC"
        return _fetch_rows(sql, params, limit)

    if force:
        sql_force = f"""
        SELECT
            e.doi,
            e.title,
            e.canonical_url,
            e.url
        FROM entries e
        WHERE e.is_relevant = 1
          AND {ENTRY_ELIGIBILITY_SQL_EXPR}
        ORDER BY {EVENT_TS_SQL_EXPR} DESC, e.doi DESC
        """
        return _fetch_rows(sql_force, [], limit)

    if only_failed:
        return _load_failed_rows_due(limit)

    groups: list[list[sqlite3.Row]] = [_load_missing_rows(limit), _load_failed_rows_due(limit)]
    if refetch_days > 0:
        threshold = datetime.now(timezone.utc) - timedelta(days=refetch_days)
        threshold_iso = threshold.replace(microsecond=0).isoformat().replace("+00:00", "Z")
        groups.append(_load_ready_stale_rows(limit, threshold_iso))
    return _merge_groups(groups, limit)


def persist_extract_result(
    conn: sqlite3.Connection,
    doi: str,
    result: ExtractResult,
    fetched_at: str,
    max_retries: int,
    retry_backoff_minutes: int,
) -> str:
    existing = conn.execute(
        """
        SELECT status, content_hash, content_text, content_length, retry_count, next_retry_at
        FROM entry_content
        WHERE doi = ?
        """,
        (doi,),
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
                doi, source_url, final_url, http_status, extractor, content_kind, content_text,
                content_hash, content_length, fetched_at, last_error, retry_count, next_retry_at, status,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                doi,
                result.source_url,
                result.final_url,
                result.http_status,
                result.extractor,
                result.content_kind,
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
            content_kind = ?,
            content_text = ?,
            content_hash = ?,
            content_length = ?,
            fetched_at = ?,
            last_error = ?,
            retry_count = ?,
            next_retry_at = ?,
            status = ?,
            updated_at = ?
        WHERE doi = ?
        """,
        (
            result.source_url,
            result.final_url,
            result.http_status,
            result.extractor,
            result.content_kind,
            content_text,
            content_hash,
            content_length,
            fetched_at,
            last_error,
            retry_count,
            next_retry_at,
            status_value,
            fetched_at,
            doi,
        ),
    )
    return state


def process_entry(conn: sqlite3.Connection, row: sqlite3.Row, args: argparse.Namespace) -> tuple[str, ExtractResult]:
    doi = normalize_space(str(row["doi"] or ""))
    source_url = choose_entry_url(row)

    api_error = ""
    result: ExtractResult | None = None
    if not args.disable_api_metadata:
        api_result = fetch_api_metadata(
            doi=doi,
            openalex_email=resolve_openalex_email(args.openalex_email),
            s2_api_key=resolve_s2_api_key(args.s2_api_key),
            timeout=args.api_timeout,
        )
        if api_result.ok:
            result = build_extract_result_from_api(doi, api_result, min_chars=args.api_min_chars)
            if result.status == "ready":
                fetched_at = now_utc_iso()
                state = persist_extract_result(
                    conn=conn,
                    doi=doi,
                    result=result,
                    fetched_at=fetched_at,
                    max_retries=args.max_retries,
                    retry_backoff_minutes=args.retry_backoff_minutes,
                )
                return state, result
            api_error = result.last_error or "api_failed"
        else:
            api_error = api_result.error or "api_failed"

    if not source_url:
        err = "missing_source_url"
        if api_error:
            err = f"{api_error}|{err}"
        result = ExtractResult(
            status="failed",
            source_url="",
            final_url=None,
            http_status=None,
            extractor="none",
            content_kind="fulltext",
            content_text=None,
            content_hash=None,
            content_length=0,
            last_error=err,
        )
    else:
        fetch_response = fetch_html(
            url=source_url,
            timeout=args.timeout,
            max_bytes=args.max_bytes,
            user_agent=args.user_agent,
        )
        result = build_extract_result_from_html(
            fetch_response=fetch_response,
            min_chars=args.min_chars,
            disable_trafilatura=args.disable_trafilatura,
        )
        if result.status != "ready" and api_error:
            result.last_error = f"{api_error}|{result.last_error or 'web_failed'}"

    fetched_at = now_utc_iso()
    state = persist_extract_result(
        conn=conn,
        doi=doi,
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


def resolve_openalex_email(value: str | None) -> str | None:
    return value or os.getenv("OPENALEX_EMAIL")


def resolve_s2_api_key(value: str | None) -> str | None:
    return value or os.getenv("S2_API_KEY")


def cmd_init_db(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        conn.commit()
    print(f"FT_INIT_OK path={args.db}")
    return 0


def cmd_fetch_entry(args: argparse.Namespace) -> int:
    validate_retry_args(args)
    with connect_db(args.db) as conn:
        init_db(conn)
        row = conn.execute(
            """
            SELECT doi, title, canonical_url, url, doi_is_surrogate, is_relevant
            FROM entries
            WHERE doi = ?
            """,
            (normalize_space(args.doi),),
        ).fetchone()
        if not row:
            print(f"FULLTEXT_ERR reason=entry_not_found doi={args.doi}", file=sys.stderr)
            return 1
        if row["is_relevant"] != 1:
            print(f"FULLTEXT_ERR reason=entry_not_relevant doi={args.doi}", file=sys.stderr)
            return 1

        state, result = process_entry(conn, row, args)
        conn.commit()

    error_text = normalize_space(str(result.last_error or ""))
    print(
        "FT_FETCH_OK "
        f"doi={args.doi} "
        f"state={state} "
        f"status={result.status} "
        f"http_status={result.http_status or ''} "
        f"extractor={result.extractor} "
        f"kind={result.content_kind} "
        f"chars={result.content_length} "
        f"error={error_text}"
    )
    return 1 if state in {"failed_new", "failed_updated"} else 0


def cmd_sync(args: argparse.Namespace) -> int:
    validate_retry_args(args)
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
        rows = list_candidate_entries(
            conn=conn,
            limit=args.limit,
            force=args.force,
            only_failed=args.only_failed,
            refetch_days=args.refetch_days,
            oldest_first=args.oldest_first,
            max_retries=args.max_retries,
        )
        if not rows:
            print(
                "FT_SYNC_OK checked=0 ready_new=0 ready_updated=0 ready_unchanged=0 "
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
        "FT_SYNC_OK "
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


def cmd_list_content(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        sql = """
        SELECT
            ec.doi,
            ec.status,
            ec.content_kind,
            ec.content_length,
            ec.retry_count,
            ec.next_retry_at,
            ec.http_status,
            ec.extractor,
            ec.fetched_at,
            COALESCE(ec.final_url, ec.source_url) AS url,
            ec.last_error,
            e.title
        FROM entry_content ec
        LEFT JOIN entries e ON e.doi = ec.doi
        """
        params: list[Any] = []
        if args.status != "all":
            sql += " WHERE ec.status = ?"
            params.append(args.status)
        sql += " ORDER BY ec.updated_at DESC, ec.doi DESC LIMIT ?"
        params.append(args.limit)
        rows = conn.execute(sql, tuple(params)).fetchall()

    print("doi\tstatus\tkind\tchars\tretry\tnext_retry_at\thttp_status\textractor\tfetched_at\ttitle\turl\tlast_error")
    for row in rows:
        title = (str(row["title"] or "")).replace("\t", " ").replace("\n", " ").strip()
        url = (str(row["url"] or "")).replace("\t", " ").replace("\n", " ").strip()
        error_text = (str(row["last_error"] or "")).replace("\t", " ").replace("\n", " ").strip()
        print(
            f"{row['doi']}\t{row['status']}\t{row['content_kind']}\t{row['content_length']}\t{row['retry_count']}\t"
            f"{row['next_retry_at'] or ''}\t"
            f"{row['http_status'] or ''}\t{row['extractor']}\t{row['fetched_at']}\t"
            f"{title}\t{url}\t{error_text}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch API metadata first, then webpage full text fallback, and store by DOI.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Create entry_content table.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_init.set_defaults(func=cmd_init_db)

    parser_sync = subparsers.add_parser("sync", help="Fetch content for pending relevant entries.")
    parser_sync.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_sync.add_argument("--limit", type=int, default=50, help="Max entries per run. 0 means no limit.")
    parser_sync.add_argument("--force", action="store_true", help="Refetch entries even when status is ready.")
    parser_sync.add_argument("--only-failed", action="store_true", help="Fetch only rows currently marked failed.")
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
    parser_sync.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds for webpage fallback.")
    parser_sync.add_argument("--max-bytes", type=int, default=2_000_000, help="Max bytes to read per webpage response.")
    parser_sync.add_argument("--min-chars", type=int, default=300, help="Minimum extracted chars for webpage-ready rows.")
    parser_sync.add_argument(
        "--openalex-email",
        default=None,
        help="Email for OpenAlex User-Agent; falls back to OPENALEX_EMAIL.",
    )
    parser_sync.add_argument(
        "--s2-api-key",
        default=None,
        help="Semantic Scholar API key; falls back to S2_API_KEY.",
    )
    parser_sync.add_argument(
        "--api-timeout",
        type=float,
        default=DEFAULT_API_TIMEOUT_SECONDS,
        help=f"API timeout seconds (default: {DEFAULT_API_TIMEOUT_SECONDS}).",
    )
    parser_sync.add_argument(
        "--api-min-chars",
        type=int,
        default=80,
        help="Minimum chars for API abstract acceptance.",
    )
    parser_sync.add_argument(
        "--disable-api-metadata",
        action="store_true",
        help="Disable API-first (OpenAlex/S2) and use webpage extraction only.",
    )
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
        help=f"HTTP User-Agent for webpage fallback (default: {DEFAULT_USER_AGENT})",
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

    parser_fetch_entry = subparsers.add_parser("fetch-entry", help="Fetch one entry by DOI.")
    parser_fetch_entry.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_fetch_entry.add_argument("--doi", required=True, help="DOI value from entries table.")
    parser_fetch_entry.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds for webpage fallback.")
    parser_fetch_entry.add_argument("--max-bytes", type=int, default=2_000_000, help="Max bytes to read per webpage response.")
    parser_fetch_entry.add_argument("--min-chars", type=int, default=300, help="Minimum extracted chars for webpage-ready rows.")
    parser_fetch_entry.add_argument(
        "--openalex-email",
        default=None,
        help="Email for OpenAlex User-Agent; falls back to OPENALEX_EMAIL.",
    )
    parser_fetch_entry.add_argument(
        "--s2-api-key",
        default=None,
        help="Semantic Scholar API key; falls back to S2_API_KEY.",
    )
    parser_fetch_entry.add_argument(
        "--api-timeout",
        type=float,
        default=DEFAULT_API_TIMEOUT_SECONDS,
        help=f"API timeout seconds (default: {DEFAULT_API_TIMEOUT_SECONDS}).",
    )
    parser_fetch_entry.add_argument(
        "--api-min-chars",
        type=int,
        default=80,
        help="Minimum chars for API abstract acceptance.",
    )
    parser_fetch_entry.add_argument(
        "--disable-api-metadata",
        action="store_true",
        help="Disable API-first (OpenAlex/S2) and use webpage extraction only.",
    )
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
        help=f"HTTP User-Agent for webpage fallback (default: {DEFAULT_USER_AGENT})",
    )
    parser_fetch_entry.add_argument(
        "--disable-trafilatura",
        action="store_true",
        help="Force fallback parser instead of trafilatura.",
    )
    parser_fetch_entry.set_defaults(func=cmd_fetch_entry)

    parser_list = subparsers.add_parser("list-content", help="List stored content rows.")
    parser_list.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_list.add_argument("--status", choices=["all", "ready", "failed"], default="all", help="Status filter.")
    parser_list.add_argument("--limit", type=int, default=100, help="Max rows to print.")
    parser_list.set_defaults(func=cmd_list_content)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        return int(args.func(args))
    except sqlite3.Error as exc:
        print(f"FULLTEXT_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        reason = str(exc)
        if reason in {"entries_table_missing", "entries_table_missing_doi"}:
            print(
                "FULLTEXT_ERR reason=entries_table_missing "
                "detail='Run sustainability-rss-fetch init-db and collect-window first.'",
                file=sys.stderr,
            )
            return 2
        if reason == "invalid_max_retries":
            print(
                "FULLTEXT_ERR reason=invalid_max_retries detail='--max-retries must be >= 0.'",
                file=sys.stderr,
            )
            return 1
        if reason == "invalid_retry_backoff_minutes":
            print(
                "FULLTEXT_ERR reason=invalid_retry_backoff_minutes "
                "detail='--retry-backoff-minutes must be >= 0.'",
                file=sys.stderr,
            )
            return 1
        print(f"FULLTEXT_ERR reason=value_error detail={reason}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"FULLTEXT_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
