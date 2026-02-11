#!/usr/bin/env python3
"""Fetch article full text for RSS entries already stored in SQLite."""

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
from urllib.request import Request, urlopen

try:
    import trafilatura  # type: ignore
except ImportError:
    trafilatura = None


DEFAULT_DB_FILENAME = "ai_rss.db"
DEFAULT_DB_PATH = os.environ.get("AI_RSS_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_USER_AGENT = "ai-tech-fulltext-fetch/1.0 (+https://github.com/tiangong-ai/skills)"
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

CREATE INDEX IF NOT EXISTS idx_entry_content_status ON entry_content(status);
CREATE INDEX IF NOT EXISTS idx_entry_content_updated_at ON entry_content(updated_at);
CREATE INDEX IF NOT EXISTS idx_entry_content_retry_count ON entry_content(retry_count);
CREATE INDEX IF NOT EXISTS idx_entry_content_status_updated_entry
    ON entry_content(status, updated_at DESC, entry_id DESC);
"""


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


def ensure_entries_table(conn: sqlite3.Connection) -> None:
    if table_exists(conn, "entries"):
        return
    raise ValueError("entries_table_missing")


def init_db(conn: sqlite3.Connection) -> None:
    ensure_entries_table(conn)
    conn.executescript(SCHEMA_SQL)
    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(entry_content)").fetchall()
    }
    if "next_retry_at" not in columns:
        conn.execute("ALTER TABLE entry_content ADD COLUMN next_retry_at TEXT")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_entry_content_failed_retry "
        "ON entry_content(status, next_retry_at, retry_count)"
    )


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
) -> list[sqlite3.Row]:
    sql = """
    SELECT
        e.id AS entry_id,
        e.title,
        e.canonical_url,
        e.url,
        ec.status AS content_status,
        ec.fetched_at,
        ec.updated_at
    FROM entries e
    LEFT JOIN entry_content ec ON ec.entry_id = e.id
    WHERE COALESCE(NULLIF(e.canonical_url, ''), NULLIF(e.url, '')) IS NOT NULL
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
        # Prioritize fresh/unfetched rows to improve hit-rate under bounded --limit.
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
            COALESCE(e.published_at, e.first_seen_at, e.last_seen_at, '') DESC,
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
    source_url = choose_entry_url(row)
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
            SELECT id AS entry_id, title, canonical_url, url
            FROM entries
            WHERE id = ?
            """,
            (args.entry_id,),
        ).fetchone()
        if not row:
            print(f"FULLTEXT_ERR reason=entry_not_found entry_id={args.entry_id}", file=sys.stderr)
            return 1

        state, result = process_entry(conn, row, args)
        conn.commit()

    error_text = normalize_space(str(result.last_error or ""))
    print(
        "FT_FETCH_OK "
        f"entry_id={args.entry_id} "
        f"state={state} "
        f"status={result.status} "
        f"http_status={result.http_status or ''} "
        f"extractor={result.extractor} "
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
            e.title
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

    print("entry_id\tstatus\tchars\tretry\tnext_retry_at\thttp_status\textractor\tfetched_at\ttitle\turl\tlast_error")
    for row in rows:
        title = (str(row["title"] or "")).replace("\t", " ").replace("\n", " ").strip()
        url = (str(row["url"] or "")).replace("\t", " ").replace("\n", " ").strip()
        error_text = (str(row["last_error"] or "")).replace("\t", " ").replace("\n", " ").strip()
        print(
            f"{row['entry_id']}\t{row['status']}\t{row['content_length']}\t{row['retry_count']}\t"
            f"{row['next_retry_at'] or ''}\t"
            f"{row['http_status'] or ''}\t{row['extractor']}\t{row['fetched_at']}\t"
            f"{title}\t{url}\t{error_text}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch full text from entries in SQLite and store results in entry_content.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Create entry_content table.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_init.set_defaults(func=cmd_init_db)

    parser_sync = subparsers.add_parser("sync", help="Fetch full text for pending entries.")
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
    parser_sync.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser_sync.add_argument("--max-bytes", type=int, default=2_000_000, help="Max bytes to read per response.")
    parser_sync.add_argument("--min-chars", type=int, default=300, help="Minimum extracted characters for ready.")
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

    parser_fetch_entry = subparsers.add_parser("fetch-entry", help="Fetch one entry by id.")
    parser_fetch_entry.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_fetch_entry.add_argument("--entry-id", type=int, required=True, help="Entry id from entries table.")
    parser_fetch_entry.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")
    parser_fetch_entry.add_argument("--max-bytes", type=int, default=2_000_000, help="Max bytes to read per response.")
    parser_fetch_entry.add_argument("--min-chars", type=int, default=300, help="Minimum extracted characters for ready.")
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

    parser_list = subparsers.add_parser("list-content", help="List stored fulltext rows.")
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
        if reason == "entries_table_missing":
            print(
                "FULLTEXT_ERR reason=entries_table_missing "
                "detail='Run ai-tech-rss-fetch init-db and sync first.'",
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
