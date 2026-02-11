#!/usr/bin/env python3
"""Subscribe to RSS feeds and persist metadata into SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    import feedparser  # type: ignore
except ImportError:
    feedparser = None


DEFAULT_DB_FILENAME = "ai_rss.db"
DEFAULT_DB_PATH = os.environ.get("AI_RSS_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_USER_AGENT = "ai-tech-rss-fetch/1.0 (+https://github.com/tiangong-ai/skills)"
TRACKING_QUERY_PARAMS = {
    "ref",
    "source",
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
}

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS feeds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feed_url TEXT NOT NULL UNIQUE,
    feed_title TEXT,
    site_url TEXT,
    etag TEXT,
    last_modified TEXT,
    last_checked_at TEXT,
    last_success_at TEXT,
    last_status INTEGER,
    last_error TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dedupe_key TEXT NOT NULL UNIQUE,
    first_feed_id INTEGER NOT NULL,
    last_feed_id INTEGER NOT NULL,
    guid TEXT,
    url TEXT,
    canonical_url TEXT,
    title TEXT,
    author TEXT,
    published_at TEXT,
    updated_at TEXT,
    summary TEXT,
    categories TEXT,
    content_hash TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    raw_entry_json TEXT,
    FOREIGN KEY(first_feed_id) REFERENCES feeds(id) ON DELETE CASCADE,
    FOREIGN KEY(last_feed_id) REFERENCES feeds(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_feeds_active ON feeds(is_active);
CREATE INDEX IF NOT EXISTS idx_feeds_last_checked_at ON feeds(last_checked_at);
CREATE INDEX IF NOT EXISTS idx_feeds_active_checked_expr ON feeds(is_active, COALESCE(last_checked_at, ''), id);
CREATE INDEX IF NOT EXISTS idx_entries_last_seen_at ON entries(last_seen_at);
CREATE INDEX IF NOT EXISTS idx_entries_published_at ON entries(published_at);
CREATE INDEX IF NOT EXISTS idx_entries_event_ts_id
    ON entries(COALESCE(CASE WHEN published_at GLOB '????-??-??T*Z' THEN published_at END, first_seen_at, last_seen_at), id);
CREATE INDEX IF NOT EXISTS idx_entries_sort_pub_seen_id
    ON entries(COALESCE(CASE WHEN published_at GLOB '????-??-??T*Z' THEN published_at END, first_seen_at), id);
"""


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_space(value: str) -> str:
    return " ".join(value.split())


def sha256_hexdigest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonicalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return raw
    if not parts.scheme or not parts.netloc:
        return raw

    filtered_query = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower.startswith("utm_"):
            continue
        if key_lower in TRACKING_QUERY_PARAMS:
            continue
        filtered_query.append((key, value))

    query = urlencode(filtered_query, doseq=True)
    normalized = urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path or "/",
            query,
            "",
        )
    )
    return normalized


def parse_datetime_utc(raw: Any) -> datetime | None:
    if raw is None:
        return None
    if hasattr(raw, "tm_year"):
        try:
            dt = datetime(
                int(raw.tm_year),
                int(raw.tm_mon),
                int(raw.tm_mday),
                int(raw.tm_hour),
                int(raw.tm_min),
                int(raw.tm_sec),
                tzinfo=timezone.utc,
            )
            return dt
        except Exception:
            pass

    text = normalize_space(str(raw))
    if not text:
        return None

    iso_candidate = text
    if iso_candidate.endswith("Z"):
        iso_candidate = iso_candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        pass

    try:
        dt = parsedate_to_datetime(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def to_utc_iso(raw: Any) -> str | None:
    dt = parse_datetime_utc(raw)
    if dt is None:
        return None
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def require_feedparser() -> None:
    if feedparser is not None:
        return
    print(
        "RSS_META_ERR reason=missing_dependency install='python3 -m pip install feedparser'",
        file=sys.stderr,
    )
    raise SystemExit(2)


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


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def load_opml_urls(opml_path: str) -> list[str]:
    tree = ET.parse(opml_path)
    root = tree.getroot()
    urls: list[str] = []
    seen: set[str] = set()
    for outline in root.iter("outline"):
        xml_url = outline.attrib.get("xmlUrl") or outline.attrib.get("xmlurl")
        if not xml_url:
            continue
        normalized = canonicalize_url(xml_url)
        key = normalized or xml_url.strip()
        if not key or key in seen:
            continue
        seen.add(key)
        urls.append(key)
    return urls


def upsert_feed(conn: sqlite3.Connection, feed_url: str, feed_title: str | None = None) -> tuple[int, bool]:
    url_value = canonicalize_url(feed_url) or feed_url.strip()
    if not url_value:
        raise ValueError("empty_feed_url")

    existing = conn.execute("SELECT id FROM feeds WHERE feed_url = ?", (url_value,)).fetchone()
    now = now_utc_iso()

    if existing:
        if feed_title:
            conn.execute(
                "UPDATE feeds SET feed_title = ?, updated_at = ? WHERE id = ?",
                (feed_title, now, int(existing["id"])),
            )
        return int(existing["id"]), False

    cursor = conn.execute(
        """
        INSERT INTO feeds (
            feed_url, feed_title, created_at, updated_at
        ) VALUES (?, ?, ?, ?)
        """,
        (url_value, feed_title, now, now),
    )
    return int(cursor.lastrowid), True


def build_entry_record(feed_url: str, entry: Any) -> dict[str, Any]:
    guid = normalize_space(str(entry.get("id") or entry.get("guid") or ""))
    raw_url = normalize_space(str(entry.get("link") or ""))
    canonical_url = canonicalize_url(raw_url)
    title = normalize_space(str(entry.get("title") or ""))
    author = normalize_space(str(entry.get("author") or ""))
    summary = normalize_space(str(entry.get("summary") or entry.get("description") or ""))
    published_at = to_utc_iso(entry.get("published_parsed")) or to_utc_iso(entry.get("published"))
    updated_at = to_utc_iso(entry.get("updated_parsed")) or to_utc_iso(entry.get("updated"))

    category_terms: list[str] = []
    for tag in entry.get("tags") or []:
        if isinstance(tag, dict):
            term = normalize_space(str(tag.get("term") or ""))
        else:
            term = normalize_space(str(tag))
        if term:
            category_terms.append(term)
    categories = sorted(set(category_terms))

    legacy_guid_dedupe_key = None
    if guid:
        feed_scope = canonicalize_url(feed_url) or normalize_space(feed_url)
        dedupe_key = f"guid:{feed_scope}:{guid}"
        legacy_guid_dedupe_key = f"guid:{guid}"
    elif canonical_url:
        dedupe_key = f"url:{canonical_url}"
    else:
        fallback = "|".join([feed_url, title, published_at or "", summary[:200]])
        dedupe_key = f"hash:{sha256_hexdigest(fallback)}"

    content_basis = "|".join(
        [
            title,
            summary,
            published_at or "",
            updated_at or "",
            canonical_url or raw_url,
            ",".join(categories),
        ]
    )
    content_hash = sha256_hexdigest(content_basis)

    raw_entry_json = json.dumps(
        {
            "id": entry.get("id"),
            "title": entry.get("title"),
            "link": entry.get("link"),
            "published": entry.get("published"),
            "updated": entry.get("updated"),
            "author": entry.get("author"),
        },
        default=str,
        ensure_ascii=True,
        sort_keys=True,
    )

    return {
        "dedupe_key": dedupe_key,
        "legacy_guid_dedupe_key": legacy_guid_dedupe_key,
        "guid": guid or None,
        "url": raw_url or None,
        "canonical_url": canonical_url or None,
        "title": title or None,
        "author": author or None,
        "published_at": published_at or None,
        "updated_at": updated_at or None,
        "summary": summary or None,
        "categories": json.dumps(categories, ensure_ascii=True),
        "content_hash": content_hash,
        "raw_entry_json": raw_entry_json,
    }


def upsert_entry(conn: sqlite3.Connection, feed_id: int, feed_url: str, entry: Any, seen_at: str) -> str:
    record = build_entry_record(feed_url, entry)
    existing = conn.execute(
        "SELECT id, content_hash FROM entries WHERE dedupe_key = ?",
        (record["dedupe_key"],),
    ).fetchone()
    if not existing and record["legacy_guid_dedupe_key"]:
        existing = conn.execute(
            "SELECT id, content_hash FROM entries WHERE dedupe_key = ?",
            (record["legacy_guid_dedupe_key"],),
        ).fetchone()

    if not existing:
        conn.execute(
            """
            INSERT INTO entries (
                dedupe_key, first_feed_id, last_feed_id, guid, url, canonical_url,
                title, author, published_at, updated_at, summary, categories,
                content_hash, first_seen_at, last_seen_at, raw_entry_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["dedupe_key"],
                feed_id,
                feed_id,
                record["guid"],
                record["url"],
                record["canonical_url"],
                record["title"],
                record["author"],
                record["published_at"],
                record["updated_at"],
                record["summary"],
                record["categories"],
                record["content_hash"],
                seen_at,
                seen_at,
                record["raw_entry_json"],
            ),
        )
        return "new"

    entry_id = int(existing["id"])
    if existing["content_hash"] == record["content_hash"]:
        conn.execute(
            """
            UPDATE entries
            SET last_feed_id = ?, last_seen_at = ?
            WHERE id = ?
            """,
            (feed_id, seen_at, entry_id),
        )
        return "unchanged"

    conn.execute(
        """
        UPDATE entries
        SET last_feed_id = ?, guid = ?, url = ?, canonical_url = ?, title = ?,
            author = ?, published_at = ?, updated_at = ?, summary = ?, categories = ?,
            content_hash = ?, last_seen_at = ?, raw_entry_json = ?
        WHERE id = ?
        """,
        (
            feed_id,
            record["guid"],
            record["url"],
            record["canonical_url"],
            record["title"],
            record["author"],
            record["published_at"],
            record["updated_at"],
            record["summary"],
            record["categories"],
            record["content_hash"],
            seen_at,
            record["raw_entry_json"],
            entry_id,
        ),
    )
    return "updated"


def sync_feed(
    conn: sqlite3.Connection,
    feed_row: sqlite3.Row,
    max_items_per_feed: int,
    use_conditional_get: bool,
    user_agent: str,
) -> dict[str, int]:
    require_feedparser()
    feed_url = str(feed_row["feed_url"])
    etag = str(feed_row["etag"] or "") if use_conditional_get else ""
    last_modified = str(feed_row["last_modified"] or "") if use_conditional_get else ""

    parsed = feedparser.parse(
        feed_url,
        etag=etag or None,
        modified=last_modified or None,
        agent=user_agent,
    )

    now = now_utc_iso()
    status = int(parsed.get("status") or 200)
    headers = parsed.get("headers") or {}
    etag_new = parsed.get("etag") or headers.get("etag") or etag or None
    modified_new = headers.get("last-modified") or last_modified or None

    result = {
        "feeds_checked": 1,
        "feeds_nochange": 0,
        "new": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
    }

    if status == 304:
        conn.execute(
            """
            UPDATE feeds
            SET etag = ?, last_modified = ?, last_checked_at = ?, last_status = ?, updated_at = ?
            WHERE id = ?
            """,
            (etag_new, modified_new, now, status, now, int(feed_row["id"])),
        )
        result["feeds_nochange"] = 1
        return result

    bozo = bool(parsed.get("bozo"))
    if bozo and not parsed.entries:
        error_text = str(parsed.get("bozo_exception") or "bozo_parse_error")
        conn.execute(
            """
            UPDATE feeds
            SET last_checked_at = ?, last_status = ?, last_error = ?, updated_at = ?
            WHERE id = ?
            """,
            (now, status, error_text, now, int(feed_row["id"])),
        )
        result["errors"] = 1
        return result

    entry_count = 0
    for entry in parsed.entries:
        if max_items_per_feed > 0 and entry_count >= max_items_per_feed:
            break
        state = upsert_entry(conn, int(feed_row["id"]), feed_url, entry, now)
        result[state] += 1
        entry_count += 1

    feed_title = normalize_space(str(parsed.feed.get("title") or "")) or None
    site_url = canonicalize_url(str(parsed.feed.get("link") or "")) or None
    conn.execute(
        """
        UPDATE feeds
        SET feed_title = COALESCE(?, feed_title),
            site_url = COALESCE(?, site_url),
            etag = ?,
            last_modified = ?,
            last_checked_at = ?,
            last_success_at = ?,
            last_status = ?,
            last_error = NULL,
            updated_at = ?
        WHERE id = ?
        """,
        (
            feed_title,
            site_url,
            etag_new,
            modified_new,
            now,
            now,
            status,
            now,
            int(feed_row["id"]),
        ),
    )
    return result


def cleanup_stale_entries(conn: sqlite3.Connection, ttl_days: int) -> int:
    if ttl_days <= 0:
        return 0
    threshold = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    threshold_iso = threshold.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    cursor = conn.execute("DELETE FROM entries WHERE last_seen_at < ?", (threshold_iso,))
    return int(cursor.rowcount)


def cmd_init_db(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
    print(f"DB_OK path={args.db}")
    return 0


def cmd_add_feed(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        _, created = upsert_feed(conn, args.url, args.title)
        conn.commit()
    print(f"ADD_OK url={canonicalize_url(args.url) or args.url} created={1 if created else 0}")
    return 0


def cmd_import_opml(args: argparse.Namespace) -> int:
    urls = load_opml_urls(args.opml)
    added = 0
    existing = 0
    with connect_db(args.db) as conn:
        init_db(conn)
        for url in urls:
            _, created = upsert_feed(conn, url)
            if created:
                added += 1
            else:
                existing += 1
        conn.commit()
    print(f"IMPORT_OK total={len(urls)} added={added} existing={existing}")
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    require_feedparser()
    totals = {
        "feeds_checked": 0,
        "feeds_nochange": 0,
        "new": 0,
        "updated": 0,
        "unchanged": 0,
        "errors": 0,
        "cleanup_deleted": 0,
    }

    with connect_db(args.db) as conn:
        init_db(conn)

        if args.feed_url:
            target_url = canonicalize_url(args.feed_url) or args.feed_url
            feed_rows = conn.execute(
                "SELECT * FROM feeds WHERE feed_url = ? AND is_active = 1",
                (target_url,),
            ).fetchall()
        else:
            sql = "SELECT * FROM feeds WHERE is_active = 1 ORDER BY COALESCE(last_checked_at, '') ASC, id ASC"
            params: tuple[Any, ...]
            if args.max_feeds > 0:
                sql += " LIMIT ?"
                params = (args.max_feeds,)
            else:
                params = ()
            feed_rows = conn.execute(sql, params).fetchall()

        if not feed_rows:
            print("SYNC_OK feeds_checked=0 feeds_nochange=0 new=0 updated=0 unchanged=0 errors=0 cleanup_deleted=0")
            return 0

        for row in feed_rows:
            row_result = sync_feed(
                conn=conn,
                feed_row=row,
                max_items_per_feed=args.max_items_per_feed,
                use_conditional_get=not args.disable_conditional_get,
                user_agent=args.user_agent,
            )
            for key in ("feeds_checked", "feeds_nochange", "new", "updated", "unchanged", "errors"):
                totals[key] += int(row_result.get(key, 0))
            conn.commit()

        if args.cleanup_ttl_days > 0:
            totals["cleanup_deleted"] = cleanup_stale_entries(conn, args.cleanup_ttl_days)

        conn.commit()

    print(
        "SYNC_OK "
        f"feeds_checked={totals['feeds_checked']} "
        f"feeds_nochange={totals['feeds_nochange']} "
        f"new={totals['new']} "
        f"updated={totals['updated']} "
        f"unchanged={totals['unchanged']} "
        f"errors={totals['errors']} "
        f"cleanup_deleted={totals['cleanup_deleted']}"
    )
    return 0


def cmd_list_feeds(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT id, feed_url, feed_title, last_checked_at, last_status, is_active
            FROM feeds
            ORDER BY id ASC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()

    print("id\tactive\tstatus\tlast_checked_at\tfeed_title\tfeed_url")
    for row in rows:
        print(
            f"{row['id']}\t{row['is_active']}\t{row['last_status'] or ''}\t"
            f"{row['last_checked_at'] or ''}\t{row['feed_title'] or ''}\t{row['feed_url']}"
        )
    return 0


def cmd_list_entries(args: argparse.Namespace) -> int:
    with connect_db(args.db) as conn:
        init_db(conn)
        rows = conn.execute(
            """
            SELECT e.id, e.dedupe_key, e.title, e.canonical_url, e.published_at, e.last_seen_at, f.feed_url
            FROM entries e
            JOIN feeds f ON f.id = e.last_feed_id
            ORDER BY COALESCE(CASE WHEN e.published_at GLOB '????-??-??T*Z' THEN e.published_at END, e.first_seen_at) DESC, e.id DESC
            LIMIT ?
            """,
            (args.limit,),
        ).fetchall()

    print("id\tpublished_at\tlast_seen_at\ttitle\turl\tdedupe_key\tfeed_url")
    for row in rows:
        print(
            f"{row['id']}\t{row['published_at'] or ''}\t{row['last_seen_at'] or ''}\t"
            f"{(row['title'] or '').replace(chr(9), ' ')}\t{row['canonical_url'] or ''}\t"
            f"{row['dedupe_key']}\t{row['feed_url']}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Subscribe RSS feeds and store metadata in SQLite.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    parser_init = subparsers.add_parser("init-db", help="Initialize SQLite schema.")
    parser_init.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_init.set_defaults(func=cmd_init_db)

    parser_add = subparsers.add_parser("add-feed", help="Add one feed URL.")
    parser_add.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_add.add_argument("--url", required=True, help="Feed URL.")
    parser_add.add_argument("--title", default=None, help="Optional feed title override.")
    parser_add.set_defaults(func=cmd_add_feed)

    parser_import = subparsers.add_parser("import-opml", help="Import feed URLs from OPML.")
    parser_import.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_import.add_argument("--opml", required=True, help="OPML file path.")
    parser_import.set_defaults(func=cmd_import_opml)

    parser_sync = subparsers.add_parser("sync", help="Fetch active feeds and persist entry metadata.")
    parser_sync.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_sync.add_argument("--feed-url", default=None, help="Sync a single feed URL.")
    parser_sync.add_argument("--max-feeds", type=int, default=0, help="Max feeds per run, 0 means no limit.")
    parser_sync.add_argument(
        "--max-items-per-feed",
        type=int,
        default=100,
        help="Max entries read from each feed response.",
    )
    parser_sync.add_argument(
        "--disable-conditional-get",
        action="store_true",
        help="Disable etag/last-modified conditional requests.",
    )
    parser_sync.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help=f"HTTP User-Agent (default: {DEFAULT_USER_AGENT})",
    )
    parser_sync.add_argument(
        "--cleanup-ttl-days",
        type=int,
        default=0,
        help="Delete entries not seen for this many days. 0 disables cleanup.",
    )
    parser_sync.set_defaults(func=cmd_sync)

    parser_list_feeds = subparsers.add_parser("list-feeds", help="List subscribed feeds.")
    parser_list_feeds.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_list_feeds.add_argument("--limit", type=int, default=50, help="Max rows to print.")
    parser_list_feeds.set_defaults(func=cmd_list_feeds)

    parser_list_entries = subparsers.add_parser("list-entries", help="List persisted entries.")
    parser_list_entries.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_list_entries.add_argument("--limit", type=int, default=100, help="Max rows to print.")
    parser_list_entries.set_defaults(func=cmd_list_entries)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except sqlite3.Error as exc:
        print(f"RSS_META_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except FileNotFoundError as exc:
        print(f"RSS_META_ERR reason=file_not_found detail={exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"RSS_META_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
