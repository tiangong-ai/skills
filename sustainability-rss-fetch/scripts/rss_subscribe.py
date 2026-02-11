#!/usr/bin/env python3
"""Subscribe to sustainability RSS feeds and persist metadata into SQLite."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
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


DEFAULT_DB_FILENAME = "sustainability_rss.db"
DEFAULT_DB_PATH = os.environ.get("SUSTAIN_RSS_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_USER_AGENT = "sustainability-rss-fetch/1.0 (+https://github.com/tiangong-ai/skills)"
DEFAULT_TOPIC_PROMPT = (
    "筛选与可持续主题相关的文章：生命周期评价(LCA)、物质流分析(MFA)、绿色供应链、绿电、"
    "绿色设计、减污降碳。可根据用户自定义主题扩展。"
)
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


def dt_to_utc_iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_window_boundary(raw: str | None, is_end: bool) -> datetime | None:
    text = normalize_space(str(raw or ""))
    if not text:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        dt = datetime.strptime(text, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt + timedelta(days=1) if is_end else dt
    parsed = parse_datetime_utc(text)
    if parsed is None:
        raise ValueError(f"invalid_window_boundary:{text}")
    return parsed


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
    urls: list[str] = []
    seen: set[str] = set()

    try:
        tree = ET.parse(opml_path)
        root = tree.getroot()
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
    except ET.ParseError:
        text = Path(opml_path).read_text(encoding="utf-8", errors="replace")
        for match in re.finditer(r'xmlUrl\s*=\s*["\']([^"\']+)["\']', text, flags=re.IGNORECASE):
            xml_url = normalize_space(match.group(1))
            if not xml_url:
                continue
            normalized = canonicalize_url(xml_url)
            key = normalized or xml_url
            if key in seen:
                continue
            seen.add(key)
            urls.append(key)
        if urls:
            return urls
        raise


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


def upsert_entry_record(conn: sqlite3.Connection, feed_id: int, record: dict[str, Any], seen_at: str) -> str:
    categories_value = record.get("categories")
    if isinstance(categories_value, list):
        record = dict(record)
        record["categories"] = json.dumps(categories_value, ensure_ascii=True)

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


def upsert_entry(conn: sqlite3.Connection, feed_id: int, feed_url: str, entry: Any, seen_at: str) -> str:
    record = build_entry_record(feed_url, entry)
    return upsert_entry_record(conn, feed_id, record, seen_at)


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


def load_active_feed_urls(db_path: str) -> list[str]:
    with connect_db(db_path) as conn:
        init_db(conn)
        rows = conn.execute(
            "SELECT feed_url FROM feeds WHERE is_active = 1 ORDER BY id ASC",
        ).fetchall()
    urls: list[str] = []
    for row in rows:
        feed_url = normalize_space(str(row["feed_url"] or ""))
        if feed_url:
            urls.append(feed_url)
    return urls


def parse_categories(categories_raw: Any) -> list[str]:
    if isinstance(categories_raw, list):
        return [normalize_space(str(item)) for item in categories_raw if normalize_space(str(item))]
    text = normalize_space(str(categories_raw or ""))
    if not text:
        return []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [normalize_space(str(item)) for item in parsed if normalize_space(str(item))]
    except json.JSONDecodeError:
        return [text]
    return []


def collect_candidates_from_feed(
    feed_url: str,
    max_items_per_feed: int,
    user_agent: str,
    start_dt: datetime | None,
    end_dt: datetime | None,
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    require_feedparser()
    parsed = feedparser.parse(feed_url, agent=user_agent)
    status = int(parsed.get("status") or 200)
    feed_title = normalize_space(str(parsed.feed.get("title") or "")) or None
    site_url = canonicalize_url(str(parsed.feed.get("link") or "")) or None

    feed_meta = {
        "feed_url": feed_url,
        "feed_title": feed_title,
        "site_url": site_url,
        "status": status,
        "error": None,
    }

    bozo = bool(parsed.get("bozo"))
    if bozo and not parsed.entries:
        feed_meta["error"] = str(parsed.get("bozo_exception") or "bozo_parse_error")
        return feed_meta, [], 0

    collected: list[dict[str, Any]] = []
    skipped_by_window = 0
    for idx, entry in enumerate(parsed.entries):
        if max_items_per_feed > 0 and idx >= max_items_per_feed:
            break
        record = build_entry_record(feed_url, entry)
        event_dt = parse_datetime_utc(record.get("published_at")) or parse_datetime_utc(record.get("updated_at"))

        if start_dt or end_dt:
            if event_dt is None:
                skipped_by_window += 1
                continue
            if start_dt and event_dt < start_dt:
                skipped_by_window += 1
                continue
            if end_dt and event_dt >= end_dt:
                skipped_by_window += 1
                continue

        collected.append(
            {
                "feed_url": feed_url,
                "feed_title": feed_title,
                "site_url": site_url,
                "title": record.get("title"),
                "url": record.get("url"),
                "canonical_url": record.get("canonical_url"),
                "author": record.get("author"),
                "published_at": record.get("published_at"),
                "updated_at": record.get("updated_at"),
                "summary": record.get("summary"),
                "categories": parse_categories(record.get("categories")),
                "dedupe_key": record.get("dedupe_key"),
                "entry_record": record,
            }
        )
    return feed_meta, collected, skipped_by_window


def cmd_collect_window(args: argparse.Namespace) -> int:
    require_feedparser()
    start_dt = parse_window_boundary(args.start, is_end=False)
    end_dt = parse_window_boundary(args.end, is_end=True)
    if start_dt and end_dt and end_dt <= start_dt:
        raise ValueError("invalid_window_range:end_must_be_larger_than_start")

    raw_urls: list[str] = []
    for feed_url in args.feed_url or []:
        normalized = canonicalize_url(feed_url) or normalize_space(feed_url)
        if normalized:
            raw_urls.append(normalized)
    if args.opml:
        raw_urls.extend(load_opml_urls(args.opml))
    if args.use_subscribed_feeds:
        raw_urls.extend(load_active_feed_urls(args.db))

    seen: set[str] = set()
    feed_urls: list[str] = []
    for raw in raw_urls:
        normalized = canonicalize_url(raw) or normalize_space(raw)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        feed_urls.append(normalized)

    if args.max_feeds > 0:
        feed_urls = feed_urls[: args.max_feeds]

    if not feed_urls:
        raise ValueError("no_feed_sources: provide --feed-url, --opml, or --use-subscribed-feeds")

    payload_candidates: list[dict[str, Any]] = []
    feed_reports: list[dict[str, Any]] = []
    skipped_by_window_total = 0
    next_candidate_id = 1
    for feed_url in feed_urls:
        feed_meta, collected, skipped_by_window = collect_candidates_from_feed(
            feed_url=feed_url,
            max_items_per_feed=args.max_items_per_feed,
            user_agent=args.user_agent,
            start_dt=start_dt,
            end_dt=end_dt,
        )
        skipped_by_window_total += skipped_by_window
        for candidate in collected:
            candidate["candidate_id"] = next_candidate_id
            payload_candidates.append(candidate)
            next_candidate_id += 1

        feed_report = dict(feed_meta)
        feed_report["candidate_count"] = len(collected)
        feed_report["skipped_by_window"] = skipped_by_window
        feed_reports.append(feed_report)

    payload = {
        "generated_at_utc": now_utc_iso(),
        "topic_prompt": normalize_space(str(args.topic_prompt or DEFAULT_TOPIC_PROMPT)),
        "window": {
            "start_utc": dt_to_utc_iso(start_dt),
            "end_utc": dt_to_utc_iso(end_dt),
        },
        "source": {
            "opml_path": args.opml,
            "db_path": str(resolve_db_path(args.db)) if args.use_subscribed_feeds else None,
            "feed_urls": feed_urls,
        },
        "feeds": feed_reports,
        "candidates": payload_candidates,
    }

    output_path = Path(args.output)
    if output_path.parent and str(output_path.parent) not in ("", "."):
        output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        if args.pretty:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
        else:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
    print(
        "COLLECT_OK "
        f"feeds={len(feed_urls)} "
        f"candidates={len(payload_candidates)} "
        f"skipped_by_window={skipped_by_window_total} "
        f"output={output_path}"
    )
    return 0


def parse_selected_ids(raw: str) -> set[int]:
    selected: set[int] = set()
    for token in (raw or "").split(","):
        value = normalize_space(token)
        if not value:
            continue
        selected.add(int(value))
    return selected


def cmd_insert_selected(args: argparse.Namespace) -> int:
    payload_path = Path(args.candidates)
    with payload_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    candidates_raw = payload.get("candidates")
    if not isinstance(candidates_raw, list):
        raise ValueError("invalid_candidates_payload: missing candidates array")

    candidate_ids = {
        int(item.get("candidate_id"))
        for item in candidates_raw
        if isinstance(item, dict) and str(item.get("candidate_id") or "").isdigit()
    }

    selected_ids: set[int] = set()
    if args.selected_ids:
        selected_ids |= parse_selected_ids(args.selected_ids)
    if args.selected_ids_file:
        file_text = Path(args.selected_ids_file).read_text(encoding="utf-8")
        selected_ids |= parse_selected_ids(file_text.replace("\n", ","))
    if args.select_all:
        selected_ids = set(candidate_ids)
    if not selected_ids:
        raise ValueError("no_selected_ids: provide --selected-ids, --selected-ids-file, or --select-all")

    unknown_ids = sorted(selected_ids - candidate_ids)
    selected_ids = selected_ids & candidate_ids
    if not selected_ids:
        raise ValueError("no_valid_selected_ids")

    totals = {
        "selected": len(selected_ids),
        "new": 0,
        "updated": 0,
        "unchanged": 0,
        "skipped_invalid": 0,
        "unknown_ids": len(unknown_ids),
    }
    seen_at = now_utc_iso()
    with connect_db(args.db) as conn:
        init_db(conn)
        for item in candidates_raw:
            if not isinstance(item, dict):
                continue
            raw_candidate_id = item.get("candidate_id")
            if not str(raw_candidate_id or "").isdigit():
                totals["skipped_invalid"] += 1
                continue
            candidate_id = int(raw_candidate_id)
            if candidate_id not in selected_ids:
                continue

            feed_url = canonicalize_url(str(item.get("feed_url") or ""))
            if not feed_url:
                totals["skipped_invalid"] += 1
                continue
            feed_title = normalize_space(str(item.get("feed_title") or "")) or None
            site_url = canonicalize_url(str(item.get("site_url") or "")) or None
            feed_id, _ = upsert_feed(conn, feed_url, feed_title)
            if site_url:
                conn.execute(
                    "UPDATE feeds SET site_url = COALESCE(?, site_url), updated_at = ? WHERE id = ?",
                    (site_url, seen_at, feed_id),
                )

            entry_record = item.get("entry_record")
            if not isinstance(entry_record, dict):
                totals["skipped_invalid"] += 1
                continue
            state = upsert_entry_record(conn, feed_id, entry_record, seen_at)
            totals[state] += 1

        conn.commit()

    print(
        "INSERT_SELECTED_OK "
        f"selected={totals['selected']} "
        f"new={totals['new']} "
        f"updated={totals['updated']} "
        f"unchanged={totals['unchanged']} "
        f"skipped_invalid={totals['skipped_invalid']} "
        f"unknown_ids={totals['unknown_ids']}"
    )
    return 0


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
    parser = argparse.ArgumentParser(description="Subscribe sustainability RSS feeds and store metadata in SQLite.")
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

    parser_collect = subparsers.add_parser(
        "collect-window",
        help="Fetch candidate entries into a JSON window for prompt-based topic filtering before DB insertion.",
    )
    parser_collect.add_argument(
        "--db",
        default=DEFAULT_DB_PATH,
        help=f"SQLite db path used when --use-subscribed-feeds is enabled (default: {DEFAULT_DB_PATH})",
    )
    parser_collect.add_argument("--opml", default=None, help="Optional OPML file path as feed source.")
    parser_collect.add_argument(
        "--feed-url",
        action="append",
        default=[],
        help="Feed URL source. Repeat the flag for multiple URLs.",
    )
    parser_collect.add_argument(
        "--use-subscribed-feeds",
        action="store_true",
        help="Use active feed URLs already stored in the SQLite feeds table.",
    )
    parser_collect.add_argument("--max-feeds", type=int, default=0, help="Max feed sources in this collection run.")
    parser_collect.add_argument(
        "--max-items-per-feed",
        type=int,
        default=100,
        help="Max entries read from each feed response.",
    )
    parser_collect.add_argument(
        "--start",
        default=None,
        help="Window start, supports YYYY-MM-DD or ISO datetime.",
    )
    parser_collect.add_argument(
        "--end",
        default=None,
        help="Window end, supports YYYY-MM-DD or ISO datetime.",
    )
    parser_collect.add_argument(
        "--topic-prompt",
        default=DEFAULT_TOPIC_PROMPT,
        help="Prompt text describing sustainability filtering intent for downstream confirmation.",
    )
    parser_collect.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help=f"HTTP User-Agent (default: {DEFAULT_USER_AGENT})",
    )
    parser_collect.add_argument("--output", required=True, help="Output JSON path for candidate window.")
    parser_collect.add_argument("--pretty", action="store_true", help="Write pretty-printed JSON.")
    parser_collect.set_defaults(func=cmd_collect_window)

    parser_insert_selected = subparsers.add_parser(
        "insert-selected",
        help="Insert only confirmed candidate IDs from collect-window JSON into SQLite.",
    )
    parser_insert_selected.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser_insert_selected.add_argument("--candidates", required=True, help="JSON file generated by collect-window.")
    parser_insert_selected.add_argument(
        "--selected-ids",
        default="",
        help="Comma-separated candidate IDs confirmed for insertion.",
    )
    parser_insert_selected.add_argument(
        "--selected-ids-file",
        default=None,
        help="Text file containing candidate IDs (comma or newline separated).",
    )
    parser_insert_selected.add_argument(
        "--select-all",
        action="store_true",
        help="Insert all candidates from the window JSON.",
    )
    parser_insert_selected.set_defaults(func=cmd_insert_selected)

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
