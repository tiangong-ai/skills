#!/usr/bin/env python3
"""Retrieve time-windowed RSS evidence for agent-side RAG summarization."""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

DEFAULT_DB_FILENAME = "rss_metadata.db"
DEFAULT_DB_PATH = os.environ.get("RSS_DB_PATH", DEFAULT_DB_FILENAME)
DEFAULT_MAX_RECORDS = 80
DEFAULT_MAX_PER_FEED = 0
DEFAULT_FULLTEXT_CHARS = 8192
DEFAULT_SUMMARY_CHARS = 8192
DEFAULT_TOP_FEEDS = 20
DEFAULT_TOP_KEYWORDS = 25

DEFAULT_FIELDS = [
    "entry_id",
    "timestamp_utc",
    "timestamp_source",
    "feed_title",
    "feed_url",
    "title",
    "url",
    "summary",
    "fulltext_status",
    "fulltext_length",
    "fulltext_excerpt",
]

ALL_FIELDS = {
    "entry_id",
    "dedupe_key",
    "timestamp_utc",
    "timestamp_source",
    "published_at",
    "first_seen_at",
    "last_seen_at",
    "feed_title",
    "feed_url",
    "title",
    "url",
    "summary",
    "categories",
    "fulltext_status",
    "fulltext_length",
    "fulltext_excerpt",
}

EVENT_TS_SQL_EXPR = (
    "COALESCE("
    "CASE WHEN e.published_at GLOB '????-??-??T*Z' THEN e.published_at END, "
    "e.first_seen_at, "
    "e.last_seen_at"
    ")"
)

STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "among",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "been",
    "before",
    "between",
    "but",
    "by",
    "can",
    "do",
    "for",
    "from",
    "get",
    "had",
    "has",
    "have",
    "how",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "just",
    "more",
    "most",
    "new",
    "no",
    "not",
    "now",
    "of",
    "on",
    "one",
    "or",
    "our",
    "out",
    "over",
    "s",
    "should",
    "so",
    "some",
    "such",
    "than",
    "that",
    "the",
    "their",
    "them",
    "there",
    "these",
    "they",
    "this",
    "those",
    "to",
    "too",
    "under",
    "up",
    "use",
    "using",
    "was",
    "we",
    "were",
    "what",
    "when",
    "which",
    "who",
    "will",
    "with",
    "you",
    "your",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_utc_iso() -> str:
    return now_utc().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_space(value: str) -> str:
    return " ".join(value.split())


def resolve_db_path(db_path: str) -> Path:
    raw = str(db_path or "").strip()
    if not raw:
        raw = DEFAULT_DB_PATH

    env_override = str(os.environ.get("RSS_DB_PATH") or "").strip()
    # Keep backward compatibility: legacy "--db rss_metadata.db" still honors env override.
    if env_override and raw == DEFAULT_DB_FILENAME:
        raw = env_override

    return Path(raw).expanduser()


def truncate_text(value: str, max_chars: int) -> str:
    clean = normalize_space(value)
    if max_chars <= 0:
        return ""
    if max_chars <= 3:
        return clean[:max_chars]
    if len(clean) <= max_chars:
        return clean
    return clean[: max_chars - 3] + "..."


def parse_datetime_utc(raw: Any) -> datetime | None:
    if raw is None:
        return None
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


def parse_anchor_date(raw: str | None) -> date:
    if not raw:
        return now_utc().date()
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid_date:{raw}") from exc


def parse_custom_boundary(raw: str, is_end: bool) -> datetime:
    text = normalize_space(raw)
    if not text:
        raise ValueError("empty_custom_boundary")

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
        d = datetime.strptime(text, "%Y-%m-%d").date()
        base = datetime.combine(d, time.min, tzinfo=timezone.utc)
        return base + timedelta(days=1) if is_end else base

    dt = parse_datetime_utc(text)
    if dt is None:
        raise ValueError(f"invalid_datetime:{text}")
    return dt


def determine_range(
    period: str,
    anchor_date: str | None,
    custom_start: str | None,
    custom_end: str | None,
) -> tuple[datetime, datetime, str]:
    if period == "custom":
        if not custom_start or not custom_end:
            raise ValueError("custom_requires_start_end")
        start = parse_custom_boundary(custom_start, is_end=False)
        end = parse_custom_boundary(custom_end, is_end=True)
        if end <= start:
            raise ValueError("invalid_custom_range")
        return start, end, "Custom"

    anchor = parse_anchor_date(anchor_date)
    if period == "daily":
        start = datetime.combine(anchor, time.min, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        return start, end, "Daily"

    if period == "weekly":
        week_start_date = anchor - timedelta(days=anchor.weekday())
        start = datetime.combine(week_start_date, time.min, tzinfo=timezone.utc)
        end = start + timedelta(days=7)
        return start, end, "Weekly"

    if period == "monthly":
        month_start = anchor.replace(day=1)
        start = datetime.combine(month_start, time.min, tzinfo=timezone.utc)
        if month_start.month == 12:
            next_month = month_start.replace(year=month_start.year + 1, month=1, day=1)
        else:
            next_month = month_start.replace(month=month_start.month + 1, day=1)
        end = datetime.combine(next_month, time.min, tzinfo=timezone.utc)
        return start, end, "Monthly"

    raise ValueError(f"unsupported_period:{period}")


def parse_fields(raw_fields: str | None) -> list[str]:
    if not raw_fields:
        return list(DEFAULT_FIELDS)
    fields = [normalize_space(item) for item in raw_fields.split(",")]
    fields = [item for item in fields if item]
    if not fields:
        return list(DEFAULT_FIELDS)
    invalid = [item for item in fields if item not in ALL_FIELDS]
    if invalid:
        allowed = ",".join(sorted(ALL_FIELDS))
        raise ValueError(f"invalid_fields:{','.join(invalid)} allowed={allowed}")
    return fields


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


def ensure_required_tables(conn: sqlite3.Connection) -> bool:
    return table_exists(conn, "feeds") and table_exists(conn, "entries")


def choose_entry_timestamp(row: sqlite3.Row) -> tuple[datetime | None, str]:
    published = parse_datetime_utc(row["published_at"])
    if published:
        return published, "published_at"
    first_seen = parse_datetime_utc(row["first_seen_at"])
    if first_seen:
        return first_seen, "first_seen_at"
    last_seen = parse_datetime_utc(row["last_seen_at"])
    if last_seen:
        return last_seen, "last_seen_at"
    return None, "none"


def tokenize(text: str) -> list[str]:
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9+#-]{2,}", text.lower())
    cleaned: list[str] = []
    for token in tokens:
        if token in STOPWORDS:
            continue
        cleaned.append(token)
    return cleaned


def count_rows_in_range(conn: sqlite3.Connection, start_utc_iso: str, end_utc_iso: str) -> int:
    row = conn.execute(
        f"""
        SELECT COUNT(1) AS c
        FROM entries e
        WHERE {EVENT_TS_SQL_EXPR} >= ?
          AND {EVENT_TS_SQL_EXPR} < ?
        """,
        (start_utc_iso, end_utc_iso),
    ).fetchone()
    if not row:
        return 0
    return int(row["c"] or 0)


def load_rows(
    conn: sqlite3.Connection,
    start_utc_iso: str,
    end_utc_iso: str,
    limit: int,
) -> tuple[list[sqlite3.Row], bool]:
    has_entry_content = table_exists(conn, "entry_content")

    content_columns = (
        "COALESCE(ec.status, 'none') AS fulltext_status, "
        "COALESCE(ec.content_length, 0) AS fulltext_length, "
        "COALESCE(ec.content_text, '') AS fulltext_text"
        if has_entry_content
        else (
            "'none' AS fulltext_status, "
            "0 AS fulltext_length, "
            "'' AS fulltext_text"
        )
    )
    join_clause = "LEFT JOIN entry_content ec ON ec.entry_id = e.id" if has_entry_content else ""

    sql = f"""
    SELECT
        e.id AS entry_id,
        e.dedupe_key,
        e.title,
        e.canonical_url,
        e.url,
        e.summary,
        e.categories,
        e.published_at,
        e.first_seen_at,
        e.last_seen_at,
        f.feed_title,
        f.feed_url,
        {content_columns}
    FROM entries e
    LEFT JOIN feeds f ON f.id = e.last_feed_id
    {join_clause}
    WHERE {EVENT_TS_SQL_EXPR} >= ?
      AND {EVENT_TS_SQL_EXPR} < ?
    ORDER BY {EVENT_TS_SQL_EXPR} DESC, e.id DESC
    """
    params: list[Any] = [start_utc_iso, end_utc_iso]
    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    rows = conn.execute(sql, tuple(params)).fetchall()
    return rows, has_entry_content


def build_record(row: sqlite3.Row, summary_chars: int, fulltext_chars: int) -> dict[str, Any] | None:
    timestamp, timestamp_source = choose_entry_timestamp(row)
    if timestamp is None:
        return None

    feed_title = normalize_space(str(row["feed_title"] or ""))
    feed_url = normalize_space(str(row["feed_url"] or ""))
    title = normalize_space(str(row["title"] or "")) or "(untitled)"
    url = normalize_space(str(row["canonical_url"] or "")) or normalize_space(str(row["url"] or ""))
    summary = truncate_text(str(row["summary"] or ""), summary_chars)
    fulltext_text = str(row["fulltext_text"] or "")
    fulltext_excerpt = truncate_text(fulltext_text, fulltext_chars)

    categories: list[str] = []
    raw_categories = row["categories"]
    if raw_categories:
        try:
            parsed = json.loads(str(raw_categories))
            if isinstance(parsed, list):
                categories = [normalize_space(str(item)) for item in parsed if normalize_space(str(item))]
        except json.JSONDecodeError:
            categories = []

    return {
        "entry_id": int(row["entry_id"]),
        "dedupe_key": str(row["dedupe_key"] or ""),
        "timestamp_utc": timestamp.isoformat().replace("+00:00", "Z"),
        "timestamp_source": timestamp_source,
        "published_at": normalize_space(str(row["published_at"] or "")),
        "first_seen_at": normalize_space(str(row["first_seen_at"] or "")),
        "last_seen_at": normalize_space(str(row["last_seen_at"] or "")),
        "feed_title": feed_title,
        "feed_url": feed_url,
        "title": title,
        "url": url,
        "summary": summary,
        "categories": categories,
        "fulltext_status": normalize_space(str(row["fulltext_status"] or "none")) or "none",
        "fulltext_length": int(row["fulltext_length"] or 0),
        "fulltext_excerpt": fulltext_excerpt,
    }


def filter_and_rank_records(
    records: list[dict[str, Any]],
    max_records: int,
    max_per_feed: int,
) -> list[dict[str, Any]]:
    in_range_records = list(records)
    in_range_records.sort(key=lambda item: item["timestamp_utc"], reverse=True)

    if max_per_feed > 0:
        limited: list[dict[str, Any]] = []
        feed_counter: dict[str, int] = defaultdict(int)
        for record in in_range_records:
            feed_key = record.get("feed_title") or record.get("feed_url") or "(unknown-feed)"
            if feed_counter[feed_key] >= max_per_feed:
                continue
            feed_counter[feed_key] += 1
            limited.append(record)
        in_range_records = limited

    if max_records > 0:
        in_range_records = in_range_records[:max_records]
    return in_range_records


def compute_aggregates(records: list[dict[str, Any]], top_feeds: int, top_keywords: int) -> dict[str, Any]:
    feed_counter = Counter()
    fulltext_status_counter = Counter()
    keyword_counter = Counter()

    for record in records:
        feed_key = record.get("feed_title") or record.get("feed_url") or "(unknown-feed)"
        feed_counter[str(feed_key)] += 1
        fulltext_status_counter[str(record.get("fulltext_status") or "none")] += 1

        source_text = " ".join(
            [
                str(record.get("title") or ""),
                str(record.get("summary") or ""),
                str(record.get("fulltext_excerpt") or ""),
            ]
        )
        keyword_counter.update(tokenize(source_text))

    top_feeds_rows = [
        {"feed": feed, "count": count}
        for feed, count in feed_counter.most_common(max(top_feeds, 1))
    ]
    top_keywords_rows = [
        {"keyword": keyword, "count": count}
        for keyword, count in keyword_counter.most_common(max(top_keywords, 1))
    ]

    return {
        "feed_counts_top": top_feeds_rows,
        "fulltext_status_counts": dict(fulltext_status_counter),
        "top_keywords": top_keywords_rows,
    }


def select_record_fields(records: list[dict[str, Any]], fields: list[str]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for record in records:
        selected.append({field: record.get(field) for field in fields})
    return selected


def build_output_payload(
    period: str,
    period_label: str,
    start: datetime,
    end: datetime,
    selected_fields: list[str],
    records_all_in_range: int,
    records_returned: list[dict[str, Any]],
    has_entry_content: bool,
    max_records: int,
    max_per_feed: int,
    top_feeds: int,
    top_keywords: int,
) -> dict[str, Any]:
    aggregates = compute_aggregates(records_returned, top_feeds=top_feeds, top_keywords=top_keywords)
    return {
        "query": {
            "period": period,
            "period_label": period_label,
            "start_utc": start.isoformat().replace("+00:00", "Z"),
            "end_utc": end.isoformat().replace("+00:00", "Z"),
            "selected_fields": selected_fields,
            "max_records": max_records,
            "max_per_feed": max_per_feed,
        },
        "dataset": {
            "generated_at_utc": now_utc_iso(),
            "has_entry_content": has_entry_content,
            "records_in_range_before_limit": records_all_in_range,
            "records_returned": len(records_returned),
            "truncated": len(records_returned) < records_all_in_range,
        },
        "aggregates": aggregates,
        "records": records_returned,
    }


def emit_output(payload: dict[str, Any], output_path: str | None, pretty: bool) -> None:
    if pretty:
        body = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    else:
        # Minified JSON for lower token cost in downstream agent prompts.
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=False)
    if output_path:
        target = Path(output_path)
        if target.parent and str(target.parent) not in ("", "."):
            target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body + "\n")
        print(f"SUMMARY_RAG_OK output={target} records={payload['dataset']['records_returned']}")
        return
    print(body)
    print(
        f"SUMMARY_RAG_OK output=stdout records={payload['dataset']['records_returned']}",
        file=sys.stderr,
    )


def run(args: argparse.Namespace) -> int:
    fields = parse_fields(args.fields)
    start, end, period_label = determine_range(
        period=args.period,
        anchor_date=args.date,
        custom_start=args.start,
        custom_end=args.end,
    )

    with connect_db(args.db) as conn:
        if not ensure_required_tables(conn):
            print(
                "SUMMARY_ERR reason=missing_tables detail='feeds/entries not found. "
                "Run ai-tech-rss-fetch init-db and sync first.'",
                file=sys.stderr,
            )
            return 2

        start_utc_iso = start.isoformat().replace("+00:00", "Z")
        end_utc_iso = end.isoformat().replace("+00:00", "Z")
        total_in_range = count_rows_in_range(conn, start_utc_iso=start_utc_iso, end_utc_iso=end_utc_iso)
        if args.max_records > 0 and args.max_per_feed <= 0:
            fetch_limit = args.max_records
        else:
            fetch_limit = total_in_range
        rows, has_entry_content = load_rows(
            conn,
            start_utc_iso=start_utc_iso,
            end_utc_iso=end_utc_iso,
            limit=fetch_limit,
        )

    records_raw: list[dict[str, Any]] = []
    for row in rows:
        record = build_record(row, summary_chars=args.summary_chars, fulltext_chars=args.fulltext_chars)
        if record is not None:
            records_raw.append(record)

    records_filtered = filter_and_rank_records(
        records=records_raw,
        max_records=args.max_records,
        max_per_feed=args.max_per_feed,
    )
    records_selected = select_record_fields(records_filtered, fields)

    payload = build_output_payload(
        period=args.period,
        period_label=period_label,
        start=start,
        end=end,
        selected_fields=fields,
        records_all_in_range=total_in_range,
        records_returned=records_selected,
        has_entry_content=has_entry_content,
        max_records=args.max_records,
        max_per_feed=args.max_per_feed,
        top_feeds=args.top_feeds,
        top_keywords=args.top_keywords,
    )
    emit_output(payload=payload, output_path=args.output, pretty=args.pretty)

    if args.fail_on_empty and payload["dataset"]["records_returned"] == 0:
        return 1
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Retrieve time-windowed RSS records for agent-side RAG summarization.",
    )
    parser.add_argument("--db", default=DEFAULT_DB_PATH, help=f"SQLite db path (default: {DEFAULT_DB_PATH})")
    parser.add_argument(
        "--period",
        choices=["daily", "weekly", "monthly", "custom"],
        default="daily",
        help="Time window type.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Anchor date for daily/weekly/monthly in YYYY-MM-DD (default: today UTC).",
    )
    parser.add_argument("--start", default=None, help="Custom start (YYYY-MM-DD or ISO datetime).")
    parser.add_argument("--end", default=None, help="Custom end (YYYY-MM-DD or ISO datetime).")
    parser.add_argument("--max-records", type=int, default=DEFAULT_MAX_RECORDS, help="Max records in final context.")
    parser.add_argument(
        "--max-per-feed",
        type=int,
        default=DEFAULT_MAX_PER_FEED,
        help="Optional cap per feed. 0 means no cap.",
    )
    parser.add_argument(
        "--summary-chars",
        type=int,
        default=DEFAULT_SUMMARY_CHARS,
        help="Max summary chars per record.",
    )
    parser.add_argument(
        "--fulltext-chars",
        type=int,
        default=DEFAULT_FULLTEXT_CHARS,
        help="Max fulltext excerpt chars per record.",
    )
    parser.add_argument("--top-feeds", type=int, default=DEFAULT_TOP_FEEDS, help="Top feed rows in aggregates.")
    parser.add_argument("--top-keywords", type=int, default=DEFAULT_TOP_KEYWORDS, help="Top keyword rows in aggregates.")
    parser.add_argument(
        "--fields",
        default=",".join(DEFAULT_FIELDS),
        help=f"Comma-separated fields to return. Allowed: {','.join(sorted(ALL_FIELDS))}",
    )
    parser.add_argument("--output", default=None, help="Optional JSON output file path.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument("--fail-on-empty", action="store_true", help="Exit 1 when no records are returned.")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return run(args)
    except sqlite3.Error as exc:
        print(f"SUMMARY_ERR reason=sqlite_error detail={exc}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"SUMMARY_ERR reason=value_error detail={exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"SUMMARY_ERR reason=unexpected detail={exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
