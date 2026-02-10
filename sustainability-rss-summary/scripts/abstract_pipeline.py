#!/usr/bin/env python3
"""
DOI-based dual-tower abstract fetching for RSS-driven research intelligence.

Primary source:
- OpenAlex (reconstruct `abstract_inverted_index`)

Fallback source:
- Semantic Scholar Graph API (`abstract` field)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

DEFAULT_DB_PATH = "sustainability-rss-summary.db"
DEFAULT_TIMEOUT_SECONDS = 12.0
DEFAULT_MAX_RETRIES = 3
DEFAULT_BACKOFF_HOURS = [24, 24, 48]
TRANSIENT_BACKOFF_MINUTES = 30

OPENALEX_BASE = "https://api.openalex.org/works/https://doi.org/"
SEMANTIC_SCHOLAR_BASE = "https://api.semanticscholar.org/graph/v1/paper/"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pending_papers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doi TEXT NOT NULL UNIQUE,
    title TEXT,
    link TEXT,
    source_feed TEXT,
    status TEXT NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'ready', 'failed')),
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    last_attempt_at TEXT,
    next_retry_at TEXT,
    abstract_source TEXT,
    abstract_text TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def normalize_doi(raw: str) -> str:
    text = (raw or "").strip()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^doi:\s*", "", text, flags=re.IGNORECASE)
    text = text.strip().strip("/")
    return text.lower()


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
        headers["User-Agent"] = "sustainability-rss-summary/1.0"

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


def fetch_abstract(doi: str, openalex_email: str | None, s2_api_key: str | None, timeout: float) -> dict[str, Any]:
    normalized = normalize_doi(doi)
    if not normalized:
        return {
            "doi": doi,
            "normalized_doi": "",
            "status": "invalid",
            "source": None,
            "abstract": None,
            "error": "invalid_doi",
        }

    openalex_abstract, openalex_error = fetch_openalex(normalized, openalex_email, timeout)
    if openalex_abstract:
        return {
            "doi": doi,
            "normalized_doi": normalized,
            "status": "ready",
            "source": "openalex",
            "abstract": openalex_abstract,
            "error": None,
        }

    s2_abstract, s2_error = fetch_semantic_scholar(normalized, s2_api_key, timeout)
    if s2_abstract:
        return {
            "doi": doi,
            "normalized_doi": normalized,
            "status": "ready",
            "source": "semanticscholar",
            "abstract": s2_abstract,
            "error": None,
        }

    errors = [err for err in [openalex_error, s2_error] if err]
    return {
        "doi": doi,
        "normalized_doi": normalized,
        "status": "missing",
        "source": None,
        "abstract": None,
        "error": "|".join(errors) if errors else "unknown",
    }


def connect_db(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    return conn


def upsert_event(
    conn: sqlite3.Connection,
    doi: str,
    title: str | None,
    link: str | None,
    source_feed: str | None,
    max_retries: int,
) -> str:
    normalized = normalize_doi(doi)
    if not normalized:
        return ""

    ts = to_iso(now_utc())
    conn.execute(
        """
        INSERT INTO pending_papers (
            doi, title, link, source_feed, status, retry_count, max_retries, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'new', 0, ?, ?, ?)
        ON CONFLICT(doi) DO UPDATE SET
            title = COALESCE(excluded.title, pending_papers.title),
            link = COALESCE(excluded.link, pending_papers.link),
            source_feed = COALESCE(excluded.source_feed, pending_papers.source_feed),
            max_retries = excluded.max_retries,
            updated_at = excluded.updated_at,
            status = CASE WHEN pending_papers.status = 'ready' THEN 'ready' ELSE 'new' END,
            next_retry_at = CASE WHEN pending_papers.status = 'ready' THEN pending_papers.next_retry_at ELSE NULL END,
            error_message = CASE WHEN pending_papers.status = 'ready' THEN pending_papers.error_message ELSE NULL END
        """,
        (
            normalized,
            title,
            link,
            source_feed,
            max_retries,
            ts,
            ts,
        ),
    )
    return normalized


def iter_events_from_jsonl(path: str | Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    file_path = Path(path)
    for line_number, raw_line in enumerate(file_path.read_text().splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSONL at line {line_number}: {exc}") from exc
        if not isinstance(row, dict):
            raise ValueError(f"Invalid event at line {line_number}: expected object")
        events.append(row)
    return events


def parse_backoff_hours(raw: str) -> list[int]:
    values: list[int] = []
    for segment in raw.split(","):
        token = segment.strip()
        if not token:
            continue
        hour = int(token)
        if hour <= 0:
            raise ValueError("backoff hour must be positive")
        values.append(hour)
    if not values:
        raise ValueError("backoff-hours cannot be empty")
    return values


def next_retry_timestamp(retry_count: int, backoff_hours: list[int]) -> str:
    index = min(max(retry_count - 1, 0), len(backoff_hours) - 1)
    delay = backoff_hours[index]
    return to_iso(now_utc() + timedelta(hours=delay))


def transient_retry_timestamp() -> str:
    return to_iso(now_utc() + timedelta(minutes=TRANSIENT_BACKOFF_MINUTES))


def has_transient_error(error: str | None) -> bool:
    if not error:
        return False
    transient_codes = {
        "openalex_network_error",
        "openalex_server_error",
        "s2_network_error",
        "s2_server_error",
        "s2_rate_limited",
    }
    return any(code in transient_codes for code in error.split("|"))


def queue_run(
    conn: sqlite3.Connection,
    limit: int,
    openalex_email: str | None,
    s2_api_key: str | None,
    timeout: float,
    backoff_hours: list[int],
    force: bool,
) -> dict[str, int]:
    if force:
        rows = conn.execute(
            """
            SELECT * FROM pending_papers
            WHERE status = 'new'
              AND retry_count < max_retries
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    else:
        current = to_iso(now_utc())
        rows = conn.execute(
            """
            SELECT * FROM pending_papers
            WHERE status = 'new'
              AND retry_count < max_retries
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (current, limit),
        ).fetchall()

    stats = {"processed": 0, "ready": 0, "retrying": 0, "failed": 0}
    for row in rows:
        stats["processed"] += 1
        result = fetch_abstract(row["doi"], openalex_email, s2_api_key, timeout)
        ts = to_iso(now_utc())
        if result["status"] == "ready":
            conn.execute(
                """
                UPDATE pending_papers
                SET status = 'ready',
                    abstract_source = ?,
                    abstract_text = ?,
                    error_message = NULL,
                    next_retry_at = NULL,
                    last_attempt_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (result["source"], result["abstract"], ts, ts, row["id"]),
            )
            stats["ready"] += 1
            print(f"READY {row['doi']} source={result['source']}")
            continue

        retry_count = int(row["retry_count"]) + 1
        max_retries = int(row["max_retries"])
        if has_transient_error(result["error"]):
            conn.execute(
                """
                UPDATE pending_papers
                SET status = 'new',
                    error_message = ?,
                    next_retry_at = ?,
                    last_attempt_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (result["error"], transient_retry_timestamp(), ts, ts, row["id"]),
            )
            stats["retrying"] += 1
            print(f"RETRY_TRANSIENT {row['doi']} error={result['error']}")
            continue

        if retry_count >= max_retries:
            conn.execute(
                """
                UPDATE pending_papers
                SET status = 'failed',
                    retry_count = ?,
                    error_message = ?,
                    next_retry_at = NULL,
                    last_attempt_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (retry_count, result["error"], ts, ts, row["id"]),
            )
            stats["failed"] += 1
            print(f"FAILED {row['doi']} error={result['error']}")
            continue

        conn.execute(
            """
            UPDATE pending_papers
            SET status = 'new',
                retry_count = ?,
                error_message = ?,
                next_retry_at = ?,
                last_attempt_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                retry_count,
                result["error"],
                next_retry_timestamp(retry_count, backoff_hours),
                ts,
                ts,
                row["id"],
            ),
        )
        stats["retrying"] += 1
        print(f"RETRY {row['doi']} error={result['error']} retry_count={retry_count}")

    conn.commit()
    return stats


def print_json(data: Any, pretty: bool) -> None:
    if pretty:
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return
    print(json.dumps(data, ensure_ascii=False))


def resolve_openalex_email(value: str | None) -> str | None:
    return value or os.getenv("OPENALEX_EMAIL")


def resolve_s2_api_key(value: str | None) -> str | None:
    return value or os.getenv("S2_API_KEY")


def cmd_fetch(args: argparse.Namespace) -> int:
    result = fetch_abstract(
        doi=args.doi,
        openalex_email=resolve_openalex_email(args.openalex_email),
        s2_api_key=resolve_s2_api_key(args.s2_api_key),
        timeout=args.timeout,
    )
    print_json(result, args.pretty)
    return 0


def cmd_queue_add(args: argparse.Namespace) -> int:
    events: list[dict[str, Any]] = []
    if args.doi:
        for raw_doi in args.doi:
            events.append(
                {
                    "doi": raw_doi,
                    "title": args.title,
                    "link": args.link,
                    "source_feed": args.source_feed,
                }
            )
    if args.jsonl:
        events.extend(iter_events_from_jsonl(args.jsonl))

    if not events:
        raise ValueError("No events supplied. Use --doi and/or --jsonl.")

    conn = connect_db(args.db)
    inserted = 0
    skipped = 0
    for event in events:
        normalized = upsert_event(
            conn=conn,
            doi=str(event.get("doi", "")),
            title=event.get("title"),
            link=event.get("link"),
            source_feed=event.get("source_feed"),
            max_retries=args.max_retries,
        )
        if normalized:
            inserted += 1
        else:
            skipped += 1
    conn.commit()

    print_json({"db": str(args.db), "upserted": inserted, "skipped": skipped}, args.pretty)
    return 0


def cmd_queue_run(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    backoff_hours = parse_backoff_hours(args.backoff_hours)
    stats = queue_run(
        conn=conn,
        limit=args.limit,
        openalex_email=resolve_openalex_email(args.openalex_email),
        s2_api_key=resolve_s2_api_key(args.s2_api_key),
        timeout=args.timeout,
        backoff_hours=backoff_hours,
        force=args.force,
    )
    print_json({"db": str(args.db), "force": args.force, **stats}, args.pretty)
    return 0


def cmd_queue_list(args: argparse.Namespace) -> int:
    conn = connect_db(args.db)
    sql = """
        SELECT doi, status, retry_count, max_retries, abstract_source, next_retry_at,
               updated_at, title, source_feed
        FROM pending_papers
    """
    params: list[Any] = []
    if args.status:
        sql += " WHERE status = ?"
        params.append(args.status)
    sql += " ORDER BY updated_at DESC LIMIT ?"
    params.append(args.limit)

    rows = conn.execute(sql, params).fetchall()
    data = [dict(row) for row in rows]
    print_json(data, args.pretty)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dual-tower abstract fetching (OpenAlex + Semantic Scholar) with retry queue.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch", help="Fetch one DOI immediately.")
    fetch_parser.add_argument("--doi", required=True, help="DOI or DOI URL.")
    fetch_parser.add_argument("--openalex-email", default=None, help="Email for OpenAlex User-Agent.")
    fetch_parser.add_argument("--s2-api-key", default=None, help="Semantic Scholar API key.")
    fetch_parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    fetch_parser.add_argument("--pretty", action="store_true")
    fetch_parser.set_defaults(func=cmd_fetch)

    queue_add_parser = subparsers.add_parser("queue-add", help="Insert DOI events into queue DB.")
    queue_add_parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    queue_add_parser.add_argument("--doi", action="append", help="DOI (repeatable).")
    queue_add_parser.add_argument("--title", default=None, help="Title for --doi records.")
    queue_add_parser.add_argument("--link", default=None, help="Link for --doi records.")
    queue_add_parser.add_argument("--source-feed", default=None, help="Feed identifier for --doi records.")
    queue_add_parser.add_argument("--jsonl", default=None, help="JSONL event file.")
    queue_add_parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    queue_add_parser.add_argument("--pretty", action="store_true")
    queue_add_parser.set_defaults(func=cmd_queue_add)

    queue_run_parser = subparsers.add_parser("queue-run", help="Process due queue records.")
    queue_run_parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    queue_run_parser.add_argument("--limit", type=int, default=100, help="Max records per run.")
    queue_run_parser.add_argument("--openalex-email", default=None, help="Email for OpenAlex User-Agent.")
    queue_run_parser.add_argument("--s2-api-key", default=None, help="Semantic Scholar API key.")
    queue_run_parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    queue_run_parser.add_argument(
        "--backoff-hours",
        default="24,24,48",
        help="Retry delays in hours, comma-separated. Example: 24,24,48",
    )
    queue_run_parser.add_argument(
        "--force",
        action="store_true",
        help="Ignore next_retry_at and process all retryable new records.",
    )
    queue_run_parser.add_argument("--pretty", action="store_true")
    queue_run_parser.set_defaults(func=cmd_queue_run)

    queue_list_parser = subparsers.add_parser("queue-list", help="List queue records.")
    queue_list_parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path.")
    queue_list_parser.add_argument("--status", choices=["new", "ready", "failed"], default=None)
    queue_list_parser.add_argument("--limit", type=int, default=200)
    queue_list_parser.add_argument("--pretty", action="store_true")
    queue_list_parser.set_defaults(func=cmd_queue_list)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        return int(args.func(args))
    except Exception as exc:
        print_json({"status": "error", "error": str(exc)}, pretty=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
