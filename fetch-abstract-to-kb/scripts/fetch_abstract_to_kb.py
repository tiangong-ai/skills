#!/usr/bin/env python3
"""Fetch DOI rows from KB and batch update abstracts by DOI."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - optional fallback
    def load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False

try:
    import psycopg2
    from psycopg2 import sql
except ModuleNotFoundError:  # pragma: no cover - handled at runtime
    psycopg2 = None  # type: ignore[assignment]
    sql = None  # type: ignore[assignment]


REQUIRED_DB_ENV = [
    "KB_DB_HOST",
    "KB_DB_PORT",
    "KB_DB_NAME",
    "KB_DB_USER",
    "KB_DB_PASSWORD",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch DOI candidates and batch update abstracts in PostgreSQL journals."
    )
    parser.add_argument("--table", default="journals", help="Target table name.")
    parser.add_argument("--schema", default=None, help="Optional schema override for --table.")
    parser.add_argument("--doi-column", default="doi", help="DOI column name.")
    parser.add_argument(
        "--author-column",
        default="author",
        help="Author column name (defaults to author; falls back to authors when available).",
    )
    parser.add_argument("--abstract-column", default="abstract", help="Abstract column name.")
    parser.add_argument("--created-column", default="created_at", help="Created timestamp column name.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch_parser = subparsers.add_parser("fetch-doi", help="Fetch DOI list from DB.")
    fetch_parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of rows to fetch, newest first. Default: 10.",
    )

    write_parser = subparsers.add_parser(
        "write-abstracts",
        help="Batch update abstract values by DOI from a JSON file.",
    )
    write_parser.add_argument(
        "--input",
        required=True,
        help="Path to JSON input. Supports [{\"doi\":...,\"abstract\":...}] or {\"doi\":\"abstract\"}.",
    )

    return parser.parse_args()


def get_db_config() -> dict[str, str]:
    load_dotenv()
    missing = [key for key in REQUIRED_DB_ENV if not os.environ.get(key)]
    if missing:
        raise RuntimeError(f"Missing database env variables: {', '.join(missing)}")

    return {
        "host": os.environ["KB_DB_HOST"],
        "port": os.environ["KB_DB_PORT"],
        "database": os.environ["KB_DB_NAME"],
        "user": os.environ["KB_DB_USER"],
        "password": os.environ["KB_DB_PASSWORD"],
    }


def parse_table_name(table_raw: str, schema_override: str | None) -> tuple[str, str]:
    table_raw = table_raw.strip()
    if not table_raw:
        raise ValueError("--table cannot be empty")

    if "." in table_raw:
        schema, table = table_raw.split(".", 1)
    else:
        schema, table = (schema_override or "public"), table_raw

    schema = schema.strip().strip('"')
    table = table.strip().strip('"')
    if not schema or not table:
        raise ValueError(f"Invalid table reference: {table_raw}")
    return schema, table


def list_table_columns(conn: Any, schema: str, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [row[0] for row in cur.fetchall()]


def match_column_name(columns: list[str], desired: str) -> str | None:
    lookup = {name.lower(): name for name in columns}
    return lookup.get(desired.lower())


def resolve_column_or_raise(columns: list[str], desired: str, label: str) -> str:
    resolved = match_column_name(columns, desired)
    if not resolved:
        raise RuntimeError(f'{label} column "{desired}" not found. Available: {", ".join(columns)}')
    return resolved


def resolve_author_column(columns: list[str], desired: str) -> str:
    resolved = match_column_name(columns, desired)
    if resolved:
        return resolved

    if desired.lower() == "author":
        fallback = match_column_name(columns, "authors")
        if fallback:
            return fallback

    raise RuntimeError(
        f'Author column "{desired}" not found. Available: {", ".join(columns)}'
    )


def fetch_doi_rows(
    conn: Any,
    schema: str,
    table: str,
    doi_column: str,
    author_column: str,
    abstract_column: str,
    created_column: str,
    limit: int,
) -> list[dict[str, Any]]:
    if limit <= 0:
        raise ValueError("--limit must be > 0")

    query = sql.SQL(
        """
        SELECT {doi}, {created}
        FROM {table}
        WHERE {doi} IS NOT NULL
          AND NULLIF(BTRIM({doi}::text), '') IS NOT NULL
          AND {author} IS NOT NULL
          AND NULLIF(BTRIM({author}::text), '') IS NOT NULL
          AND ({abstract} IS NULL OR NULLIF(BTRIM({abstract}::text), '') IS NULL)
        ORDER BY {created} DESC NULLS LAST
        LIMIT %s
        """
    ).format(
        doi=sql.Identifier(doi_column),
        author=sql.Identifier(author_column),
        abstract=sql.Identifier(abstract_column),
        created=sql.Identifier(created_column),
        table=sql.Identifier(schema, table),
    )

    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        rows = cur.fetchall()

    payload: list[dict[str, Any]] = []
    for doi, created_at in rows:
        payload.append(
            {
                "doi": str(doi).strip(),
                "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else created_at,
            }
        )
    return payload


def load_write_input(input_path: str) -> list[tuple[str, str]]:
    path = Path(input_path)
    if not path.exists():
        raise RuntimeError(f"Input file not found: {input_path}")

    raw = json.loads(path.read_text(encoding="utf-8"))
    pairs: list[tuple[str, str]] = []

    if isinstance(raw, dict):
        for doi, abstract in raw.items():
            if doi is None or abstract is None:
                continue
            pairs.append((str(doi), str(abstract)))
    elif isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            doi = item.get("doi")
            abstract = item.get("abstract")
            if doi is None or abstract is None:
                continue
            pairs.append((str(doi), str(abstract)))
    else:
        raise RuntimeError("Unsupported input format. Use object map or list of {doi, abstract}.")

    dedup: dict[str, str] = {}
    for doi_raw, abstract_raw in pairs:
        doi = doi_raw.strip()
        abstract = abstract_raw.strip()
        if not doi or not abstract:
            continue
        dedup[doi] = abstract

    if not dedup:
        raise RuntimeError("No valid {doi, abstract} pairs found in input JSON.")

    return list(dedup.items())


def write_abstracts(
    conn: Any,
    schema: str,
    table: str,
    doi_column: str,
    abstract_column: str,
    items: list[tuple[str, str]],
) -> dict[str, int]:
    query = sql.SQL(
        """
        UPDATE {table}
        SET {abstract} = %s
        WHERE {doi} = %s
          AND ({abstract} IS NULL OR NULLIF(BTRIM({abstract}::text), '') IS NULL)
        """
    ).format(
        table=sql.Identifier(schema, table),
        doi=sql.Identifier(doi_column),
        abstract=sql.Identifier(abstract_column),
    )

    matched_updates = 0
    with conn.cursor() as cur:
        for doi, abstract in items:
            cur.execute(query, (abstract, doi))
            matched_updates += cur.rowcount
    conn.commit()

    return {
        "input_items": len(items),
        "updated": matched_updates,
        "skipped_not_empty_or_missing": len(items) - matched_updates,
    }


def main() -> int:
    args = parse_args()

    if psycopg2 is None or sql is None:
        raise RuntimeError(
            "Missing dependency: psycopg2. Install with `python3 -m pip install psycopg2-binary`."
        )

    schema, table = parse_table_name(args.table, args.schema)
    db_cfg = get_db_config()

    conn = psycopg2.connect(
        database=db_cfg["database"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        host=db_cfg["host"],
        port=db_cfg["port"],
    )

    try:
        columns = list_table_columns(conn, schema, table)
        if not columns:
            raise RuntimeError(f"Table not found or no columns visible: {schema}.{table}")

        doi_column = resolve_column_or_raise(columns, args.doi_column, "DOI")
        abstract_column = resolve_column_or_raise(columns, args.abstract_column, "Abstract")

        if args.command == "fetch-doi":
            author_column = resolve_author_column(columns, args.author_column)
            created_column = resolve_column_or_raise(columns, args.created_column, "Created")
            rows = fetch_doi_rows(
                conn=conn,
                schema=schema,
                table=table,
                doi_column=doi_column,
                author_column=author_column,
                abstract_column=abstract_column,
                created_column=created_column,
                limit=args.limit,
            )
            print(
                json.dumps(
                    {
                        "command": "fetch-doi",
                        "limit": args.limit,
                        "count": len(rows),
                        "rows": rows,
                    },
                    ensure_ascii=False,
                )
            )
            return 0

        if args.command == "write-abstracts":
            items = load_write_input(args.input)
            result = write_abstracts(
                conn=conn,
                schema=schema,
                table=table,
                doi_column=doi_column,
                abstract_column=abstract_column,
                items=items,
            )
            print(
                json.dumps(
                    {
                        "command": "write-abstracts",
                        "result": result,
                    },
                    ensure_ascii=False,
                )
            )
            return 0

        raise RuntimeError(f"Unsupported command: {args.command}")
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
