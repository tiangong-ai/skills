#!/usr/bin/env python3
"""Backfill missing abstracts from DOI pages via OpenClaw Browser."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

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

LOG_DIR_ENV = "KB_LOG_DIR"
REQUIRED_CREATED_COLUMN = "created_at"
NOISE_HINTS = (
    "cookie",
    "privacy",
    "javascript",
    "enable javascript",
    "your browser",
)

EXTRACT_ABSTRACT_JS = r"""(() => {
  const normalize = (value) =>
    String(value || "")
      .replace(/\u00a0/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  const cleanup = (value) => normalize(value).replace(/^abstract[:\s-]*/i, "").trim();
  const candidates = [];
  const add = (value, source) => {
    const text = cleanup(value);
    if (!text) return;
    candidates.push({ text, source, length: text.length });
  };

  const metaSelectors = [
    ["meta[name='dc.Description']", "content"],
    ["meta[name='dc.description']", "content"],
    ["meta[name='description']", "content"],
    ["meta[property='og:description']", "content"],
    ["meta[name='twitter:description']", "content"],
    ["meta[name='citation_abstract']", "content"],
    ["meta[name='eprints.abstract']", "content"],
    ["meta[name='wt.abstract']", "content"],
  ];
  for (const [selector, attr] of metaSelectors) {
    const node = document.querySelector(selector);
    if (node) add(node.getAttribute(attr), `meta:${selector}`);
  }

  const selectors = [
    "#abstract p",
    "#Abs1-content",
    ".abstract p",
    ".article__abstract p",
    ".c-article-section__content p",
    "[id*='abstract'] p",
    "[class*='abstract'] p",
  ];
  for (const selector of selectors) {
    const nodes = Array.from(document.querySelectorAll(selector));
    for (const node of nodes.slice(0, 8)) {
      add(node.innerText || node.textContent || "", `selector:${selector}`);
    }
  }

  const headingNodes = Array.from(document.querySelectorAll("h1,h2,h3,h4,strong,b,span,div,p"));
  for (const node of headingNodes.slice(0, 150)) {
    const label = normalize(node.textContent || "");
    if (!/^abstract\b/i.test(label)) continue;
    const chunks = [];
    let sibling = node.nextElementSibling;
    let count = 0;
    while (sibling && count < 4) {
      const text = normalize(sibling.textContent || "");
      if (text) chunks.push(text);
      sibling = sibling.nextElementSibling;
      count += 1;
    }
    if (chunks.length > 0) add(chunks.join(" "), "heading-next-siblings");
  }

  candidates.sort((a, b) => b.length - a.length);
  const best = candidates[0] || null;
  return {
    abstract: best ? best.text : "",
    source: best ? best.source : null,
    candidates: candidates.slice(0, 5),
    title: normalize(document.title || ""),
    finalUrl: location.href,
  };
})()"""


@dataclass
class CandidateRow:
    doi: str
    created_value: Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch abstract for rows with empty abstract by opening DOI pages via OpenClaw Browser."
        )
    )
    parser.add_argument("--limit", type=int, default=100, help="Max rows to process (default: 100).")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually write abstract back to DB. Default is dry-run.",
    )
    parser.add_argument("--table", default="journals", help="Target table (default: journals).")
    parser.add_argument("--schema", default=None, help="Optional schema override for --table.")
    parser.add_argument("--doi-column", default="doi", help="DOI column name.")
    parser.add_argument("--abstract-column", default="abstract", help="Abstract column name.")
    parser.add_argument(
        "--min-chars",
        type=int,
        default=80,
        help="Minimum accepted abstract length after cleanup.",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=8000,
        help="Maximum stored abstract length (trim when exceeded).",
    )
    parser.add_argument(
        "--openclaw-bin",
        default="openclaw",
        help="OpenClaw CLI executable path/name.",
    )
    parser.add_argument(
        "--browser-profile",
        default=None,
        help="Optional OpenClaw browser profile name.",
    )
    parser.add_argument(
        "--browser-timeout-ms",
        type=int,
        default=30000,
        help="Timeout per browser command in milliseconds.",
    )
    parser.add_argument(
        "--max-errors",
        type=int,
        default=20,
        help="Abort if runtime errors exceed this count.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run local parser/noise tests and exit.",
    )
    return parser.parse_args()


def configure_logging() -> Path:
    log_dir_raw = os.environ.get(LOG_DIR_ENV, "").strip()
    if not log_dir_raw:
        raise RuntimeError(f"Missing required environment variable: {LOG_DIR_ENV}")
    log_dir = Path(log_dir_raw)
    log_dir.mkdir(parents=True, exist_ok=True)

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    log_path = log_dir / f"kb-abstract-fetch-{run_ts}.log"
    logging.basicConfig(
        filename=str(log_path),
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(message)s",
        force=True,
    )
    return log_path


def get_db_config() -> dict[str, str]:
    load_dotenv()
    required = ["KB_DB_HOST", "KB_DB_PORT", "KB_DB_NAME", "KB_DB_USER", "KB_DB_PASSWORD"]
    missing = [key for key in required if not os.environ.get(key)]
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


def list_table_columns(conn: psycopg2.extensions.connection, schema: str, table: str) -> list[str]:
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


def resolve_created_column(columns: list[str]) -> str:
    matched = match_column_name(columns, REQUIRED_CREATED_COLUMN)
    if matched:
        return matched
    raise RuntimeError(
        f'Missing required created-time column "{REQUIRED_CREATED_COLUMN}". '
        f"Available columns: {', '.join(columns)}"
    )


def parse_json_payload(text: str) -> Any:
    raw = text.strip()
    if not raw:
        raise ValueError("Empty JSON output")
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        for open_char, close_char in (("{", "}"), ("[", "]")):
            start = raw.find(open_char)
            end = raw.rfind(close_char)
            if start >= 0 and end > start:
                snippet = raw[start : end + 1]
                try:
                    return json.loads(snippet)
                except json.JSONDecodeError:
                    continue
    raise ValueError(f"Unable to parse JSON output: {raw[:300]}")


def run_openclaw_browser_json(
    openclaw_bin: str,
    browser_profile: str | None,
    args: list[str],
    timeout_ms: int,
) -> dict[str, Any]:
    cmd = [openclaw_bin, "browser"]
    if browser_profile:
        cmd.extend(["--browser-profile", browser_profile])
    cmd.extend(args)
    cmd.append("--json")

    timeout_s = max(1, timeout_ms) / 1000
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout_s,
        check=False,
    )
    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()

    if proc.returncode != 0:
        detail = stderr or stdout or f"exit code {proc.returncode}"
        raise RuntimeError(f"OpenClaw command failed: {' '.join(cmd)} | {detail}")

    payload = parse_json_payload(stdout)
    if not isinstance(payload, dict):
        raise RuntimeError(f"Unexpected OpenClaw payload type: {type(payload).__name__}")
    return payload


def ensure_browser_ready(
    openclaw_bin: str,
    browser_profile: str | None,
    timeout_ms: int,
) -> None:
    status = run_openclaw_browser_json(openclaw_bin, browser_profile, ["status"], timeout_ms)
    if status.get("running") is True:
        return

    logging.info("OpenClaw browser not running, attempting start.")
    run_openclaw_browser_json(openclaw_bin, browser_profile, ["start"], timeout_ms)
    status = run_openclaw_browser_json(openclaw_bin, browser_profile, ["status"], timeout_ms)
    if status.get("running") is not True:
        raise RuntimeError(
            "OpenClaw browser is still not running. "
            "Attach the browser extension/session, then retry."
        )


def normalize_doi_for_url(doi: str) -> str:
    value = doi.strip()
    value = re.sub(r"^doi:\s*", "", value, flags=re.IGNORECASE)
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value, flags=re.IGNORECASE)
    return value.strip()


def normalize_abstract(text: str) -> str:
    value = str(text or "")
    value = value.replace("\u00a0", " ")
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"^abstract[:\s-]*", "", value, flags=re.IGNORECASE).strip()
    return value


def looks_like_noise(text: str) -> bool:
    lower = text.lower()
    hits = sum(1 for token in NOISE_HINTS if token in lower)
    return hits >= 2 and len(text) < 500


def fetch_candidates(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    doi_column: str,
    abstract_column: str,
    created_column: str,
    limit: int,
) -> list[CandidateRow]:
    query = sql.SQL(
        """
        SELECT {doi}, {created}
        FROM {table}
        WHERE {doi} IS NOT NULL
          AND NULLIF(BTRIM({doi}::text), '') IS NOT NULL
          AND ({abstract} IS NULL OR NULLIF(BTRIM({abstract}::text), '') IS NULL)
        ORDER BY {created} DESC NULLS LAST
        LIMIT %s
        """
    ).format(
        doi=sql.Identifier(doi_column),
        created=sql.Identifier(created_column),
        abstract=sql.Identifier(abstract_column),
        table=sql.Identifier(schema, table),
    )

    with conn.cursor() as cur:
        cur.execute(query, (limit,))
        rows = cur.fetchall()
    return [CandidateRow(doi=str(row[0]), created_value=row[1]) for row in rows]


def update_abstract_if_empty(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    doi_column: str,
    abstract_column: str,
    doi: str,
    abstract: str,
) -> int:
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
    with conn.cursor() as cur:
        cur.execute(query, (abstract, doi))
        return cur.rowcount


def extract_abstract_from_doi(
    openclaw_bin: str,
    browser_profile: str | None,
    doi: str,
    timeout_ms: int,
) -> dict[str, Any]:
    doi_normalized = normalize_doi_for_url(doi)
    if not doi_normalized:
        raise ValueError("DOI is empty after normalization")
    url = f"https://doi.org/{quote(doi_normalized, safe='/')}"

    target_id: str | None = None
    try:
        opened = run_openclaw_browser_json(
            openclaw_bin, browser_profile, ["open", url], timeout_ms
        )
        target = opened.get("targetId")
        if not isinstance(target, str) or not target.strip():
            raise RuntimeError(f"OpenClaw open response missing targetId: {opened}")
        target_id = target.strip()

        run_openclaw_browser_json(
            openclaw_bin,
            browser_profile,
            [
                "wait",
                "--target-id",
                target_id,
                "--load",
                "domcontentloaded",
                "--timeout-ms",
                str(timeout_ms),
            ],
            timeout_ms,
        )
        run_openclaw_browser_json(
            openclaw_bin,
            browser_profile,
            [
                "wait",
                "--target-id",
                target_id,
                "--time",
                "800",
                "--timeout-ms",
                str(timeout_ms),
            ],
            timeout_ms,
        )

        evaluated = run_openclaw_browser_json(
            openclaw_bin,
            browser_profile,
            ["evaluate", "--target-id", target_id, "--fn", EXTRACT_ABSTRACT_JS],
            timeout_ms,
        )
        result = evaluated.get("result")
        if isinstance(result, dict):
            abstract = result.get("abstract")
            source = result.get("source")
            final_url = result.get("finalUrl") or evaluated.get("url") or url
            title = result.get("title")
            top_candidates = result.get("candidates")
        else:
            abstract = result if isinstance(result, str) else ""
            source = None
            final_url = evaluated.get("url") or url
            title = None
            top_candidates = None

        return {
            "source_url": url,
            "final_url": final_url,
            "title": title,
            "source": source,
            "abstract": abstract,
            "candidates": top_candidates,
        }
    finally:
        if target_id:
            try:
                run_openclaw_browser_json(
                    openclaw_bin, browser_profile, ["close", target_id], timeout_ms
                )
            except Exception as exc:  # noqa: BLE001
                logging.warning("Failed to close tab %s: %s", target_id, exc)


def run_self_test() -> int:
    normalize_cases = [
        ("  Abstract:  Hello   world  ", "Hello world"),
        ("ABSTRACT -  A short summary.", "A short summary."),
        ("\nAbstract  This is\nmulti-line.\n", "This is multi-line."),
    ]
    for raw, expected in normalize_cases:
        actual = normalize_abstract(raw)
        if actual != expected:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "phase": "normalize_abstract",
                        "raw": raw,
                        "expected": expected,
                        "actual": actual,
                    },
                    ensure_ascii=False,
                )
            )
            return 1

    noise_cases = [
        ("Enable JavaScript and cookie settings in your browser for privacy.", True),
        ("This study evaluates lifecycle impacts of recycled alloys.", False),
    ]
    for text, expected in noise_cases:
        actual = looks_like_noise(text)
        if actual != expected:
            print(
                json.dumps(
                    {
                        "ok": False,
                        "phase": "looks_like_noise",
                        "text": text,
                        "expected": expected,
                        "actual": actual,
                    },
                    ensure_ascii=False,
                )
            )
            return 1

    print(json.dumps({"ok": True, "self_test": "passed"}))
    return 0


def main() -> int:
    args = parse_args()
    if args.self_test:
        return run_self_test()

    if psycopg2 is None or sql is None:
        raise RuntimeError(
            "Missing dependency: psycopg2. Install with `python3 -m pip install psycopg2-binary`."
        )

    if args.limit <= 0:
        raise ValueError("--limit must be > 0")
    if args.min_chars < 1:
        raise ValueError("--min-chars must be >= 1")
    if args.max_chars < args.min_chars:
        raise ValueError("--max-chars must be >= --min-chars")
    if args.max_errors < 1:
        raise ValueError("--max-errors must be >= 1")

    load_dotenv()
    log_path = configure_logging()
    db_cfg = get_db_config()
    logging.info("Log file: %s", log_path)
    logging.info("Mode: %s", "apply" if args.apply else "dry-run")

    summary: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry-run",
        "limit": args.limit,
        "selected": 0,
        "processed": 0,
        "fetched": 0,
        "accepted": 0,
        "updated": 0,
        "skipped_short": 0,
        "skipped_noise": 0,
        "update_skipped_not_empty": 0,
        "fetch_errors": 0,
        "errors": 0,
    }

    schema, table = parse_table_name(args.table, args.schema)
    conn = psycopg2.connect(
        database=db_cfg["database"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        host=db_cfg["host"],
        port=db_cfg["port"],
    )
    conn.autocommit = True

    try:
        columns = list_table_columns(conn, schema, table)
        if not columns:
            raise RuntimeError(f"Table not found or no columns visible: {schema}.{table}")

        doi_column = match_column_name(columns, args.doi_column)
        if not doi_column:
            raise RuntimeError(f'DOI column "{args.doi_column}" not found in {schema}.{table}')
        abstract_column = match_column_name(columns, args.abstract_column)
        if not abstract_column:
            raise RuntimeError(
                f'Abstract column "{args.abstract_column}" not found in {schema}.{table}'
            )
        created_column = resolve_created_column(columns)

        logging.info(
            "Resolved columns: doi=%s abstract=%s created=%s",
            doi_column,
            abstract_column,
            created_column,
        )
        rows = fetch_candidates(
            conn=conn,
            schema=schema,
            table=table,
            doi_column=doi_column,
            abstract_column=abstract_column,
            created_column=created_column,
            limit=args.limit,
        )
        summary["selected"] = len(rows)
        logging.info("Selected %s candidate rows", len(rows))

        if not rows:
            print(f"RUN_SUMMARY_JSON={json.dumps(summary, ensure_ascii=False)}")
            return 0

        ensure_browser_ready(args.openclaw_bin, args.browser_profile, args.browser_timeout_ms)

        for index, row in enumerate(rows, start=1):
            if summary["errors"] >= args.max_errors:
                logging.error("Abort: errors reached max-errors=%s", args.max_errors)
                break

            doi_raw = row.doi.strip()
            if not doi_raw:
                logging.warning("Skip empty DOI at row %s", index)
                continue

            summary["processed"] += 1
            logging.info("Processing %s/%s DOI=%s", index, len(rows), doi_raw)

            try:
                extraction = extract_abstract_from_doi(
                    openclaw_bin=args.openclaw_bin,
                    browser_profile=args.browser_profile,
                    doi=doi_raw,
                    timeout_ms=args.browser_timeout_ms,
                )
                summary["fetched"] += 1
            except Exception as exc:  # noqa: BLE001
                summary["fetch_errors"] += 1
                summary["errors"] += 1
                logging.error("Fetch failed DOI=%s error=%s", doi_raw, exc)
                continue

            abstract = normalize_abstract(str(extraction.get("abstract") or ""))
            if len(abstract) < args.min_chars:
                summary["skipped_short"] += 1
                logging.info(
                    "Skip short abstract DOI=%s len=%s source=%s final_url=%s",
                    doi_raw,
                    len(abstract),
                    extraction.get("source"),
                    extraction.get("final_url"),
                )
                continue

            if looks_like_noise(abstract):
                summary["skipped_noise"] += 1
                logging.info(
                    "Skip noise-like abstract DOI=%s source=%s final_url=%s",
                    doi_raw,
                    extraction.get("source"),
                    extraction.get("final_url"),
                )
                continue

            if len(abstract) > args.max_chars:
                abstract = abstract[: args.max_chars].rstrip()

            summary["accepted"] += 1
            if not args.apply:
                logging.info(
                    "Dry-run accept DOI=%s len=%s source=%s final_url=%s",
                    doi_raw,
                    len(abstract),
                    extraction.get("source"),
                    extraction.get("final_url"),
                )
                continue

            try:
                changed = update_abstract_if_empty(
                    conn=conn,
                    schema=schema,
                    table=table,
                    doi_column=doi_column,
                    abstract_column=abstract_column,
                    doi=doi_raw,
                    abstract=abstract,
                )
                if changed > 0:
                    summary["updated"] += 1
                    logging.info(
                        "Updated DOI=%s len=%s source=%s final_url=%s",
                        doi_raw,
                        len(abstract),
                        extraction.get("source"),
                        extraction.get("final_url"),
                    )
                else:
                    summary["update_skipped_not_empty"] += 1
                    logging.info("Skip update DOI=%s because abstract already filled", doi_raw)
            except Exception as exc:  # noqa: BLE001
                summary["errors"] += 1
                logging.error("DB update failed DOI=%s error=%s", doi_raw, exc)

    finally:
        conn.close()

    summary["log_file"] = str(log_path)
    print(f"RUN_SUMMARY_JSON={json.dumps(summary, ensure_ascii=False)}")
    logging.info("RUN_SUMMARY_JSON=%s", json.dumps(summary, ensure_ascii=False))
    return 0 if summary["errors"] < args.max_errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
