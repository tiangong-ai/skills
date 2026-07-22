from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from .api import fetch_paper
from .errors import PaperFetchError
from .http import HttpClient
from .idempotency import IdempotencyStore, request_fingerprint
from .models import FetchRequest
from .normalize import normalize_doi
from .pipeline import SOURCE_ORDER
from .sanitize import sanitize_data


CLI_VERSION = "1.0.0"
SCHEMA_VERSION = "2.0.0"
EXIT_SUCCESS = 0
EXIT_UNRESOLVED = 1
EXIT_VALIDATION = 3
EXIT_TRANSPORT = 4


def schema() -> dict[str, Any]:
    return {
        "command": "academic-paper-download",
        "cli_version": CLI_VERSION,
        "schema_version": SCHEMA_VERSION,
        "source_order": list(SOURCE_ORDER),
        "artifact_pipeline": [
            "temporary_file",
            "pdf_structure_validation",
            "sha256",
            "atomic_rename",
            "manifest_commit",
        ],
        "dependencies": {"pypdf": "==6.14.2"},
        "environment": {
            "UNPAYWALL_EMAIL": "Enable Unpaywall with its required contact email.",
            "SEMANTIC_SCHOLAR_API_KEY": "Optional Semantic Scholar API key.",
        },
        "exit_codes": {"0": "success", "1": "unresolved", "3": "validation", "4": "transport"},
    }


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise PaperFetchError("validation_error", message, retryable=False)


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="academic-paper-download",
        description="Resolve and reliably save academic PDFs from legal open-access sources.",
    )
    parser.add_argument("doi", nargs="?")
    parser.add_argument("--title")
    parser.add_argument("--author", help="Disambiguate an exact title by author name")
    parser.add_argument("--year", type=int, help="Disambiguate an exact title by publication year")
    parser.add_argument("--batch", metavar="FILE")
    parser.add_argument("--out", required=True, help="Explicit final output directory")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--format", choices=["json", "text"], default="json")
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--stream", action="store_true")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--version", action="version", version=f"%(prog)s {CLI_VERSION}")
    return parser


def _emit(payload: dict[str, Any], *, pretty: bool) -> None:
    print(json.dumps(sanitize_data(payload), ensure_ascii=False, indent=2 if pretty else None))


def _load_inputs(args: argparse.Namespace) -> list[str]:
    selected = sum(bool(value) for value in (args.doi, args.title, args.batch))
    if selected != 1:
        raise PaperFetchError("validation_error", "Pass exactly one of DOI, --title, or --batch")
    if args.batch:
        text = sys.stdin.read() if args.batch == "-" else Path(args.batch).read_text(encoding="utf-8")
        values = [line.strip() for line in text.splitlines() if line.strip()]
        if not values:
            raise PaperFetchError("validation_error", "Batch input contains no DOIs")
        return [normalize_doi(value) for value in values]
    return [normalize_doi(args.doi)] if args.doi else []


def _exit_code(results: list[dict[str, Any]]) -> int:
    if all(result.get("success") for result in results):
        return EXIT_SUCCESS
    codes = {(result.get("error") or {}).get("code", "") for result in results if not result.get("success")}
    if codes & {
        "validation_error",
        "idempotency_conflict",
        "title_low_confidence",
        "title_ambiguous",
        "pdf_validator_unavailable",
        "output_dir_error",
    }:
        return EXIT_VALIDATION
    if any(bool((result.get("error") or {}).get("retryable")) for result in results):
        return EXIT_TRANSPORT
    return EXIT_UNRESOLVED


def _text(payload: dict[str, Any]) -> None:
    for result in (payload.get("data") or {}).get("results", []):
        status = "saved" if result.get("success") else "failed"
        if result.get("skipped"):
            status = "verified-existing"
        print(f"[{result.get('source') or '?'}] {result.get('doi')} -> {result.get('file') or '-'} ({status})")


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "schema":
        pretty = "--pretty" in argv[1:]
        _emit({"ok": True, "data": schema()}, pretty=pretty)
        return EXIT_SUCCESS
    try:
        args = _parser().parse_args(argv)
    except PaperFetchError as exc:
        _emit({"ok": False, "error": exc.as_dict()}, pretty=False)
        return EXIT_VALIDATION
    if (args.author or args.year is not None) and not args.title:
        _emit(
            {"ok": False, "error": {"code": "validation_error", "message": "--author and --year require --title"}},
            pretty=args.pretty,
        )
        return EXIT_VALIDATION
    if args.timeout <= 0:
        _emit({"ok": False, "error": {"code": "validation_error", "message": "--timeout must be positive"}}, pretty=args.pretty)
        return EXIT_VALIDATION
    if args.stream and args.pretty:
        _emit({"ok": False, "error": {"code": "validation_error", "message": "--stream and --pretty cannot be combined"}}, pretty=False)
        return EXIT_VALIDATION
    if args.stream and args.format != "json":
        _emit({"ok": False, "error": {"code": "validation_error", "message": "--stream requires --format json"}}, pretty=False)
        return EXIT_VALIDATION

    started = time.monotonic()
    request_id = f"req_{uuid.uuid4().hex[:12]}"

    def progress(event: str, fields: dict[str, Any]) -> None:
        if args.format == "json":
            print(
                json.dumps(sanitize_data({"event": event, "request_id": request_id, **fields})),
                file=sys.stderr,
                flush=True,
            )

    email = os.environ.get("UNPAYWALL_EMAIL", "").strip()
    s2_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "").strip()
    transport = HttpClient(user_agent=f"academic-paper-download/{CLI_VERSION}")
    try:
        inputs = _load_inputs(args)
    except (OSError, UnicodeError, PaperFetchError) as exc:
        error = exc if isinstance(exc, PaperFetchError) else PaperFetchError("validation_error", str(exc))
        _emit({"ok": False, "error": error.as_dict(), "meta": {"request_id": request_id}}, pretty=args.pretty)
        return EXIT_TRANSPORT if error.retryable else EXIT_VALIDATION

    output_dir = str(Path(args.out).expanduser().absolute())
    fingerprint_payload = {
        "dois": inputs,
        "title": args.title,
        "author": args.author,
        "year": args.year,
        "out": output_dir,
        "dry_run": args.dry_run,
        "unpaywall_enabled": bool(email),
        "schema_version": SCHEMA_VERSION,
    }
    fingerprint = request_fingerprint(fingerprint_payload)
    idem = IdempotencyStore(Path(output_dir))
    if args.idempotency_key:
        try:
            cached = idem.load(args.idempotency_key, fingerprint)
        except PaperFetchError as exc:
            _emit({"ok": False, "error": exc.as_dict(), "meta": {"request_id": request_id}}, pretty=args.pretty)
            return EXIT_VALIDATION
        if cached is not None:
            cached = dict(cached)
            cached["meta"] = {
                **(cached.get("meta") or {}),
                "request_id": request_id,
                "replayed": True,
                "latency_ms": int((time.monotonic() - started) * 1000),
            }
            _emit(cached, pretty=args.pretty)
            return EXIT_SUCCESS

    requests = (
        [
            FetchRequest(
                output_dir=output_dir,
                title=args.title,
                author=args.author,
                year=args.year,
                timeout=args.timeout,
                dry_run=args.dry_run,
                unpaywall_email=email,
                semantic_scholar_api_key=s2_key,
            )
        ]
        if args.title
        else [
            FetchRequest(
                output_dir=output_dir,
                doi=doi,
                timeout=args.timeout,
                dry_run=args.dry_run,
                unpaywall_email=email,
                semantic_scholar_api_key=s2_key,
            )
            for doi in inputs
        ]
    )
    results: list[dict[str, Any]] = []
    for request in requests:
        result = fetch_paper(request, transport=transport, progress=progress)
        results.append(result)
        if args.stream and args.format == "json":
            _emit({"ok": bool(result.get("success")), "data": result}, pretty=False)
    succeeded = sum(bool(result.get("success")) for result in results)
    ok: bool | str = True if succeeded == len(results) else ("partial" if succeeded else False)
    title_resolution = results[0].get("title_resolution") if args.title and results else None
    envelope = {
        "ok": ok,
        "data": {
            "results": results,
            "summary": {"total": len(results), "succeeded": succeeded, "failed": len(results) - succeeded},
        },
        "meta": {
            "request_id": request_id,
            "cli_version": CLI_VERSION,
            "schema_version": SCHEMA_VERSION,
            "latency_ms": int((time.monotonic() - started) * 1000),
            **({"title_resolution": title_resolution} if title_resolution else {}),
        },
    }
    if not args.stream:
        if args.format == "text":
            _text(sanitize_data(envelope))
        else:
            _emit(envelope, pretty=args.pretty)
    elif args.format == "json":
        _emit({"ok": ok, "summary": envelope["data"]["summary"], "meta": envelope["meta"]}, pretty=False)
    if args.idempotency_key and not args.dry_run:
        idem.store(args.idempotency_key, fingerprint, sanitize_data(envelope))
    return _exit_code(results)
