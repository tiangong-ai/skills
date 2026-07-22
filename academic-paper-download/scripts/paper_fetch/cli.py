from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from .artifact import ArtifactStore
from .errors import PaperFetchError
from .http import HttpClient
from .idempotency import IdempotencyStore, request_fingerprint
from .models import PaperMetadata
from .normalize import normalize_doi
from .pipeline import PaperFetcher
from .resolvers import OpenAccessResolvers
from .scihub import SciHubResolver


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
        "source_order": ["unpaywall", "semantic_scholar", "arxiv", "scihub", "browser_handoff"],
        "artifact_pipeline": ["temporary_file", "pdf_structure_validation", "sha256", "atomic_rename", "manifest_commit"],
        "dependencies": {"pypdf": ">=5.0,<7"},
        "environment": {
            "UNPAYWALL_EMAIL": "Enable Unpaywall with its required contact email.",
            "SEMANTIC_SCHOLAR_API_KEY": "Optional Semantic Scholar API key.",
            "PAPER_FETCH_NO_SCIHUB": "Set any non-empty value to disable Sci-Hub.",
            "PAPER_FETCH_SCIHUB_MIRRORS": "Optional comma-separated mirror hostnames.",
        },
        "exit_codes": {"0": "success", "1": "unresolved", "3": "validation", "4": "transport"},
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="academic-paper-download",
        description="Resolve and reliably save academic PDFs through OA sources, then Sci-Hub.",
    )
    parser.add_argument("doi", nargs="?")
    parser.add_argument("--title")
    parser.add_argument("--author", help="Disambiguate an exact title by author name")
    parser.add_argument("--year", type=int, help="Disambiguate an exact title by publication year")
    parser.add_argument("--batch", metavar="FILE")
    parser.add_argument("--out", default=str(Path.home() / "Downloads"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--format", choices=["json", "text"], default="json")
    parser.add_argument("--pretty", action="store_true")
    parser.add_argument("--stream", action="store_true")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--version", action="version", version=f"%(prog)s {CLI_VERSION}")
    return parser


def _emit(payload: dict[str, Any], *, pretty: bool) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2 if pretty else None))


def _load_inputs(args: argparse.Namespace) -> list[str]:
    selected = sum(bool(value) for value in (args.doi, args.title, args.batch))
    if selected != 1:
        raise PaperFetchError("validation_error", "Pass exactly one of DOI, --title, or --batch")
    if args.batch:
        text = sys.stdin.read() if args.batch == "-" else Path(args.batch).read_text(encoding="utf-8")
        values = [line.strip() for line in text.splitlines() if line.strip()]
        if not values:
            raise PaperFetchError("validation_error", "Batch input contains no DOIs")
        return values
    return [args.doi] if args.doi else []


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
    args = _parser().parse_args(argv)
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
            print(json.dumps({"event": event, "request_id": request_id, **fields}), file=sys.stderr, flush=True)

    email = os.environ.get("UNPAYWALL_EMAIL", "").strip()
    s2_key = os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "").strip()
    http = HttpClient(user_agent=f"academic-paper-download/{CLI_VERSION} (mailto:{email or 'anonymous'})")
    resolvers = OpenAccessResolvers(http, unpaywall_email=email, semantic_scholar_api_key=s2_key)
    fetcher = PaperFetcher(
        resolvers,
        SciHubResolver(http),
        ArtifactStore(Path(args.out), http),
        scihub_enabled=not bool(os.environ.get("PAPER_FETCH_NO_SCIHUB")),
        progress=progress,
    )

    title_resolution: dict[str, Any] | None = None
    seed = PaperMetadata()
    try:
        if args.title:
            resolved = resolvers.resolve_title(
                args.title,
                timeout=args.timeout,
                author=args.author,
                year=args.year,
            )
            assert resolved.doi is not None
            inputs = [resolved.doi]
            seed = resolved.metadata
            title_resolution = resolved.details
        else:
            inputs = _load_inputs(args)
            inputs = [normalize_doi(value) for value in inputs]
    except (OSError, UnicodeError, PaperFetchError) as exc:
        error = exc if isinstance(exc, PaperFetchError) else PaperFetchError("validation_error", str(exc))
        _emit({"ok": False, "error": error.as_dict(), "meta": {"request_id": request_id}}, pretty=args.pretty)
        return EXIT_TRANSPORT if error.retryable else EXIT_VALIDATION

    fingerprint_payload = {
        "dois": inputs,
        "title": args.title,
        "author": args.author,
        "year": args.year,
        "out": str(Path(args.out).expanduser().resolve()),
        "dry_run": args.dry_run,
        "unpaywall_enabled": bool(email),
        "scihub_enabled": fetcher.scihub_enabled,
        "mirrors": os.environ.get("PAPER_FETCH_SCIHUB_MIRRORS", ""),
        "schema_version": SCHEMA_VERSION,
    }
    fingerprint = request_fingerprint(fingerprint_payload)
    idem = IdempotencyStore(Path(args.out))
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

    results: list[dict[str, Any]] = []
    for doi in inputs:
        result = fetcher.fetch(
            doi,
            timeout=args.timeout,
            dry_run=args.dry_run,
            seed_metadata=PaperMetadata(**seed.as_dict()),
        )
        results.append(result)
        if args.stream and args.format == "json":
            _emit({"ok": bool(result.get("success")), "data": result}, pretty=False)
    succeeded = sum(bool(result.get("success")) for result in results)
    ok: bool | str = True if succeeded == len(results) else ("partial" if succeeded else False)
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
            _text(envelope)
        else:
            _emit(envelope, pretty=args.pretty)
    elif args.format == "json":
        _emit({"ok": ok, "summary": envelope["data"]["summary"], "meta": envelope["meta"]}, pretty=False)
    if args.idempotency_key and not args.dry_run:
        idem.store(args.idempotency_key, fingerprint, envelope)
    return _exit_code(results)
