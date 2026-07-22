#!/usr/bin/env python3
"""Finalize one exact PDF downloaded through the user's browser session."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from paper_fetch.artifact import (
    DEFAULT_MAX_BYTES,
    atomic_write_json,
    build_manifest,
    choose_available_path,
    filename_for,
    manifest_path,
    validate_pdf,
    verify_existing,
)
from paper_fetch.errors import PaperFetchError
from paper_fetch.models import Candidate, PaperMetadata
from paper_fetch.normalize import normalize_doi


PARTIAL_SUFFIXES = (".crdownload", ".part", ".download", ".tmp")


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def fail(code: str, message: str, exit_code: int, **details: Any) -> int:
    emit({"ok": False, "error": {"code": code, "message": message, **details}})
    return exit_code


def _safe_pdf_filename(value: str) -> str:
    name = Path(value).name
    if name != value or not name.casefold().endswith(".pdf"):
        raise ValueError("expected filename must be one PDF basename")
    return name


def _partial_paths(path: Path) -> list[Path]:
    return [Path(str(path) + suffix) for suffix in PARTIAL_SUFFIXES]


def snapshot(downloads_dir: Path, expected_filename: str, output: Path) -> int:
    if not downloads_dir.is_dir():
        return fail("downloads_dir_missing", f"Downloads directory does not exist: {downloads_dir}", 3)
    try:
        expected_filename = _safe_pdf_filename(expected_filename)
    except ValueError as exc:
        return fail("invalid_expected_filename", str(exc), 3)
    target = downloads_dir / expected_filename
    if target.exists() or manifest_path(target).exists() or any(path.exists() for path in _partial_paths(target)):
        return fail(
            "expected_path_reserved",
            "The expected browser download path already exists; run plan-save and use its collision-free filename",
            3,
            path=str(target),
        )
    payload = {
        "schema_version": "browser-download-snapshot.v2",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_at_ns": time.time_ns(),
        "downloads_dir": str(downloads_dir.resolve()),
        "expected_filename": expected_filename,
    }
    try:
        atomic_write_json(output, payload)
    except OSError as exc:
        return fail("snapshot_write_failed", str(exc), 4, snapshot=str(output))
    emit({"ok": True, "snapshot": str(output.resolve()), "expected_path": str(target)})
    return 0


def load_snapshot(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("schema_version") != "browser-download-snapshot.v2":
        raise ValueError("unsupported snapshot schema")
    _safe_pdf_filename(str(data.get("expected_filename", "")))
    downloads_dir = Path(str(data.get("downloads_dir", ""))).expanduser()
    if not downloads_dir.is_absolute():
        raise ValueError("snapshot downloads_dir must be absolute")
    if not isinstance(data.get("created_at_ns"), int) or data["created_at_ns"] <= 0:
        raise ValueError("snapshot created_at_ns must be a positive integer")
    return data


def wait_for_exact_pdf(
    path: Path,
    *,
    created_at_ns: int,
    timeout: float,
    poll_interval: float,
    stable_seconds: float,
) -> Path | None:
    deadline = time.monotonic() + timeout
    observed_size: int | None = None
    stable_since = 0.0
    while time.monotonic() < deadline:
        if path.is_symlink():
            raise PaperFetchError(
                "unsafe_download_path",
                "The expected browser download must not be a symbolic link",
                path=str(path),
            )
        partial = any(candidate.exists() for candidate in _partial_paths(path))
        if path.is_file() and not partial:
            try:
                stat = path.stat()
            except OSError:
                time.sleep(poll_interval)
                continue
            if stat.st_mtime_ns < created_at_ns:
                raise PaperFetchError(
                    "download_predates_snapshot",
                    "The expected PDF predates the browser-download snapshot",
                    path=str(path),
                    file_mtime_ns=stat.st_mtime_ns,
                    snapshot_created_at_ns=created_at_ns,
                )
            if observed_size != stat.st_size:
                observed_size = stat.st_size
                stable_since = time.monotonic()
            elif time.monotonic() - stable_since >= stable_seconds:
                return path
        time.sleep(poll_interval)
    return None


def choose_plan_filename(downloads_dir: Path, filename: str) -> str:
    return choose_available_path(downloads_dir, _safe_pdf_filename(filename)).name


def plan_save(args: argparse.Namespace) -> int:
    downloads_dir = Path(args.downloads_dir).expanduser().resolve()
    if not downloads_dir.is_dir():
        return fail("downloads_dir_missing", f"Downloads directory does not exist: {downloads_dir}", 3)
    metadata = PaperMetadata(title=args.title, author=args.author, year=args.year)
    if args.filename:
        try:
            base = _safe_pdf_filename(args.filename)
        except ValueError as exc:
            return fail("invalid_filename", str(exc), 3)
    else:
        base = filename_for(metadata, args.doi or "10.0000/unknown")
    filename = choose_plan_filename(downloads_dir, base)
    emit(
        {
            "ok": True,
            "data": {
                "schema_version": "browser-save-plan.v2",
                "downloads_dir": str(downloads_dir),
                "filename": filename,
                "path": str(downloads_dir / filename),
            },
        }
    )
    return 0


def _atomic_copy(source: Path, destination: Path, max_bytes: int) -> tuple[int, str]:
    descriptor, temporary_name = tempfile.mkstemp(
        dir=destination.parent,
        prefix=f".{destination.stem}.",
        suffix=".part",
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        with source.open("rb") as reader, temporary.open("wb") as writer:
            shutil.copyfileobj(reader, writer, length=1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        size, digest = validate_pdf(temporary, max_bytes)
        os.replace(temporary, destination)
        return size, digest
    finally:
        temporary.unlink(missing_ok=True)


def finalize(args: argparse.Namespace) -> int:
    snapshot_path = Path(args.snapshot).expanduser().resolve()
    try:
        state = load_snapshot(snapshot_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return fail("invalid_snapshot", str(exc), 3, snapshot=str(snapshot_path))
    downloads_dir = Path(state["downloads_dir"]).expanduser().resolve()
    if args.downloads_dir:
        supplied_downloads_dir = Path(args.downloads_dir).expanduser().resolve()
        if supplied_downloads_dir != downloads_dir:
            return fail(
                "snapshot_directory_mismatch",
                "The finalize downloads directory must match the snapshot directory",
                3,
                snapshot_directory=str(downloads_dir),
                supplied_directory=str(supplied_downloads_dir),
            )
    if not downloads_dir.is_dir():
        return fail("downloads_dir_missing", f"Downloads directory does not exist: {downloads_dir}", 3)
    expected_filename = state["expected_filename"]
    if args.expected_filename:
        try:
            supplied = _safe_pdf_filename(args.expected_filename)
        except ValueError as exc:
            return fail("invalid_expected_filename", str(exc), 3)
        if supplied != expected_filename:
            return fail(
                "expected_filename_mismatch",
                "The finalize filename does not match the pre-download snapshot",
                3,
                snapshot_filename=expected_filename,
                supplied_filename=supplied,
            )
    try:
        source = wait_for_exact_pdf(
            downloads_dir / expected_filename,
            created_at_ns=int(state["created_at_ns"]),
            timeout=args.timeout,
            poll_interval=args.poll_interval,
            stable_seconds=args.stable_seconds,
        )
    except PaperFetchError as exc:
        return fail(exc.code, exc.message, 3, **exc.details)
    if source is None:
        return fail(
            "download_timeout",
            "The exact expected PDF did not complete before timeout",
            2,
            expected_path=str(downloads_dir / expected_filename),
        )
    try:
        doi = normalize_doi(args.doi)
    except PaperFetchError as exc:
        return fail(exc.code, exc.message, 3, **exc.details)
    try:
        size, digest = validate_pdf(source, args.max_bytes)
    except PaperFetchError as exc:
        return fail(exc.code, exc.message, 4, **exc.details)

    metadata = PaperMetadata(title=args.title, author=args.author, year=args.year, journal=args.journal)
    if args.filename:
        try:
            final_filename = _safe_pdf_filename(args.filename)
        except ValueError as exc:
            return fail("invalid_filename", str(exc), 3)
    else:
        final_filename = filename_for(metadata, doi)

    browser_original_file = source
    desired_browser_path = source.parent / final_filename
    if desired_browser_path != source:
        destination_in_downloads = choose_available_path(source.parent, final_filename)
        try:
            os.replace(source, destination_in_downloads)
        except OSError as exc:
            return fail("rename_failed", str(exc), 4, source=str(source), destination=str(destination_in_downloads))
        source = destination_in_downloads
        try:
            renamed_size, renamed_digest = validate_pdf(source, args.max_bytes)
        except PaperFetchError as exc:
            return fail(exc.code, exc.message, 4, **exc.details)
        if (renamed_size, renamed_digest) != (size, digest):
            return fail("rename_verification_failed", "Renamed file does not match the exact browser download", 4)

    copied = False
    duplicate = False
    destination = source
    if args.output_dir:
        output_dir = Path(args.output_dir).expanduser().resolve()
        try:
            output_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            return fail("output_dir_error", str(exc), 4, output_dir=str(output_dir))
        if output_dir == source.parent:
            destination = source
        else:
            preferred = output_dir / final_filename
            existing = verify_existing(preferred, doi)
            if existing and existing.get("sha256") == digest:
                destination = preferred
                duplicate = True
            else:
                destination = choose_available_path(output_dir, final_filename)
                try:
                    copied_size, copied_digest = _atomic_copy(source, destination, args.max_bytes)
                except (OSError, PaperFetchError) as exc:
                    return fail("copy_failed", str(exc), 4, source=str(source), destination=str(destination))
                if (copied_size, copied_digest) != (size, digest):
                    destination.unlink(missing_ok=True)
                    return fail("copy_verification_failed", "Copied PDF does not match the browser download", 4)
                copied = True

    if duplicate:
        existing_manifest = verify_existing(destination, doi)
        assert existing_manifest is not None
        emit(
            {
                "ok": True,
                "data": {**existing_manifest, "duplicate": True, "browser_source_file": str(source)},
                "manifest": str(manifest_path(destination)),
            }
        )
        return 0

    candidate = Candidate(
        "publisher_browser",
        args.source_url or f"https://doi.org/{doi}",
        detail={"download_id": args.download_id} if args.download_id else {},
    )
    manifest = build_manifest(
        doi=doi,
        candidate=candidate,
        metadata=metadata,
        path=destination,
        size=size,
        digest=digest,
        access_mode="current-browser-session",
        extra={
            "browser_original_file": str(browser_original_file),
            "browser_source_file": str(source),
            "copied": copied,
            "duplicate": duplicate,
        },
    )
    try:
        atomic_write_json(manifest_path(destination), manifest)
    except OSError as exc:
        if copied:
            destination.unlink(missing_ok=True)
        return fail("manifest_write_error", str(exc), 4, manifest=str(manifest_path(destination)))
    emit({"ok": True, "data": manifest, "manifest": str(manifest_path(destination))})
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    snapshot_parser = subparsers.add_parser("snapshot")
    snapshot_parser.add_argument("--downloads-dir", default=str(Path.home() / "Downloads"))
    snapshot_parser.add_argument("--expected-filename", required=True)
    snapshot_parser.add_argument("--output", required=True)

    plan_parser = subparsers.add_parser("plan-save")
    plan_parser.add_argument("--downloads-dir", default=str(Path.home() / "Downloads"))
    plan_parser.add_argument("--doi")
    plan_parser.add_argument("--title")
    plan_parser.add_argument("--author")
    plan_parser.add_argument("--year")
    plan_parser.add_argument("--filename")

    finalize_parser = subparsers.add_parser("finalize")
    finalize_parser.add_argument("--snapshot", required=True)
    finalize_parser.add_argument("--downloads-dir")
    finalize_parser.add_argument("--expected-filename")
    finalize_parser.add_argument("--output-dir")
    finalize_parser.add_argument("--doi", required=True)
    finalize_parser.add_argument("--title")
    finalize_parser.add_argument("--author")
    finalize_parser.add_argument("--year")
    finalize_parser.add_argument("--journal")
    finalize_parser.add_argument("--source-url")
    finalize_parser.add_argument("--download-id")
    finalize_parser.add_argument("--filename")
    finalize_parser.add_argument("--timeout", type=float, default=180.0)
    finalize_parser.add_argument("--poll-interval", type=float, default=1.0)
    finalize_parser.add_argument("--stable-seconds", type=float, default=2.0)
    finalize_parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.command == "snapshot":
        return snapshot(
            Path(args.downloads_dir).expanduser().resolve(),
            args.expected_filename,
            Path(args.output).expanduser().resolve(),
        )
    if args.command == "plan-save":
        return plan_save(args)
    if args.timeout <= 0 or args.poll_interval <= 0 or args.stable_seconds < 0 or args.max_bytes <= 0:
        return fail("invalid_arguments", "Timeout, polling, stability, and size values must be positive", 3)
    return finalize(args)


if __name__ == "__main__":
    sys.exit(main())
