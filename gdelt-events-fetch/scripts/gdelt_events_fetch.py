#!/usr/bin/env python3
"""Fetch GDELT 2.0 events export files with retries, throttling, and rich logs."""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import re
import sys
import time
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "GDELT_EVENTS_BASE_URL"
ENV_TIMEOUT_SECONDS = "GDELT_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "GDELT_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "GDELT_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "GDELT_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "GDELT_MIN_REQUEST_INTERVAL_SECONDS"
ENV_MAX_FILES_PER_RUN = "GDELT_MAX_FILES_PER_RUN"
ENV_USER_AGENT = "GDELT_USER_AGENT"

DEFAULT_BASE_URL = "http://data.gdeltproject.org/gdeltv2"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.4
DEFAULT_MAX_FILES_PER_RUN = 20
DEFAULT_USER_AGENT = "gdelt-events-fetch/1.0"

LASTUPDATE_PATH = "lastupdate.txt"
MASTERFILELIST_PATH = "masterfilelist.txt"
EVENT_URL_PATTERN = re.compile(r"/(?P<ts>\d{14})\.export\.CSV\.zip$")
TS_FORMAT = "%Y%m%d%H%M%S"
TS_FORMAT_HELP = "YYYYMMDDHHMMSS"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
DEFAULT_EVENT_EXPECTED_COLUMNS = 61
DEFAULT_MAX_VALIDATION_ISSUES = 20


@dataclass(frozen=True)
class ExportFileEntry:
    timestamp: datetime
    timestamp_raw: str
    url: str
    md5: str
    size_bytes: int | None


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    max_files_per_run: int
    user_agent: str


def env_or_default(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value or default


def parse_positive_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {value}")
    return value


def parse_non_negative_int(name: str, raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got: {value}")
    return value


def parse_positive_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got: {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got: {value}")
    return value


def parse_non_negative_float(name: str, raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number, got: {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got: {value}")
    return value


def normalize_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized}")
    return normalized


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    base_url = normalize_base_url(
        args.base_url if args.base_url else env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL)
    )
    timeout_seconds = parse_positive_int(
        "--timeout-seconds",
        str(
            args.timeout_seconds
            if args.timeout_seconds is not None
            else env_or_default(ENV_TIMEOUT_SECONDS, str(DEFAULT_TIMEOUT_SECONDS))
        ),
    )
    max_retries = parse_non_negative_int(
        "--max-retries",
        str(
            args.max_retries
            if args.max_retries is not None
            else env_or_default(ENV_MAX_RETRIES, str(DEFAULT_MAX_RETRIES))
        ),
    )
    retry_backoff_seconds = parse_positive_float(
        "--retry-backoff-seconds",
        str(
            args.retry_backoff_seconds
            if args.retry_backoff_seconds is not None
            else env_or_default(ENV_RETRY_BACKOFF_SECONDS, str(DEFAULT_RETRY_BACKOFF_SECONDS))
        ),
    )
    retry_backoff_multiplier = parse_positive_float(
        "--retry-backoff-multiplier",
        str(
            args.retry_backoff_multiplier
            if args.retry_backoff_multiplier is not None
            else env_or_default(
                ENV_RETRY_BACKOFF_MULTIPLIER, str(DEFAULT_RETRY_BACKOFF_MULTIPLIER)
            )
        ),
    )
    min_request_interval_seconds = parse_non_negative_float(
        "--min-request-interval-seconds",
        str(
            args.min_request_interval_seconds
            if args.min_request_interval_seconds is not None
            else env_or_default(
                ENV_MIN_REQUEST_INTERVAL_SECONDS, str(DEFAULT_MIN_REQUEST_INTERVAL_SECONDS)
            )
        ),
    )
    max_files_per_run = parse_positive_int(
        "--max-files-per-run",
        str(
            args.max_files_per_run
            if args.max_files_per_run is not None
            else env_or_default(ENV_MAX_FILES_PER_RUN, str(DEFAULT_MAX_FILES_PER_RUN))
        ),
    )
    user_agent = (
        args.user_agent
        if args.user_agent is not None
        else env_or_default(ENV_USER_AGENT, DEFAULT_USER_AGENT)
    ).strip()
    if not user_agent:
        raise ValueError("User-Agent cannot be empty.")

    return RuntimeConfig(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_backoff_multiplier=retry_backoff_multiplier,
        min_request_interval_seconds=min_request_interval_seconds,
        max_files_per_run=max_files_per_run,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("gdelt-events-fetch")
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file.strip():
        log_path = Path(log_file).expanduser().resolve()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    return logger


class RetryableHttpClient:
    def __init__(self, config: RuntimeConfig, logger: logging.Logger) -> None:
        self._cfg = config
        self._logger = logger
        self._last_request_monotonic: float | None = None

    def _throttle(self) -> None:
        if self._last_request_monotonic is None:
            return
        gap = time.monotonic() - self._last_request_monotonic
        sleep_seconds = self._cfg.min_request_interval_seconds - gap
        if sleep_seconds > 0:
            self._logger.debug("throttle-sleep=%.3fs", sleep_seconds)
            time.sleep(sleep_seconds)

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if not value:
            return None
        value = value.strip()
        if not value:
            return None
        try:
            parsed = float(value)
        except ValueError:
            return None
        return parsed if parsed >= 0 else None

    def get_bytes(self, url: str) -> tuple[bytes, dict[str, str]]:
        attempts = self._cfg.max_retries + 1

        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("User-Agent", self._cfg.user_agent)
            req.add_header("Accept", "*/*")

            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    payload = resp.read()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    self._last_request_monotonic = time.monotonic()
                    self._logger.info(
                        "http-ok status=%s bytes=%d url=%s",
                        getattr(resp, "status", "unknown"),
                        len(payload),
                        url,
                    )
                    return payload, headers
            except HTTPError as exc:
                self._last_request_monotonic = time.monotonic()
                status = int(exc.code)
                body = exc.read().decode("utf-8", errors="replace").strip()
                body_excerpt = body[:300]
                retriable = status in RETRIABLE_HTTP_CODES
                retry_after = self._parse_retry_after(exc.headers.get("Retry-After"))
                if retriable and attempt < attempts:
                    delay = (
                        retry_after
                        if retry_after is not None
                        else self._cfg.retry_backoff_seconds
                        * (self._cfg.retry_backoff_multiplier ** (attempt - 1))
                    )
                    self._logger.warning(
                        "http-retry status=%d delay=%.2fs attempt=%d/%d url=%s body=%s",
                        status,
                        delay,
                        attempt,
                        attempts,
                        url,
                        body_excerpt,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(
                    f"HTTP {status} for {url}. body_excerpt={body_excerpt!r}"
                ) from exc
            except (URLError, TimeoutError) as exc:
                self._last_request_monotonic = time.monotonic()
                if attempt < attempts:
                    delay = self._cfg.retry_backoff_seconds * (
                        self._cfg.retry_backoff_multiplier ** (attempt - 1)
                    )
                    self._logger.warning(
                        "network-retry delay=%.2fs attempt=%d/%d url=%s err=%s",
                        delay,
                        attempt,
                        attempts,
                        url,
                        exc,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Request failed for {url}: {exc}") from exc

        raise RuntimeError(f"Failed to fetch after retries: {url}")

    def get_text(self, url: str) -> str:
        content, _ = self.get_bytes(url)
        return content.decode("utf-8", errors="replace")


def parse_timestamp(raw: str) -> datetime:
    try:
        parsed = datetime.strptime(raw, TS_FORMAT)
    except ValueError as exc:
        raise ValueError(
            f"Invalid timestamp {raw!r}. Use UTC format {TS_FORMAT} (YYYYMMDDHHMMSS)."
        ) from exc
    return parsed.replace(tzinfo=timezone.utc)


def parse_filelist_line(line: str, logger: logging.Logger) -> tuple[int | None, str, str] | None:
    parts = line.strip().split()
    if len(parts) != 3:
        if line.strip():
            logger.debug("skip-bad-line=%s", line.strip()[:160])
        return None
    size_raw, md5, url = parts
    size = None
    try:
        size = int(size_raw)
    except ValueError:
        logger.debug("invalid-size size=%s line=%s", size_raw, line.strip()[:160])
    return size, md5, url


def as_event_entry(size: int | None, md5: str, url: str) -> ExportFileEntry | None:
    match = EVENT_URL_PATTERN.search(url)
    if not match:
        return None
    ts_raw = match.group("ts")
    return ExportFileEntry(
        timestamp=parse_timestamp(ts_raw),
        timestamp_raw=ts_raw,
        url=url,
        md5=md5,
        size_bytes=size,
    )


def parse_lastupdate_events(text: str, logger: logging.Logger) -> list[ExportFileEntry]:
    results: list[ExportFileEntry] = []
    for line in text.splitlines():
        parsed = parse_filelist_line(line, logger)
        if not parsed:
            continue
        size, md5, url = parsed
        entry = as_event_entry(size=size, md5=md5, url=url)
        if entry is not None:
            results.append(entry)
    return sorted(results, key=lambda x: x.timestamp)


def iter_masterfile_events(
    text: str, start: datetime, end: datetime, logger: logging.Logger
) -> Iterable[ExportFileEntry]:
    scanned = 0
    matched = 0
    for line in text.splitlines():
        scanned += 1
        parsed = parse_filelist_line(line, logger)
        if not parsed:
            continue
        size, md5, url = parsed
        entry = as_event_entry(size=size, md5=md5, url=url)
        if entry is None:
            continue
        if start <= entry.timestamp <= end:
            matched += 1
            yield entry
        if scanned % 100000 == 0:
            logger.info("masterfile-progress scanned=%d matched=%d", scanned, matched)
    logger.info("masterfile-finished scanned=%d matched=%d", scanned, matched)


def lastupdate_url(config: RuntimeConfig) -> str:
    return f"{config.base_url}/{LASTUPDATE_PATH}"


def masterfilelist_url(config: RuntimeConfig) -> str:
    return f"{config.base_url}/{MASTERFILELIST_PATH}"


def preview_zip_lines(raw: bytes, limit: int) -> tuple[str | None, list[str]]:
    if limit <= 0:
        return None, []
    with zipfile.ZipFile(io.BytesIO(raw), mode="r") as zipped:
        members = [item for item in zipped.namelist() if not item.endswith("/")]
        if not members:
            return None, []
        first_member = members[0]
        preview: list[str] = []
        with zipped.open(first_member, mode="r") as handle:
            for _ in range(limit):
                line = handle.readline()
                if not line:
                    break
                preview.append(line.decode("utf-8", errors="replace").rstrip("\n"))
        return first_member, preview


def validate_zip_event_payload(
    *,
    payload: bytes,
    expected_columns: int,
    max_lines: int,
    max_issues: int,
) -> dict[str, Any]:
    scan_complete = True
    scanned_lines = 0
    empty_line_count = 0
    decode_error_count = 0
    column_mismatch_count = 0
    issues: list[dict[str, Any]] = []

    with zipfile.ZipFile(io.BytesIO(payload), mode="r") as zipped:
        bad_member = zipped.testzip()
        if bad_member is not None:
            raise RuntimeError(f"ZIP CRC check failed for member {bad_member!r}.")

        members = [item for item in zipped.namelist() if not item.endswith("/")]
        if not members:
            raise RuntimeError("ZIP payload has no file members.")
        member = members[0]

        with zipped.open(member, mode="r") as handle:
            for raw_line in handle:
                if max_lines > 0 and scanned_lines >= max_lines:
                    scan_complete = False
                    break
                scanned_lines += 1

                line_bytes = raw_line.rstrip(b"\r\n")
                if not line_bytes:
                    empty_line_count += 1
                    continue

                try:
                    text = line_bytes.decode("utf-8", errors="strict")
                except UnicodeDecodeError as exc:
                    decode_error_count += 1
                    if len(issues) < max_issues:
                        issues.append(
                            {
                                "line_number": scanned_lines,
                                "reason": "decode_error",
                                "error": str(exc),
                                "line_hex_prefix": line_bytes[:64].hex(),
                            }
                        )
                    continue

                column_count = text.count("\t") + 1
                if column_count != expected_columns:
                    column_mismatch_count += 1
                    if len(issues) < max_issues:
                        issues.append(
                            {
                                "line_number": scanned_lines,
                                "reason": "column_mismatch",
                                "column_count": column_count,
                                "expected_columns": expected_columns,
                                "line_excerpt": text[:240],
                            }
                        )

    issue_count = decode_error_count + column_mismatch_count
    return {
        "passed": issue_count == 0,
        "checked_member": member,
        "expected_columns": expected_columns,
        "scanned_lines": scanned_lines,
        "scan_complete": scan_complete,
        "max_lines": max_lines,
        "empty_line_count": empty_line_count,
        "issue_count": issue_count,
        "decode_error_count": decode_error_count,
        "column_mismatch_count": column_mismatch_count,
        "issues": issues,
    }


def write_quarantine_issues(
    *,
    quarantine_dir: Path,
    source_filename: str,
    issues: list[dict[str, Any]],
) -> Path | None:
    if not issues:
        return None
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    output_path = quarantine_dir / f"{source_filename}.bad-lines.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for item in issues:
            handle.write(json.dumps(item, ensure_ascii=False))
            handle.write("\n")
    return output_path


def serialize_entry(entry: ExportFileEntry) -> dict[str, Any]:
    return {
        "timestamp_utc": entry.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "timestamp_raw": entry.timestamp_raw,
        "url": entry.url,
        "md5": entry.md5,
        "size_bytes": entry.size_bytes,
    }


def validate_max_files(max_files: int, config: RuntimeConfig) -> None:
    if max_files < 1:
        raise ValueError("--max-files must be >= 1.")
    if max_files > config.max_files_per_run:
        raise ValueError(
            f"--max-files={max_files} exceeds configured cap {config.max_files_per_run} "
            f"(set by --max-files-per-run or {ENV_MAX_FILES_PER_RUN})."
        )


def resolve_latest_entries(
    *,
    max_files: int,
    config: RuntimeConfig,
    client: RetryableHttpClient,
    logger: logging.Logger,
) -> list[ExportFileEntry]:
    text = client.get_text(lastupdate_url(config))
    entries = parse_lastupdate_events(text=text, logger=logger)
    if not entries:
        raise RuntimeError("No events export entry found in lastupdate.txt.")
    selected = entries[-max_files:]
    logger.info("selected-latest count=%d", len(selected))
    return selected


def save_bytes(content: bytes, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_bytes(content)


def download_entries(
    *,
    entries: list[ExportFileEntry],
    output_dir: Path,
    overwrite: bool,
    preview_lines: int,
    validate_structure: bool,
    expected_columns: int,
    validation_max_lines: int,
    max_validation_issues: int,
    fail_on_structure_error: bool,
    quarantine_dir: Path | None,
    client: RetryableHttpClient,
    logger: logging.Logger,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    downloads: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for item in entries:
        filename = Path(parse.urlparse(item.url).path).name
        output_path = output_dir / filename
        if output_path.exists() and not overwrite:
            logger.info("skip-existing path=%s", output_path)
            skipped.append(
                {
                    "path": str(output_path),
                    "reason": "exists",
                    "entry": serialize_entry(item),
                }
            )
            continue

        payload, headers = client.get_bytes(item.url)
        save_bytes(payload, output_path)

        validation: dict[str, Any] | None = None
        if validate_structure:
            validation = validate_zip_event_payload(
                payload=payload,
                expected_columns=expected_columns,
                max_lines=validation_max_lines,
                max_issues=max_validation_issues,
            )

            if validation["issue_count"] > 0:
                logger.warning(
                    "structure-validation-issues path=%s issues=%d decode_errors=%d column_mismatch=%d scan_complete=%s",
                    output_path,
                    validation["issue_count"],
                    validation["decode_error_count"],
                    validation["column_mismatch_count"],
                    validation["scan_complete"],
                )
                if quarantine_dir is not None:
                    quarantine_path = write_quarantine_issues(
                        quarantine_dir=quarantine_dir,
                        source_filename=filename,
                        issues=validation["issues"],
                    )
                    validation["quarantine_path"] = str(quarantine_path) if quarantine_path else None
                else:
                    validation["quarantine_path"] = None

                if fail_on_structure_error:
                    raise RuntimeError(
                        f"Structure validation failed for {output_path} "
                        f"(issues={validation['issue_count']})."
                    )
            else:
                validation["quarantine_path"] = None
                logger.info(
                    "structure-validation-ok path=%s scanned_lines=%d scan_complete=%s",
                    output_path,
                    validation["scanned_lines"],
                    validation["scan_complete"],
                )

        preview_member, line_preview = preview_zip_lines(payload, limit=preview_lines)
        downloads.append(
            {
                "entry": serialize_entry(item),
                "request_url": item.url,
                "output_path": str(output_path),
                "bytes_written": len(payload),
                "content_type": headers.get("content-type"),
                "preview_member": preview_member,
                "preview_lines": line_preview,
                "validation": validation,
            }
        )
        logger.info(
            "downloaded path=%s bytes=%d preview_lines=%d",
            output_path,
            len(payload),
            len(line_preview),
        )
    return downloads, skipped


def print_json(payload: dict[str, Any], pretty: bool) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
    )


def command_check_config(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    payload = {
        "ok": True,
        "config": {
            "base_url": config.base_url,
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "retry_backoff_multiplier": config.retry_backoff_multiplier,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "max_files_per_run": config.max_files_per_run,
            "user_agent": config.user_agent,
        },
        "source_urls": {
            "lastupdate": lastupdate_url(config),
            "masterfilelist": masterfilelist_url(config),
        },
        "env_keys": {
            "base_url": ENV_BASE_URL,
            "timeout_seconds": ENV_TIMEOUT_SECONDS,
            "max_retries": ENV_MAX_RETRIES,
            "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
            "retry_backoff_multiplier": ENV_RETRY_BACKOFF_MULTIPLIER,
            "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
            "max_files_per_run": ENV_MAX_FILES_PER_RUN,
            "user_agent": ENV_USER_AGENT,
        },
    }
    print_json(payload, pretty=args.pretty)
    return 0


def command_resolve_latest(args: argparse.Namespace) -> int:
    logger = build_logger(level=args.log_level, log_file=args.log_file)
    config = build_runtime_config(args)
    client = RetryableHttpClient(config=config, logger=logger)

    text = client.get_text(lastupdate_url(config))
    entries = parse_lastupdate_events(text=text, logger=logger)
    if not entries:
        raise RuntimeError("No events export entry found in lastupdate.txt.")

    latest = entries[-1]
    payload = {
        "ok": True,
        "source": "lastupdate",
        "latest_event_export": serialize_entry(latest),
        "candidate_count": len(entries),
    }
    print_json(payload, pretty=args.pretty)
    return 0


def select_entries(args: argparse.Namespace, config: RuntimeConfig, logger: logging.Logger) -> list[ExportFileEntry]:
    client = RetryableHttpClient(config=config, logger=logger)
    validate_max_files(max_files=args.max_files, config=config)

    if args.mode == "latest":
        return resolve_latest_entries(
            max_files=args.max_files,
            config=config,
            client=client,
            logger=logger,
        )

    if args.mode != "range":
        raise ValueError(f"Unsupported mode: {args.mode}")

    if not args.start_datetime or not args.end_datetime:
        raise ValueError("--start-datetime and --end-datetime are required in range mode.")

    start = parse_timestamp(args.start_datetime)
    end = parse_timestamp(args.end_datetime)
    if end < start:
        raise ValueError("--end-datetime must be >= --start-datetime.")

    text = client.get_text(masterfilelist_url(config))
    candidates = sorted(
        iter_masterfile_events(text=text, start=start, end=end, logger=logger),
        key=lambda x: x.timestamp,
    )
    selected = candidates[: args.max_files]
    logger.info(
        "selected-range candidates=%d selected=%d start=%s end=%s",
        len(candidates),
        len(selected),
        args.start_datetime,
        args.end_datetime,
    )
    return selected


def command_fetch(args: argparse.Namespace) -> int:
    logger = build_logger(level=args.log_level, log_file=args.log_file)
    config = build_runtime_config(args)
    if args.expected_columns < 1:
        raise ValueError("--expected-columns must be >= 1.")
    if args.validation_max_lines < 0:
        raise ValueError("--validation-max-lines must be >= 0.")
    if args.max_validation_issues < 1:
        raise ValueError("--max-validation-issues must be >= 1.")

    selected = select_entries(args=args, config=config, logger=logger)
    if not selected:
        raise RuntimeError("No matching export files found for the given filters.")

    if args.dry_run:
        payload = {
            "ok": True,
            "dry_run": True,
            "mode": args.mode,
            "selected_count": len(selected),
            "files": [serialize_entry(item) for item in selected],
        }
        print_json(payload, pretty=args.pretty)
        return 0

    out_dir = Path(args.output_dir).expanduser().resolve()
    logger.info("output-dir=%s", out_dir)

    quarantine_dir = None
    if args.quarantine_dir.strip():
        quarantine_dir = Path(args.quarantine_dir).expanduser().resolve()

    client = RetryableHttpClient(config=config, logger=logger)
    downloads, skipped = download_entries(
        entries=selected,
        output_dir=out_dir,
        overwrite=args.overwrite,
        preview_lines=args.preview_lines,
        validate_structure=args.validate_structure,
        expected_columns=args.expected_columns,
        validation_max_lines=args.validation_max_lines,
        max_validation_issues=args.max_validation_issues,
        fail_on_structure_error=args.fail_on_structure_error,
        quarantine_dir=quarantine_dir,
        client=client,
        logger=logger,
    )

    result = {
        "ok": True,
        "mode": args.mode,
        "selected_count": len(selected),
        "downloaded_count": len(downloads),
        "skipped_count": len(skipped),
        "downloads": downloads,
        "skipped": skipped,
    }
    print_json(result, pretty=args.pretty)
    return 0


def add_runtime_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default="", help=f"Override base URL. Default: {DEFAULT_BASE_URL}")
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help=f"HTTP timeout in seconds. Env: {ENV_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help=f"Retry count for transient errors. Env: {ENV_MAX_RETRIES}.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=None,
        help=f"Initial backoff seconds before retries. Env: {ENV_RETRY_BACKOFF_SECONDS}.",
    )
    parser.add_argument(
        "--retry-backoff-multiplier",
        type=float,
        default=None,
        help=f"Backoff multiplier between retry attempts. Env: {ENV_RETRY_BACKOFF_MULTIPLIER}.",
    )
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=None,
        help=f"Minimum interval between requests. Env: {ENV_MIN_REQUEST_INTERVAL_SECONDS}.",
    )
    parser.add_argument(
        "--max-files-per-run",
        type=int,
        default=None,
        help=f"Safety cap for --max-files. Env: {ENV_MAX_FILES_PER_RUN}.",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help=f"HTTP User-Agent header. Env: {ENV_USER_AGENT}.",
    )


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Log verbosity written to stderr/log-file.",
    )
    parser.add_argument(
        "--log-file",
        default="",
        help="Optional log file path. When set, append logs to this file.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch GDELT 2.0 events export files via lastupdate/masterfilelist."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Show effective runtime config and source URLs.")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    latest = sub.add_parser("resolve-latest", help="Resolve the latest events export file.")
    add_runtime_config_args(latest)
    add_logging_args(latest)
    latest.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch = sub.add_parser("fetch", help="Download events export file(s).")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument(
        "--mode",
        choices=["latest", "range"],
        default="latest",
        help="latest: use lastupdate.txt; range: filter masterfilelist by datetime range.",
    )
    fetch.add_argument(
        "--start-datetime",
        default="",
        help=f"UTC start timestamp for range mode, format {TS_FORMAT_HELP}.",
    )
    fetch.add_argument(
        "--end-datetime",
        default="",
        help=f"UTC end timestamp for range mode, format {TS_FORMAT_HELP}.",
    )
    fetch.add_argument(
        "--max-files",
        type=int,
        default=1,
        help="Maximum number of files selected for this fetch command.",
    )
    fetch.add_argument(
        "--output-dir",
        default="./data/gdelt-events",
        help="Directory for downloaded .export.CSV.zip files.",
    )
    fetch.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite files when output path already exists.",
    )
    fetch.add_argument(
        "--preview-lines",
        type=int,
        default=0,
        help="Read this many decompressed lines from each downloaded ZIP payload for quick inspection.",
    )
    fetch.add_argument(
        "--validate-structure",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Validate ZIP integrity, UTF-8 decoding, and column counts after download.",
    )
    fetch.add_argument(
        "--expected-columns",
        type=int,
        default=DEFAULT_EVENT_EXPECTED_COLUMNS,
        help=f"Expected tab-separated column count for events rows. Default: {DEFAULT_EVENT_EXPECTED_COLUMNS}.",
    )
    fetch.add_argument(
        "--validation-max-lines",
        type=int,
        default=0,
        help="Maximum rows to validate per file (0 means full scan).",
    )
    fetch.add_argument(
        "--max-validation-issues",
        type=int,
        default=DEFAULT_MAX_VALIDATION_ISSUES,
        help=f"Maximum number of issue records kept in output/quarantine. Default: {DEFAULT_MAX_VALIDATION_ISSUES}.",
    )
    fetch.add_argument(
        "--fail-on-structure-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail command when structure validation finds decode/column issues.",
    )
    fetch.add_argument(
        "--quarantine-dir",
        default="",
        help="Optional directory to save bad-line issue JSONL files.",
    )
    fetch.add_argument("--dry-run", action="store_true", help="Resolve files without downloading.")
    fetch.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "check-config":
            return command_check_config(args)
        if args.command == "resolve-latest":
            return command_resolve_latest(args)
        if args.command == "fetch":
            return command_fetch(args)
        raise ValueError(f"Unknown command: {args.command}")
    except KeyboardInterrupt:
        print("[ERROR] Interrupted by user.", file=sys.stderr)
        return 130
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
