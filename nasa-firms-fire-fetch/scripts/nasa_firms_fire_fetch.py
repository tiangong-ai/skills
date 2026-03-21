#!/usr/bin/env python3
"""Fetch NASA FIRMS active fire detections with validation and logs."""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import math
import os
import re
import sys
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "NASA_FIRMS_BASE_URL"
ENV_MAP_KEY = "NASA_FIRMS_MAP_KEY"
ENV_TIMEOUT_SECONDS = "NASA_FIRMS_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "NASA_FIRMS_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "NASA_FIRMS_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "NASA_FIRMS_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "NASA_FIRMS_MIN_REQUEST_INTERVAL_SECONDS"
ENV_MAX_DAYS_PER_RUN = "NASA_FIRMS_MAX_DAYS_PER_RUN"
ENV_MAX_CHUNK_DAYS = "NASA_FIRMS_MAX_CHUNK_DAYS"
ENV_MAX_ESTIMATED_TRANSACTIONS_PER_RUN = "NASA_FIRMS_MAX_ESTIMATED_TRANSACTIONS_PER_RUN"
ENV_MAX_RETRY_AFTER_SECONDS = "NASA_FIRMS_MAX_RETRY_AFTER_SECONDS"
ENV_ENABLE_AVAILABILITY_PROBE = "NASA_FIRMS_ENABLE_AVAILABILITY_PROBE"
ENV_USER_AGENT = "NASA_FIRMS_USER_AGENT"

DEFAULT_BASE_URL = "https://firms.modaps.eosdis.nasa.gov"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 2.0
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.5
DEFAULT_MAX_DAYS_PER_RUN = 31
DEFAULT_MAX_CHUNK_DAYS = 5
DEFAULT_MAX_ESTIMATED_TRANSACTIONS_PER_RUN = 250
DEFAULT_MAX_RETRY_AFTER_SECONDS = 300
DEFAULT_ENABLE_AVAILABILITY_PROBE = False
DEFAULT_USER_AGENT = "nasa-firms-fire-fetch/1.0"

RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
DATE_FORMAT = "%Y-%m-%d"
FIRMS_MAX_DAY_RANGE = 5
DEFAULT_MAX_VALIDATION_ISSUES = 50
MAP_KEY_PATTERN = re.compile(r"^[0-9A-Za-z]{32}$")
SOURCE_CHOICES = (
    "LANDSAT_NRT",
    "MODIS_NRT",
    "MODIS_SP",
    "VIIRS_NOAA20_NRT",
    "VIIRS_NOAA20_SP",
    "VIIRS_NOAA21_NRT",
    "VIIRS_SNPP_NRT",
    "VIIRS_SNPP_SP",
)
PROBE_SOURCE_CHOICES = SOURCE_CHOICES + ("ALL",)
REQUIRED_FIRE_COLUMNS = ("latitude", "longitude", "acq_date", "acq_time")


@dataclass(frozen=True)
class BoundingBox:
    west: float
    south: float
    east: float
    north: float


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    map_key: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    max_days_per_run: int
    max_chunk_days: int
    max_estimated_transactions_per_run: int
    max_retry_after_seconds: int
    enable_availability_probe: bool
    user_agent: str


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    payload_bytes: bytes
    byte_length: int


@dataclass(frozen=True)
class CsvTable:
    header: list[str]
    records: list[dict[str, str]]
    row_count: int


@dataclass(frozen=True)
class ChunkPlan:
    index: int
    start_date: date
    end_date: date
    day_count: int
    estimated_transactions: int
    request_url: str


@dataclass
class IssueCollector:
    max_issues: int
    total_count: int = 0
    issues: list[dict[str, Any]] = field(default_factory=list)

    def add(self, issue: dict[str, Any]) -> None:
        self.total_count += 1
        if len(self.issues) < self.max_issues:
            self.issues.append(issue)


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


def parse_bool(name: str, raw: str) -> bool:
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value, got: {raw!r}")


def normalize_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized!r}")
    return normalized


def normalize_map_key(value: str, *, required: bool) -> str:
    normalized = value.strip()
    if not normalized:
        if required:
            raise ValueError("NASA FIRMS MAP_KEY is required.")
        return ""
    if not MAP_KEY_PATTERN.fullmatch(normalized):
        raise ValueError("MAP_KEY must be a 32-character alphanumeric token.")
    return normalized


def normalize_source(value: str, *, field_name: str = "Source", allow_all: bool = False) -> str:
    normalized = value.strip().upper()
    allowed = PROBE_SOURCE_CHOICES if allow_all else SOURCE_CHOICES
    if normalized not in allowed:
        raise ValueError(
            f"{field_name} must be one of {', '.join(allowed)}, got: {value!r}"
        )
    return normalized


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def parse_date_arg(name: str, raw: str) -> date:
    value = raw.strip()
    if not value:
        raise ValueError(f"{name} cannot be empty.")
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD, got: {raw!r}") from exc


def parse_optional_date(raw: str) -> date | None:
    value = raw.strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, DATE_FORMAT).date()
    except ValueError:
        return None


def parse_bbox(raw: str) -> BoundingBox:
    parts = [item.strip() for item in raw.split(",")]
    if len(parts) != 4:
        raise ValueError(
            f"Invalid --bbox value {raw!r}. Use west,south,east,north format."
        )

    try:
        west = float(parts[0])
        south = float(parts[1])
        east = float(parts[2])
        north = float(parts[3])
    except ValueError as exc:
        raise ValueError(f"Invalid --bbox value {raw!r}. Coordinates must be numbers.") from exc

    if west < -180 or west > 180 or east < -180 or east > 180:
        raise ValueError("BBox west/east must be between -180 and 180.")
    if south < -90 or south > 90 or north < -90 or north > 90:
        raise ValueError("BBox south/north must be between -90 and 90.")
    if east <= west:
        raise ValueError("BBox east must be greater than west.")
    if north <= south:
        raise ValueError("BBox north must be greater than south.")

    return BoundingBox(west=west, south=south, east=east, north=north)


def format_float(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def bbox_to_area_string(bbox: BoundingBox) -> str:
    return ",".join(
        [
            format_float(bbox.west),
            format_float(bbox.south),
            format_float(bbox.east),
            format_float(bbox.north),
        ]
    )


def estimate_transactions_for_area(source: str, bbox: BoundingBox, day_count: int) -> int:
    src_weight = 2.0 if "VIIRS" in source else 0.5
    x_tiles = max(1, math.ceil((bbox.east - bbox.west) / 60.0))
    y_tiles = max(1, math.ceil((bbox.north - bbox.south) / 60.0))
    return math.ceil(x_tiles * y_tiles * src_weight * day_count)


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    base_url = normalize_base_url(
        args.base_url if args.base_url else env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL)
    )
    map_key = normalize_map_key(
        args.map_key if args.map_key is not None else os.environ.get(ENV_MAP_KEY, ""),
        required=False,
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
                ENV_RETRY_BACKOFF_MULTIPLIER,
                str(DEFAULT_RETRY_BACKOFF_MULTIPLIER),
            )
        ),
    )
    min_request_interval_seconds = parse_non_negative_float(
        "--min-request-interval-seconds",
        str(
            args.min_request_interval_seconds
            if args.min_request_interval_seconds is not None
            else env_or_default(
                ENV_MIN_REQUEST_INTERVAL_SECONDS,
                str(DEFAULT_MIN_REQUEST_INTERVAL_SECONDS),
            )
        ),
    )
    max_days_per_run = parse_positive_int(
        "--max-days-per-run",
        str(
            args.max_days_per_run
            if args.max_days_per_run is not None
            else env_or_default(ENV_MAX_DAYS_PER_RUN, str(DEFAULT_MAX_DAYS_PER_RUN))
        ),
    )
    max_chunk_days = parse_positive_int(
        "--max-chunk-days",
        str(
            args.max_chunk_days
            if args.max_chunk_days is not None
            else env_or_default(ENV_MAX_CHUNK_DAYS, str(DEFAULT_MAX_CHUNK_DAYS))
        ),
    )
    if max_chunk_days > FIRMS_MAX_DAY_RANGE:
        raise ValueError(
            f"--max-chunk-days cannot exceed FIRMS limit {FIRMS_MAX_DAY_RANGE}, got: {max_chunk_days}"
        )
    max_estimated_transactions_per_run = parse_positive_int(
        "--max-estimated-transactions-per-run",
        str(
            args.max_estimated_transactions_per_run
            if args.max_estimated_transactions_per_run is not None
            else env_or_default(
                ENV_MAX_ESTIMATED_TRANSACTIONS_PER_RUN,
                str(DEFAULT_MAX_ESTIMATED_TRANSACTIONS_PER_RUN),
            )
        ),
    )
    max_retry_after_seconds = parse_non_negative_int(
        "--max-retry-after-seconds",
        str(
            args.max_retry_after_seconds
            if args.max_retry_after_seconds is not None
            else env_or_default(
                ENV_MAX_RETRY_AFTER_SECONDS,
                str(DEFAULT_MAX_RETRY_AFTER_SECONDS),
            )
        ),
    )
    if args.enable_availability_probe is not None:
        enable_availability_probe = args.enable_availability_probe
    else:
        enable_availability_probe = parse_bool(
            ENV_ENABLE_AVAILABILITY_PROBE,
            env_or_default(
                ENV_ENABLE_AVAILABILITY_PROBE,
                "true" if DEFAULT_ENABLE_AVAILABILITY_PROBE else "false",
            ),
        )
    user_agent = (
        args.user_agent if args.user_agent is not None else os.environ.get(ENV_USER_AGENT, "")
    ).strip() or DEFAULT_USER_AGENT

    return RuntimeConfig(
        base_url=base_url,
        map_key=map_key,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_backoff_multiplier=retry_backoff_multiplier,
        min_request_interval_seconds=min_request_interval_seconds,
        max_days_per_run=max_days_per_run,
        max_chunk_days=max_chunk_days,
        max_estimated_transactions_per_run=max_estimated_transactions_per_run,
        max_retry_after_seconds=max_retry_after_seconds,
        enable_availability_probe=enable_availability_probe,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("nasa-firms-fire-fetch")
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


def redact_url(url: str) -> str:
    split = parse.urlsplit(url)
    path_parts = split.path.split("/")
    redacted_path = "/".join(
        "***" if MAP_KEY_PATTERN.fullmatch(part) else part for part in path_parts
    )
    query_pairs = parse.parse_qsl(split.query, keep_blank_values=True)
    redacted_pairs = []
    for key, value in query_pairs:
        if key.upper() == "MAP_KEY" and value:
            redacted_pairs.append((key, "***"))
        else:
            redacted_pairs.append((key, value))
    return parse.urlunsplit(
        (
            split.scheme,
            split.netloc,
            redacted_path,
            parse.urlencode(redacted_pairs),
            split.fragment,
        )
    )


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
        stripped = value.strip()
        if not stripped:
            return None
        try:
            numeric = float(stripped)
        except ValueError:
            try:
                dt = parsedate_to_datetime(stripped)
            except (TypeError, ValueError):
                return None
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            delay = (dt - datetime.now(timezone.utc)).total_seconds()
            return max(delay, 0.0)
        return numeric if numeric >= 0 else None

    def _compute_retry_delay(
        self,
        *,
        attempt: int,
        headers: dict[str, str] | None,
        reason: str,
    ) -> float:
        retry_after = self._parse_retry_after(headers.get("retry-after") if headers else None)
        if retry_after is not None:
            if retry_after > self._cfg.max_retry_after_seconds:
                raise RuntimeError(
                    f"Retry-After {retry_after:.3f}s exceeds configured cap "
                    f"{self._cfg.max_retry_after_seconds}s for {reason}."
                )
            return retry_after
        return self._cfg.retry_backoff_seconds * (
            self._cfg.retry_backoff_multiplier ** max(attempt - 1, 0)
        )

    @staticmethod
    def _decode_json_error(payload_bytes: bytes) -> str | None:
        try:
            text = payload_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return None
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return None

        if isinstance(payload, dict):
            for key in ("reason", "message", "detail", "error"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    def _http_error_message(
        self,
        *,
        status_code: int,
        reason: str,
        payload_bytes: bytes,
    ) -> str:
        if not payload_bytes:
            return f"HTTP {status_code}: {reason}"

        json_message = self._decode_json_error(payload_bytes)
        if json_message:
            return f"HTTP {status_code}: {json_message}"

        try:
            text = payload_bytes.decode("utf-8", errors="replace").strip()
        except Exception:  # noqa: BLE001
            text = ""
        return f"HTTP {status_code}: {text or reason}"

    def get(self, url: str, *, accept: str) -> HttpResponse:
        attempts = self._cfg.max_retries + 1

        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("Accept", accept)
            req.add_header("User-Agent", self._cfg.user_agent)
            self._logger.info(
                "http-request attempt=%d/%d url=%s",
                attempt,
                attempts,
                redact_url(url),
            )

            started = time.monotonic()

            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    payload_bytes = resp.read()
                    headers = {key.lower(): value for key, value in resp.headers.items()}
                    status_code = getattr(resp, "status", resp.getcode())
            except HTTPError as exc:
                self._last_request_monotonic = time.monotonic()
                headers = {key.lower(): value for key, value in exc.headers.items()}
                payload_bytes = exc.read()
                if exc.code in RETRIABLE_HTTP_CODES and attempt < attempts:
                    delay = self._compute_retry_delay(
                        attempt=attempt,
                        headers=headers,
                        reason=f"HTTP {exc.code}",
                    )
                    self._logger.warning(
                        "http-retry status=%s delay=%.3fs url=%s",
                        exc.code,
                        delay,
                        redact_url(url),
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(
                    self._http_error_message(
                        status_code=exc.code,
                        reason=exc.reason,
                        payload_bytes=payload_bytes,
                    )
                ) from exc
            except (URLError, TimeoutError) as exc:
                self._last_request_monotonic = time.monotonic()
                if attempt < attempts:
                    delay = self._compute_retry_delay(
                        attempt=attempt,
                        headers=None,
                        reason=type(exc).__name__,
                    )
                    self._logger.warning(
                        "network-retry error=%s delay=%.3fs url=%s",
                        exc,
                        delay,
                        redact_url(url),
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Request failed: {exc}") from exc

            self._last_request_monotonic = time.monotonic()
            elapsed_ms = (time.monotonic() - started) * 1000.0
            self._logger.info(
                "http-response status=%s bytes=%d elapsed_ms=%.3f url=%s",
                status_code,
                len(payload_bytes),
                elapsed_ms,
                redact_url(url),
            )
            return HttpResponse(
                url=url,
                status_code=status_code,
                headers=headers,
                payload_bytes=payload_bytes,
                byte_length=len(payload_bytes),
            )

        raise RuntimeError("Exhausted request attempts.")


def decode_utf8_bytes(payload_bytes: bytes) -> str:
    try:
        return payload_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise RuntimeError("Response body is not valid UTF-8.") from exc


def parse_json_payload(response: HttpResponse) -> dict[str, Any] | list[Any]:
    content_type = response.headers.get("content-type", "")
    if "json" not in content_type.lower():
        raise RuntimeError(f"Expected JSON content-type, got: {content_type!r}")
    text = decode_utf8_bytes(response.payload_bytes)
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Response body is not valid JSON.") from exc
    if not isinstance(payload, (dict, list)):
        raise RuntimeError("Response JSON must be an object or array.")
    return payload


def parse_csv_payload(response: HttpResponse, *, context: str) -> CsvTable:
    content_type = response.headers.get("content-type", "")
    lowered = content_type.lower()
    if not lowered:
        raise RuntimeError(f"{context} response is missing content-type.")
    if "html" in lowered or ("json" in lowered and "csv" not in lowered):
        raise RuntimeError(f"{context} expected CSV-like content-type, got: {content_type!r}")

    text = decode_utf8_bytes(response.payload_bytes)
    stripped = text.strip()
    if not stripped:
        raise RuntimeError(f"{context} returned an empty body.")
    if "\n" not in stripped and "," not in stripped:
        raise RuntimeError(stripped)

    reader = csv.reader(io.StringIO(text))
    header: list[str] | None = None
    records: list[dict[str, str]] = []
    row_count = 0

    for line_number, row in enumerate(reader, start=1):
        if not row or all(not cell.strip() for cell in row):
            continue
        if header is None:
            header = [cell.lstrip("\ufeff").strip() for cell in row]
            if not header or not any(header):
                raise RuntimeError(f"{context} CSV header is empty.")
            duplicates = sorted({item for item in header if header.count(item) > 1})
            if duplicates:
                raise RuntimeError(f"{context} CSV header has duplicate columns: {duplicates}")
            continue
        if len(row) != len(header):
            raise RuntimeError(
                f"{context} CSV row {line_number} has {len(row)} columns, expected {len(header)}."
            )
        row_count += 1
        records.append({header[index]: cell.strip() for index, cell in enumerate(row)})

    if header is None:
        raise RuntimeError(f"{context} CSV header is missing.")

    return CsvTable(header=header, records=records, row_count=row_count)


def write_json_file(path: Path, data: dict[str, Any], *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
        handle.write("\n")


def build_area_request_url(
    *,
    config: RuntimeConfig,
    source: str,
    bbox: BoundingBox,
    start_date_value: date,
    day_count: int,
) -> str:
    area = bbox_to_area_string(bbox)
    map_key = config.map_key or "[MAP_KEY]"
    return "/".join(
        [
            config.base_url,
            "api",
            "area",
            "csv",
            parse.quote(map_key, safe="[]"),
            parse.quote(source, safe=""),
            parse.quote(area, safe=",.-"),
            str(day_count),
            start_date_value.isoformat(),
        ]
    )


def build_data_availability_url(config: RuntimeConfig, source: str) -> str:
    map_key = config.map_key or "[MAP_KEY]"
    return "/".join(
        [
            config.base_url,
            "api",
            "data_availability",
            "csv",
            parse.quote(map_key, safe="[]"),
            parse.quote(source, safe=""),
        ]
    )


def build_map_key_status_url(config: RuntimeConfig) -> str:
    map_key = config.map_key or "[MAP_KEY]"
    return (
        f"{config.base_url}/mapserver/mapkey_status/?"
        f"{parse.urlencode({'MAP_KEY': map_key})}"
    )


def build_chunk_plan(
    *,
    config: RuntimeConfig,
    source: str,
    bbox: BoundingBox,
    start_date_value: date,
    end_date_value: date,
) -> list[ChunkPlan]:
    chunk_plans: list[ChunkPlan] = []
    cursor = start_date_value
    index = 1

    while cursor <= end_date_value:
        chunk_end = min(cursor + timedelta(days=config.max_chunk_days - 1), end_date_value)
        day_count = (chunk_end - cursor).days + 1
        estimated_transactions = estimate_transactions_for_area(source, bbox, day_count)
        request_url = build_area_request_url(
            config=config,
            source=source,
            bbox=bbox,
            start_date_value=cursor,
            day_count=day_count,
        )
        chunk_plans.append(
            ChunkPlan(
                index=index,
                start_date=cursor,
                end_date=chunk_end,
                day_count=day_count,
                estimated_transactions=estimated_transactions,
                request_url=request_url,
            )
        )
        cursor = chunk_end + timedelta(days=1)
        index += 1

    return chunk_plans


def add_issue(
    collector: IssueCollector,
    *,
    scope: str,
    message: str,
    field: str = "",
    value: Any = None,
    record_index: int | None = None,
    chunk_index: int | None = None,
) -> None:
    issue: dict[str, Any] = {"scope": scope, "message": message}
    if field:
        issue["field"] = field
    if value is not None:
        issue["value"] = value
    if record_index is not None:
        issue["record_index"] = record_index
    if chunk_index is not None:
        issue["chunk_index"] = chunk_index
    collector.add(issue)


def normalize_acq_time(raw: str) -> str | None:
    value = raw.strip()
    if not value or not value.isdigit() or len(value) > 4:
        return None
    normalized = value.zfill(4)
    hour = int(normalized[:2])
    minute = int(normalized[2:])
    if hour > 23 or minute > 59:
        return None
    return normalized


def parse_float_or_none(raw: str) -> float | None:
    value = raw.strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def normalize_fire_records(
    raw_records: list[dict[str, str]],
    *,
    chunk: ChunkPlan,
    source: str,
    start_index: int,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for offset, raw_record in enumerate(raw_records):
        record: dict[str, Any] = dict(raw_record)
        record["_record_index"] = start_index + offset
        record["_source"] = source
        record["_chunk_index"] = chunk.index
        record["_chunk_start_date"] = chunk.start_date.isoformat()
        record["_chunk_end_date"] = chunk.end_date.isoformat()

        latitude = parse_float_or_none(raw_record.get("latitude", ""))
        longitude = parse_float_or_none(raw_record.get("longitude", ""))
        if latitude is not None:
            record["_latitude"] = latitude
        if longitude is not None:
            record["_longitude"] = longitude

        acq_date_value = parse_optional_date(raw_record.get("acq_date", ""))
        if acq_date_value is not None:
            record["_acq_date"] = acq_date_value.isoformat()

        acq_time_hhmm = normalize_acq_time(raw_record.get("acq_time", ""))
        if acq_time_hhmm is not None:
            record["_acq_time_hhmm"] = acq_time_hhmm

        if acq_date_value is not None and acq_time_hhmm is not None:
            acquired_at = datetime(
                year=acq_date_value.year,
                month=acq_date_value.month,
                day=acq_date_value.day,
                hour=int(acq_time_hhmm[:2]),
                minute=int(acq_time_hhmm[2:]),
                tzinfo=timezone.utc,
            )
            record["_acquired_at_utc"] = acquired_at.isoformat().replace("+00:00", "Z")

        records.append(record)

    return records


def validate_headers(
    *,
    headers_by_chunk: list[dict[str, Any]],
    max_validation_issues: int,
) -> tuple[bool, dict[str, bool], IssueCollector]:
    collector = IssueCollector(max_issues=max_validation_issues)
    if not headers_by_chunk:
        return True, {column: False for column in REQUIRED_FIRE_COLUMNS}, collector

    first_header = headers_by_chunk[0]["header"]
    required_columns_present = {
        column: column in first_header for column in REQUIRED_FIRE_COLUMNS
    }
    duplicates = sorted({item for item in first_header if first_header.count(item) > 1})
    if duplicates:
        add_issue(
            collector,
            scope="header",
            message="Header has duplicate columns.",
            field="header",
            value=duplicates,
        )

    for column in REQUIRED_FIRE_COLUMNS:
        if not required_columns_present[column]:
            add_issue(
                collector,
                scope="header",
                message="Required fire column missing from header.",
                field=column,
            )

    header_consistent = True
    for item in headers_by_chunk[1:]:
        if item["header"] != first_header:
            header_consistent = False
            add_issue(
                collector,
                scope="header",
                message="Chunk header differs from the first chunk header.",
                field="header",
                value=item["header"],
                chunk_index=item["chunk_index"],
            )

    return header_consistent, required_columns_present, collector


def validate_records(
    *,
    records: list[dict[str, Any]],
    header: list[str],
    headers_by_chunk: list[dict[str, Any]],
    bbox: BoundingBox,
    start_date_value: date,
    end_date_value: date,
    max_validation_issues: int,
) -> dict[str, Any]:
    header_consistent, required_columns_present, header_issues = validate_headers(
        headers_by_chunk=headers_by_chunk,
        max_validation_issues=max_validation_issues,
    )
    collector = IssueCollector(max_issues=max_validation_issues)
    for issue in header_issues.issues:
        collector.add(issue)
    collector.total_count = header_issues.total_count

    normalized_count = 0
    acquired_at_count = 0

    for record in records:
        record_index = record.get("_record_index")
        chunk_index = record.get("_chunk_index")

        latitude = parse_float_or_none(str(record.get("latitude", "")))
        if latitude is None:
            add_issue(
                collector,
                scope="record",
                message="Latitude is missing or not numeric.",
                field="latitude",
                value=record.get("latitude"),
                record_index=record_index,
                chunk_index=chunk_index,
            )
        elif latitude < bbox.south or latitude > bbox.north:
            add_issue(
                collector,
                scope="record",
                message="Latitude falls outside the requested bbox.",
                field="latitude",
                value=latitude,
                record_index=record_index,
                chunk_index=chunk_index,
            )

        longitude = parse_float_or_none(str(record.get("longitude", "")))
        if longitude is None:
            add_issue(
                collector,
                scope="record",
                message="Longitude is missing or not numeric.",
                field="longitude",
                value=record.get("longitude"),
                record_index=record_index,
                chunk_index=chunk_index,
            )
        elif longitude < bbox.west or longitude > bbox.east:
            add_issue(
                collector,
                scope="record",
                message="Longitude falls outside the requested bbox.",
                field="longitude",
                value=longitude,
                record_index=record_index,
                chunk_index=chunk_index,
            )

        acq_date_value = parse_optional_date(str(record.get("acq_date", "")))
        if acq_date_value is None:
            add_issue(
                collector,
                scope="record",
                message="acq_date is missing or invalid.",
                field="acq_date",
                value=record.get("acq_date"),
                record_index=record_index,
                chunk_index=chunk_index,
            )
        else:
            if acq_date_value < start_date_value or acq_date_value > end_date_value:
                add_issue(
                    collector,
                    scope="record",
                    message="acq_date falls outside the requested date window.",
                    field="acq_date",
                    value=acq_date_value.isoformat(),
                    record_index=record_index,
                    chunk_index=chunk_index,
                )

        acq_time_hhmm = normalize_acq_time(str(record.get("acq_time", "")))
        if acq_time_hhmm is None:
            add_issue(
                collector,
                scope="record",
                message="acq_time is missing or invalid.",
                field="acq_time",
                value=record.get("acq_time"),
                record_index=record_index,
                chunk_index=chunk_index,
            )

        if latitude is not None and longitude is not None and acq_date_value is not None:
            normalized_count += 1
        if record.get("_acquired_at_utc"):
            acquired_at_count += 1

    return {
        "ok": collector.total_count == 0,
        "total_issue_count": collector.total_count,
        "issues": collector.issues,
        "record_count": len(records),
        "empty_result": len(records) == 0,
        "required_columns": list(REQUIRED_FIRE_COLUMNS),
        "required_columns_present": required_columns_present,
        "header": header,
        "header_consistent_across_chunks": header_consistent,
        "normalized_record_count": normalized_count,
        "acquired_at_count": acquired_at_count,
    }


def counter_to_sorted_dict(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def build_record_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    counts_by_date: Counter[str] = Counter()
    counts_by_satellite: Counter[str] = Counter()
    counts_by_instrument: Counter[str] = Counter()
    counts_by_daynight: Counter[str] = Counter()

    for record in records:
        acq_date_value = str(record.get("acq_date", "")).strip()
        if acq_date_value:
            counts_by_date[acq_date_value] += 1

        satellite = str(record.get("satellite", "")).strip()
        if satellite:
            counts_by_satellite[satellite] += 1

        instrument = str(record.get("instrument", "")).strip()
        if instrument:
            counts_by_instrument[instrument] += 1

        daynight = str(record.get("daynight", "")).strip()
        if daynight:
            counts_by_daynight[daynight] += 1

    return {
        "record_count": len(records),
        "counts_by_acq_date": counter_to_sorted_dict(counts_by_date),
        "counts_by_satellite": counter_to_sorted_dict(counts_by_satellite),
        "counts_by_instrument": counter_to_sorted_dict(counts_by_instrument),
        "counts_by_daynight": counter_to_sorted_dict(counts_by_daynight),
    }


def parse_map_key_status(payload: dict[str, Any] | list[Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise RuntimeError("MAP_KEY status response must be a JSON object.")
    status: dict[str, Any] = {
        "transaction_limit": payload.get("transaction_limit"),
        "current_transactions": payload.get("current_transactions"),
        "transaction_interval": payload.get("transaction_interval"),
    }
    return status


def parse_data_availability_table(table: CsvTable) -> list[dict[str, Any]]:
    required_columns = {"data_id", "min_date", "max_date"}
    missing = sorted(required_columns - set(table.header))
    if missing:
        raise RuntimeError(f"Data availability CSV missing columns: {missing}")

    records: list[dict[str, Any]] = []
    for row in table.records:
        min_date_value = parse_optional_date(row.get("min_date", ""))
        max_date_value = parse_optional_date(row.get("max_date", ""))
        records.append(
            {
                "data_id": row.get("data_id", "").strip(),
                "min_date": min_date_value.isoformat() if min_date_value else row.get("min_date", ""),
                "max_date": max_date_value.isoformat() if max_date_value else row.get("max_date", ""),
            }
        )
    return records


def fetch_map_key_status(
    client: RetryableHttpClient,
    config: RuntimeConfig,
) -> tuple[dict[str, Any], dict[str, Any]]:
    url = build_map_key_status_url(config)
    response = client.get(url, accept="application/json")
    payload = parse_json_payload(response)
    status = parse_map_key_status(payload)
    transport = {
        "request_url": redact_url(response.url),
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type", ""),
        "byte_length": response.byte_length,
    }
    return status, transport


def fetch_data_availability(
    client: RetryableHttpClient,
    config: RuntimeConfig,
    source: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    url = build_data_availability_url(config, source)
    response = client.get(url, accept="text/csv,text/plain;q=0.9,*/*;q=0.8")
    table = parse_csv_payload(response, context="data_availability")
    records = parse_data_availability_table(table)
    transport = {
        "request_url": redact_url(response.url),
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type", ""),
        "byte_length": response.byte_length,
        "row_count": table.row_count,
        "header": table.header,
    }
    return records, transport


def ensure_window_in_availability(
    availability_records: list[dict[str, Any]],
    *,
    source: str,
    start_date_value: date,
    end_date_value: date,
) -> dict[str, Any]:
    target = None
    for record in availability_records:
        if record.get("data_id") == source:
            target = record
            break

    if target is None:
        raise RuntimeError(f"Source {source} not found in data availability response.")

    min_date_value = parse_optional_date(str(target.get("min_date", "")))
    max_date_value = parse_optional_date(str(target.get("max_date", "")))
    if min_date_value is None or max_date_value is None:
        raise RuntimeError(f"Source {source} availability dates are invalid: {target}")
    if start_date_value < min_date_value or end_date_value > max_date_value:
        raise ValueError(
            f"Requested window {start_date_value.isoformat()}..{end_date_value.isoformat()} "
            f"falls outside available window {min_date_value.isoformat()}..{max_date_value.isoformat()} "
            f"for {source}."
        )

    return target


def serialize_json(data: dict[str, Any], *, pretty: bool) -> str:
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    )


def add_runtime_overrides(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default="", help=f"API base URL (default env {ENV_BASE_URL}).")
    parser.add_argument("--map-key", default=None, help=f"MAP_KEY override (default env {ENV_MAP_KEY}).")
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help=f"HTTP timeout seconds (default env {ENV_TIMEOUT_SECONDS}).",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help=f"Retry count for transient failures (default env {ENV_MAX_RETRIES}).",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=None,
        help=f"Initial retry delay seconds (default env {ENV_RETRY_BACKOFF_SECONDS}).",
    )
    parser.add_argument(
        "--retry-backoff-multiplier",
        type=float,
        default=None,
        help=f"Retry backoff multiplier (default env {ENV_RETRY_BACKOFF_MULTIPLIER}).",
    )
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=None,
        help=f"Minimum sleep between requests (default env {ENV_MIN_REQUEST_INTERVAL_SECONDS}).",
    )
    parser.add_argument(
        "--max-days-per-run",
        type=int,
        default=None,
        help=f"Safety cap for inclusive day range (default env {ENV_MAX_DAYS_PER_RUN}).",
    )
    parser.add_argument(
        "--max-chunk-days",
        type=int,
        default=None,
        help=f"Internal chunk size (default env {ENV_MAX_CHUNK_DAYS}).",
    )
    parser.add_argument(
        "--max-estimated-transactions-per-run",
        type=int,
        default=None,
        help=(
            "Local cap for estimated transaction weight "
            f"(default env {ENV_MAX_ESTIMATED_TRANSACTIONS_PER_RUN})."
        ),
    )
    parser.add_argument(
        "--max-retry-after-seconds",
        type=int,
        default=None,
        help=f"Fail when Retry-After exceeds this cap (default env {ENV_MAX_RETRY_AFTER_SECONDS}).",
    )
    parser.add_argument(
        "--enable-availability-probe",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=f"Default check-availability behavior (default env {ENV_ENABLE_AVAILABILITY_PROBE}).",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help=f"HTTP User-Agent (default env {ENV_USER_AGENT}).",
    )


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Log level for stderr/file logs.",
    )
    parser.add_argument("--log-file", default="", help="Optional log file path.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="NASA FIRMS fire fetch helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Validate effective configuration.")
    add_runtime_overrides(check)
    add_logging_args(check)
    check.add_argument(
        "--probe-map-key",
        action="store_true",
        help="Probe MAP_KEY transaction status remotely.",
    )
    check.add_argument(
        "--probe-source",
        default="",
        help=f"Optional source to probe via data_availability. Choices: {', '.join(PROBE_SOURCE_CHOICES)}.",
    )
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch = sub.add_parser("fetch", help="Fetch FIRMS active fire data.")
    add_runtime_overrides(fetch)
    add_logging_args(fetch)
    fetch.add_argument(
        "--source",
        required=True,
        choices=SOURCE_CHOICES,
        help="FIRMS source identifier.",
    )
    fetch.add_argument(
        "--bbox",
        required=True,
        help="Bounding box west,south,east,north.",
    )
    fetch.add_argument(
        "--start-date",
        required=True,
        help="Inclusive start date in YYYY-MM-DD format.",
    )
    fetch.add_argument(
        "--end-date",
        required=True,
        help="Inclusive end date in YYYY-MM-DD format.",
    )
    fetch.add_argument(
        "--check-availability",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Probe data availability before the fetch.",
    )
    fetch.add_argument(
        "--max-validation-issues",
        type=int,
        default=DEFAULT_MAX_VALIDATION_ISSUES,
        help=f"Maximum issue samples kept in output. Default: {DEFAULT_MAX_VALIDATION_ISSUES}.",
    )
    fetch.add_argument(
        "--fail-on-validation-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Return non-zero exit code when structure validation finds issues.",
    )
    fetch.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate arguments/config and print the chunk plan without remote calls.",
    )
    fetch.add_argument("--output", default="", help="Optional path for full JSON payload.")
    fetch.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def command_check_config(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    logger = build_logger(args.log_level, args.log_file)
    payload: dict[str, Any] = {
        "command": "check-config",
        "config": {
            "base_url": config.base_url,
            "map_key_set": bool(config.map_key),
            "map_key_masked": mask_secret(config.map_key),
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "retry_backoff_multiplier": config.retry_backoff_multiplier,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "max_days_per_run": config.max_days_per_run,
            "max_chunk_days": config.max_chunk_days,
            "max_estimated_transactions_per_run": config.max_estimated_transactions_per_run,
            "max_retry_after_seconds": config.max_retry_after_seconds,
            "enable_availability_probe": config.enable_availability_probe,
            "user_agent": config.user_agent,
        },
    }

    probe_source = args.probe_source.strip()
    if args.probe_map_key or probe_source:
        if not config.map_key:
            raise ValueError("MAP_KEY is required for remote probes.")
        client = RetryableHttpClient(config, logger)

        if args.probe_map_key:
            logger.info("probe-map-key")
            status, transport = fetch_map_key_status(client, config)
            payload["map_key_status"] = status
            payload["map_key_status_transport"] = transport

        if probe_source:
            normalized_source = normalize_source(
                probe_source,
                field_name="Probe source",
                allow_all=True,
            )
            logger.info("probe-data-availability source=%s", normalized_source)
            records, transport = fetch_data_availability(client, config, normalized_source)
            payload["data_availability"] = records
            payload["data_availability_transport"] = transport

    print(serialize_json(payload, pretty=args.pretty))
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    logger = build_logger(args.log_level, args.log_file)

    if not config.map_key and not args.dry_run:
        raise ValueError("MAP_KEY is required for fetch. Set NASA_FIRMS_MAP_KEY or use --map-key.")

    source = normalize_source(args.source)
    bbox = parse_bbox(args.bbox)
    start_date_value = parse_date_arg("--start-date", args.start_date)
    end_date_value = parse_date_arg("--end-date", args.end_date)
    if end_date_value < start_date_value:
        raise ValueError("--end-date must be on or after --start-date.")

    day_count = (end_date_value - start_date_value).days + 1
    if day_count > config.max_days_per_run:
        raise ValueError(
            f"Requested {day_count} days, exceeds cap {config.max_days_per_run}. "
            "Reduce the window or raise NASA_FIRMS_MAX_DAYS_PER_RUN explicitly."
        )

    chunk_plan = build_chunk_plan(
        config=config,
        source=source,
        bbox=bbox,
        start_date_value=start_date_value,
        end_date_value=end_date_value,
    )
    estimated_transactions_total = sum(item.estimated_transactions for item in chunk_plan)
    if estimated_transactions_total > config.max_estimated_transactions_per_run:
        raise ValueError(
            "Estimated FIRMS transaction weight "
            f"{estimated_transactions_total} exceeds cap "
            f"{config.max_estimated_transactions_per_run}. "
            "Reduce the bbox or window, or raise NASA_FIRMS_MAX_ESTIMATED_TRANSACTIONS_PER_RUN explicitly."
        )

    check_availability = (
        args.check_availability
        if args.check_availability is not None
        else config.enable_availability_probe
    )

    request_meta = {
        "base_url": config.base_url,
        "source": source,
        "bbox": {
            "west": bbox.west,
            "south": bbox.south,
            "east": bbox.east,
            "north": bbox.north,
        },
        "bbox_area_string": bbox_to_area_string(bbox),
        "start_date": start_date_value.isoformat(),
        "end_date": end_date_value.isoformat(),
        "day_count": day_count,
        "chunk_count": len(chunk_plan),
        "chunk_day_limit": config.max_chunk_days,
        "map_key_set": bool(config.map_key),
        "estimated_transactions_total": estimated_transactions_total,
        "estimated_transaction_formula_note": (
            "Estimate follows the FIRMS area page client-side weight formula and is a local safeguard."
        ),
        "check_availability": check_availability,
    }

    logger.info(
        "fetch-request source=%s bbox=%s days=%d chunks=%d est_tx=%d dry_run=%s",
        source,
        bbox_to_area_string(bbox),
        day_count,
        len(chunk_plan),
        estimated_transactions_total,
        args.dry_run,
    )

    payload: dict[str, Any] = {
        "command": "fetch",
        "request": request_meta,
        "chunk_plan": [
            {
                "chunk_index": item.index,
                "start_date": item.start_date.isoformat(),
                "end_date": item.end_date.isoformat(),
                "day_count": item.day_count,
                "estimated_transactions": item.estimated_transactions,
                "request_url": redact_url(item.request_url),
            }
            for item in chunk_plan
        ],
        "artifacts": {},
    }

    if args.dry_run:
        payload["dry_run"] = True
        print(serialize_json(payload, pretty=args.pretty))
        return 0

    client = RetryableHttpClient(config, logger)

    availability_payload: dict[str, Any] | None = None
    if check_availability:
        logger.info("availability-probe source=%s", source)
        availability_records, availability_transport = fetch_data_availability(client, config, source)
        selected_availability = ensure_window_in_availability(
            availability_records,
            source=source,
            start_date_value=start_date_value,
            end_date_value=end_date_value,
        )
        availability_payload = {
            "records": availability_records,
            "selected_source": selected_availability,
            "transport": availability_transport,
        }

    all_records: list[dict[str, Any]] = []
    transport_chunks: list[dict[str, Any]] = []
    headers_by_chunk: list[dict[str, Any]] = []
    canonical_header: list[str] = []

    for item in chunk_plan:
        logger.info(
            "fetch-chunk chunk=%d start=%s end=%s est_tx=%d url=%s",
            item.index,
            item.start_date.isoformat(),
            item.end_date.isoformat(),
            item.estimated_transactions,
            redact_url(item.request_url),
        )
        response = client.get(
            item.request_url,
            accept="text/csv,text/plain;q=0.9,*/*;q=0.8",
        )
        table = parse_csv_payload(response, context=f"chunk {item.index}")
        headers_by_chunk.append({"chunk_index": item.index, "header": table.header})
        if not canonical_header:
            canonical_header = list(table.header)

        normalized_records = normalize_fire_records(
            table.records,
            chunk=item,
            source=source,
            start_index=len(all_records),
        )
        all_records.extend(normalized_records)

        transport_chunks.append(
            {
                "chunk_index": item.index,
                "request_url": redact_url(response.url),
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type", ""),
                "byte_length": response.byte_length,
                "row_count": table.row_count,
                "header": table.header,
                "start_date": item.start_date.isoformat(),
                "end_date": item.end_date.isoformat(),
                "day_count": item.day_count,
                "estimated_transactions": item.estimated_transactions,
            }
        )

    validation_summary = validate_records(
        records=all_records,
        header=canonical_header,
        headers_by_chunk=headers_by_chunk,
        bbox=bbox,
        start_date_value=start_date_value,
        end_date_value=end_date_value,
        max_validation_issues=args.max_validation_issues,
    )

    payload["dry_run"] = False
    payload["transport"] = {
        "chunk_count": len(transport_chunks),
        "estimated_transactions_total": estimated_transactions_total,
        "chunks": transport_chunks,
    }
    if availability_payload is not None:
        payload["availability_probe"] = availability_payload
    payload["validation_summary"] = validation_summary
    payload["summary"] = build_record_summary(all_records)
    payload["records"] = all_records

    logger.info(
        "fetch-complete chunks=%d records=%d issues=%d",
        len(transport_chunks),
        len(all_records),
        validation_summary["total_issue_count"],
    )

    if args.output.strip():
        output_path = Path(args.output).expanduser().resolve()
        write_json_file(output_path, payload, pretty=args.pretty)
        payload["artifacts"]["full_payload_json"] = str(output_path)

    print(serialize_json(payload, pretty=args.pretty))

    if validation_summary["total_issue_count"] > 0 and args.fail_on_validation_error:
        logger.error(
            "validation-failed total_issue_count=%d",
            validation_summary["total_issue_count"],
        )
        return 1
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "check-config":
        return command_check_config(args)
    if args.command == "fetch":
        return command_fetch(args)
    parser.print_help(sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
