#!/usr/bin/env python3
"""Fetch Open-Meteo flood data with validation and logs."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "OPEN_METEO_FLOOD_BASE_URL"
ENV_API_KEY = "OPEN_METEO_FLOOD_API_KEY"
ENV_TIMEOUT_SECONDS = "OPEN_METEO_FLOOD_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "OPEN_METEO_FLOOD_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "OPEN_METEO_FLOOD_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "OPEN_METEO_FLOOD_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "OPEN_METEO_FLOOD_MIN_REQUEST_INTERVAL_SECONDS"
ENV_MAX_LOCATIONS_PER_RUN = "OPEN_METEO_FLOOD_MAX_LOCATIONS_PER_RUN"
ENV_MAX_DAYS_PER_RUN = "OPEN_METEO_FLOOD_MAX_DAYS_PER_RUN"
ENV_MAX_DAILY_VARIABLES_PER_RUN = "OPEN_METEO_FLOOD_MAX_DAILY_VARIABLES_PER_RUN"
ENV_MAX_RETRY_AFTER_SECONDS = "OPEN_METEO_FLOOD_MAX_RETRY_AFTER_SECONDS"
ENV_DEFAULT_TIMEZONE = "OPEN_METEO_FLOOD_DEFAULT_TIMEZONE"
ENV_DEFAULT_CELL_SELECTION = "OPEN_METEO_FLOOD_DEFAULT_CELL_SELECTION"
ENV_USER_AGENT = "OPEN_METEO_FLOOD_USER_AGENT"

DEFAULT_BASE_URL = "https://flood-api.open-meteo.com/v1/flood"
DEFAULT_TIMEOUT_SECONDS = 45
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.4
DEFAULT_MAX_LOCATIONS_PER_RUN = 10
DEFAULT_MAX_DAYS_PER_RUN = 366
DEFAULT_MAX_DAILY_VARIABLES_PER_RUN = 8
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_TIMEZONE = "GMT"
DEFAULT_CELL_SELECTION = "nearest"
DEFAULT_USER_AGENT = "open-meteo-flood-fetch/1.0"

RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
DAILY_TIME_FORMAT = "%Y-%m-%d"
DEFAULT_MAX_VALIDATION_ISSUES = 50
CELL_SELECTION_CHOICES = ("nearest", "land", "sea")
ENSEMBLE_MEMBER_PATTERN = re.compile(r"^river_discharge_member\d+$")


@dataclass(frozen=True)
class Location:
    latitude: float
    longitude: float


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    api_key: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    max_locations_per_run: int
    max_days_per_run: int
    max_daily_variables_per_run: int
    max_retry_after_seconds: int
    default_timezone: str
    default_cell_selection: str
    user_agent: str


@dataclass(frozen=True)
class HttpJsonResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    payload: dict[str, Any] | list[Any]
    byte_length: int


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


def normalize_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if not normalized:
        raise ValueError("Base URL cannot be empty.")
    if not normalized.startswith("http://") and not normalized.startswith("https://"):
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized!r}")
    return normalized


def normalize_cell_selection(value: str, *, field_name: str) -> str:
    normalized = value.strip().lower()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    if normalized not in CELL_SELECTION_CHOICES:
        raise ValueError(
            f"{field_name} must be one of {', '.join(CELL_SELECTION_CHOICES)}, got: {value!r}"
        )
    return normalized


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def unique_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in items:
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        output.append(value)
    return output


def parse_date_arg(name: str, raw: str) -> date:
    value = raw.strip()
    if not value:
        raise ValueError(f"{name} cannot be empty.")
    try:
        return datetime.strptime(value, DAILY_TIME_FORMAT).date()
    except ValueError as exc:
        raise ValueError(f"{name} must use YYYY-MM-DD, got: {raw!r}") from exc


def parse_daily_timestamp(raw: str) -> date | None:
    value = raw.strip()
    try:
        return datetime.strptime(value, DAILY_TIME_FORMAT).date()
    except ValueError:
        return None


def parse_location(raw: str) -> Location:
    parts = [item.strip() for item in raw.split(",")]
    if len(parts) != 2:
        raise ValueError(
            f"Invalid --location value {raw!r}. Use latitude,longitude format, for example 52.52,13.41."
        )

    try:
        latitude = float(parts[0])
        longitude = float(parts[1])
    except ValueError as exc:
        raise ValueError(
            f"Invalid --location value {raw!r}. Latitude and longitude must be numbers."
        ) from exc

    if latitude < -90 or latitude > 90:
        raise ValueError(f"Latitude must be between -90 and 90, got: {latitude}")
    if longitude < -180 or longitude > 180:
        raise ValueError(f"Longitude must be between -180 and 180, got: {longitude}")

    return Location(latitude=latitude, longitude=longitude)


def parse_locations(raw_locations: list[str]) -> list[Location]:
    if not raw_locations:
        raise ValueError("At least one --location is required.")
    return [parse_location(item) for item in raw_locations]


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    base_url = normalize_base_url(
        args.base_url if args.base_url else env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL)
    )
    api_key = (
        args.api_key if args.api_key is not None else os.environ.get(ENV_API_KEY, "")
    ).strip()
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
    max_locations_per_run = parse_positive_int(
        "--max-locations-per-run",
        str(
            args.max_locations_per_run
            if args.max_locations_per_run is not None
            else env_or_default(ENV_MAX_LOCATIONS_PER_RUN, str(DEFAULT_MAX_LOCATIONS_PER_RUN))
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
    max_daily_variables_per_run = parse_non_negative_int(
        "--max-daily-variables-per-run",
        str(
            args.max_daily_variables_per_run
            if args.max_daily_variables_per_run is not None
            else env_or_default(
                ENV_MAX_DAILY_VARIABLES_PER_RUN, str(DEFAULT_MAX_DAILY_VARIABLES_PER_RUN)
            )
        ),
    )
    max_retry_after_seconds = parse_positive_int(
        "--max-retry-after-seconds",
        str(
            args.max_retry_after_seconds
            if args.max_retry_after_seconds is not None
            else env_or_default(
                ENV_MAX_RETRY_AFTER_SECONDS, str(DEFAULT_MAX_RETRY_AFTER_SECONDS)
            )
        ),
    )
    default_timezone = (
        args.default_timezone
        if args.default_timezone
        else env_or_default(ENV_DEFAULT_TIMEZONE, DEFAULT_TIMEZONE)
    ).strip()
    if not default_timezone:
        raise ValueError("Default timezone cannot be empty.")
    default_cell_selection = normalize_cell_selection(
        args.default_cell_selection
        if args.default_cell_selection
        else env_or_default(ENV_DEFAULT_CELL_SELECTION, DEFAULT_CELL_SELECTION),
        field_name="Default cell selection",
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
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_backoff_multiplier=retry_backoff_multiplier,
        min_request_interval_seconds=min_request_interval_seconds,
        max_locations_per_run=max_locations_per_run,
        max_days_per_run=max_days_per_run,
        max_daily_variables_per_run=max_daily_variables_per_run,
        max_retry_after_seconds=max_retry_after_seconds,
        default_timezone=default_timezone,
        default_cell_selection=default_cell_selection,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("open-meteo-flood-fetch")
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
    pairs = parse.parse_qsl(split.query, keep_blank_values=True)
    redacted_pairs = []
    for key, value in pairs:
        if key.lower() == "apikey" and value:
            redacted_pairs.append((key, "***"))
        else:
            redacted_pairs.append((key, value))
    return parse.urlunsplit(
        (split.scheme, split.netloc, split.path, parse.urlencode(redacted_pairs), split.fragment)
    )


class RetryableHttpJsonClient:
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
    def _decode_json_bytes(payload_bytes: bytes) -> dict[str, Any] | list[Any]:
        try:
            text = payload_bytes.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise RuntimeError("Response body is not valid UTF-8.") from exc
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError("Response body is not valid JSON.") from exc
        if not isinstance(payload, (dict, list)):
            raise RuntimeError("Response JSON must be an object or array.")
        return payload

    def _http_error_message(
        self,
        *,
        status_code: int,
        reason: str,
        payload_bytes: bytes,
    ) -> str:
        if not payload_bytes:
            return f"HTTP {status_code}: {reason}"

        try:
            payload = self._decode_json_bytes(payload_bytes)
        except RuntimeError:
            try:
                text = payload_bytes.decode("utf-8", errors="replace").strip()
            except Exception:  # noqa: BLE001
                text = ""
            return f"HTTP {status_code}: {text or reason}"

        if isinstance(payload, dict):
            if payload.get("error") is True and isinstance(payload.get("reason"), str):
                return f"HTTP {status_code}: {payload['reason'].strip()}"
            for key in ("reason", "message", "detail"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return f"HTTP {status_code}: {value.strip()}"
        return f"HTTP {status_code}: {reason}"

    def get_json(self, url: str) -> HttpJsonResponse:
        attempts = self._cfg.max_retries + 1

        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("Accept", "application/json")
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

            content_type = headers.get("content-type", "")
            if "json" not in content_type.lower():
                raise RuntimeError(f"Expected JSON content-type, got: {content_type!r}")

            payload = self._decode_json_bytes(payload_bytes)
            elapsed_ms = (time.monotonic() - started) * 1000.0
            self._logger.info(
                "http-response status=%s bytes=%d elapsed_ms=%.3f url=%s",
                status_code,
                len(payload_bytes),
                elapsed_ms,
                redact_url(url),
            )
            return HttpJsonResponse(
                url=url,
                status_code=status_code,
                headers=headers,
                payload=payload,
                byte_length=len(payload_bytes),
            )

        raise RuntimeError("Exhausted request attempts.")


def write_json_file(path: Path, data: dict[str, Any], *, pretty: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            data,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )


def format_float(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def build_request_query(
    *,
    config: RuntimeConfig,
    locations: list[Location],
    start_date_value: date,
    end_date_value: date,
    daily_vars: list[str],
    timezone_value: str,
    cell_selection: str,
    ensemble: bool,
) -> dict[str, str]:
    query = {
        "latitude": ",".join(format_float(item.latitude) for item in locations),
        "longitude": ",".join(format_float(item.longitude) for item in locations),
        "start_date": start_date_value.isoformat(),
        "end_date": end_date_value.isoformat(),
        "daily": ",".join(daily_vars),
        "timezone": timezone_value,
        "cell_selection": cell_selection,
    }
    if ensemble:
        query["ensemble"] = "true"
    if config.api_key:
        query["apikey"] = config.api_key
    return query


def build_request_url(base_url: str, query: dict[str, str]) -> str:
    split = parse.urlsplit(base_url)
    existing = parse.parse_qsl(split.query, keep_blank_values=True)
    merged = dict(existing)
    merged.update(query)
    return parse.urlunsplit(
        (
            split.scheme,
            split.netloc,
            split.path,
            parse.urlencode(merged),
            split.fragment,
        )
    )


def normalize_records(payload: dict[str, Any] | list[Any]) -> tuple[list[dict[str, Any]], str]:
    if isinstance(payload, dict):
        if payload.get("error") is True:
            reason = payload.get("reason")
            raise RuntimeError(
                f"Open-Meteo returned an error object: {reason}" if reason else "Open-Meteo returned an error object."
            )
        return [payload], "object"

    if not isinstance(payload, list):
        raise RuntimeError("Top-level response must be an object or array.")

    normalized: list[dict[str, Any]] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise RuntimeError(
                f"Top-level array item at index {index} must be an object, got {type(item).__name__}."
            )
        if item.get("error") is True:
            reason = item.get("reason")
            raise RuntimeError(
                f"Open-Meteo returned an error object at index {index}: {reason}"
                if reason
                else f"Open-Meteo returned an error object at index {index}."
            )
        normalized.append(item)
    return normalized, "array"


def add_record_issue(
    collector: IssueCollector,
    *,
    issue_type: str,
    record_index: int,
    message: str,
    field: str = "",
) -> None:
    issue: dict[str, Any] = {
        "type": issue_type,
        "record_index": record_index,
        "message": message,
    }
    if field:
        issue["field"] = field
    collector.add(issue)


def validate_time_values(
    *,
    collector: IssueCollector,
    record_index: int,
    raw_values: Any,
    start_date_value: date,
    end_date_value: date,
    exact_expected_count: int,
) -> tuple[list[str], str | None, str | None]:
    if not isinstance(raw_values, list):
        add_record_issue(
            collector,
            issue_type="daily-time-type",
            record_index=record_index,
            field="daily.time",
            message="daily.time must be a list.",
        )
        return [], None, None

    parsed_count = len(raw_values)
    first_value: str | None = raw_values[0] if raw_values else None
    last_value: str | None = raw_values[-1] if raw_values else None
    if parsed_count != exact_expected_count:
        add_record_issue(
            collector,
            issue_type="daily-time-count",
            record_index=record_index,
            field="daily.time",
            message=f"daily.time length mismatch: expected {exact_expected_count}, got {parsed_count}.",
        )

    previous_value: date | None = None
    for index, value in enumerate(raw_values):
        if not isinstance(value, str):
            add_record_issue(
                collector,
                issue_type="daily-time-value-type",
                record_index=record_index,
                field="daily.time",
                message=f"daily.time[{index}] must be a string.",
            )
            continue
        parsed = parse_daily_timestamp(value)
        if parsed is None:
            add_record_issue(
                collector,
                issue_type="daily-time-parse",
                record_index=record_index,
                field="daily.time",
                message=f"Cannot parse daily.time[{index}]={value!r}.",
            )
            continue
        if parsed < start_date_value or parsed > end_date_value:
            add_record_issue(
                collector,
                issue_type="daily-time-range",
                record_index=record_index,
                field="daily.time",
                message=(
                    f"daily.time[{index}]={value!r} is outside requested date range "
                    f"{start_date_value.isoformat()}..{end_date_value.isoformat()}."
                ),
            )
        if previous_value is not None and parsed <= previous_value:
            add_record_issue(
                collector,
                issue_type="daily-time-order",
                record_index=record_index,
                field="daily.time",
                message=f"daily.time is not strictly ascending at index {index}.",
            )
        previous_value = parsed

    return raw_values, first_value, last_value


def collect_ensemble_member_fields(section: dict[str, Any]) -> list[str]:
    return sorted(key for key in section if ENSEMBLE_MEMBER_PATTERN.match(key))


def validate_daily_section(
    *,
    record: dict[str, Any],
    record_index: int,
    daily_vars: list[str],
    start_date_value: date,
    end_date_value: date,
    ensemble: bool,
    collector: IssueCollector,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "requested_variables": list(daily_vars),
        "requested_variable_count": len(daily_vars),
        "returned_time_count": 0,
        "first_time": None,
        "last_time": None,
        "total_value_count": 0,
        "variable_lengths": {},
        "ensemble_member_field_count": 0,
        "ensemble_member_fields_sample": [],
    }

    daily = record.get("daily")
    if not isinstance(daily, dict):
        add_record_issue(
            collector,
            issue_type="daily-missing",
            record_index=record_index,
            field="daily",
            message="Missing or invalid daily section.",
        )
        return summary

    daily_units = record.get("daily_units")
    if not isinstance(daily_units, dict):
        add_record_issue(
            collector,
            issue_type="daily-units-missing",
            record_index=record_index,
            field="daily_units",
            message="Missing or invalid daily_units section.",
        )
        daily_units = {}

    expected_count = (end_date_value - start_date_value).days + 1
    time_values, first_time, last_time = validate_time_values(
        collector=collector,
        record_index=record_index,
        raw_values=daily.get("time"),
        start_date_value=start_date_value,
        end_date_value=end_date_value,
        exact_expected_count=expected_count,
    )
    summary["returned_time_count"] = len(time_values)
    summary["first_time"] = first_time
    summary["last_time"] = last_time

    variable_lengths: dict[str, int | None] = {}
    for variable in daily_vars:
        if variable not in daily:
            add_record_issue(
                collector,
                issue_type="daily-variable-missing",
                record_index=record_index,
                field=variable,
                message=f"Requested variable {variable!r} is missing from daily.",
            )
            variable_lengths[variable] = None
            continue
        values = daily.get(variable)
        if not isinstance(values, list):
            add_record_issue(
                collector,
                issue_type="daily-variable-type",
                record_index=record_index,
                field=variable,
                message=f"daily.{variable} must be a list.",
            )
            variable_lengths[variable] = None
            continue
        variable_lengths[variable] = len(values)
        summary["total_value_count"] += len(values)
        if len(values) != len(time_values):
            add_record_issue(
                collector,
                issue_type="daily-variable-length",
                record_index=record_index,
                field=variable,
                message=(
                    f"daily.{variable} length mismatch: expected {len(time_values)}, "
                    f"got {len(values)}."
                ),
            )
        if variable not in daily_units:
            add_record_issue(
                collector,
                issue_type="daily-unit-missing",
                record_index=record_index,
                field=variable,
                message=f"daily_units.{variable} is missing.",
            )

    summary["variable_lengths"] = variable_lengths

    if ensemble and "river_discharge" in daily_vars:
        member_fields = collect_ensemble_member_fields(daily)
        summary["ensemble_member_field_count"] = len(member_fields)
        summary["ensemble_member_fields_sample"] = member_fields[:5]
        if not member_fields:
            add_record_issue(
                collector,
                issue_type="ensemble-members-missing",
                record_index=record_index,
                field="daily",
                message="Ensemble mode requested for river_discharge, but no member fields were returned.",
            )
        for member_field in member_fields:
            values = daily.get(member_field)
            if not isinstance(values, list):
                add_record_issue(
                    collector,
                    issue_type="ensemble-member-type",
                    record_index=record_index,
                    field=member_field,
                    message=f"daily.{member_field} must be a list.",
                )
                continue
            summary["total_value_count"] += len(values)
            if len(values) != len(time_values):
                add_record_issue(
                    collector,
                    issue_type="ensemble-member-length",
                    record_index=record_index,
                    field=member_field,
                    message=(
                        f"daily.{member_field} length mismatch: expected {len(time_values)}, "
                        f"got {len(values)}."
                    ),
                )
            if member_field not in daily_units:
                add_record_issue(
                    collector,
                    issue_type="ensemble-member-unit-missing",
                    record_index=record_index,
                    field=member_field,
                    message=f"daily_units.{member_field} is missing.",
                )

    return summary


def validate_records(
    *,
    records: list[dict[str, Any]],
    requested_locations: list[Location],
    start_date_value: date,
    end_date_value: date,
    daily_vars: list[str],
    ensemble: bool,
    max_validation_issues: int,
) -> dict[str, Any]:
    collector = IssueCollector(max_issues=max_validation_issues)
    if len(records) != len(requested_locations):
        collector.add(
            {
                "type": "record-count",
                "message": (
                    f"Response record count mismatch: expected {len(requested_locations)}, "
                    f"got {len(records)}."
                ),
            }
        )

    record_summaries: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            collector.add(
                {
                    "type": "record-type",
                    "record_index": index,
                    "message": f"Record at index {index} must be an object.",
                }
            )
            continue

        requested_location = requested_locations[index] if index < len(requested_locations) else None

        latitude = record.get("latitude")
        longitude = record.get("longitude")
        if not isinstance(latitude, (int, float)):
            add_record_issue(
                collector,
                issue_type="record-field",
                record_index=index,
                field="latitude",
                message="Record latitude must be numeric.",
            )
        if not isinstance(longitude, (int, float)):
            add_record_issue(
                collector,
                issue_type="record-field",
                record_index=index,
                field="longitude",
                message="Record longitude must be numeric.",
            )
        timezone_value = record.get("timezone")
        if not isinstance(timezone_value, str) or not timezone_value.strip():
            add_record_issue(
                collector,
                issue_type="record-field",
                record_index=index,
                field="timezone",
                message="Record timezone must be a non-empty string.",
            )

        record_summaries.append(
            {
                "record_index": index,
                "requested_location": (
                    {
                        "latitude": requested_location.latitude,
                        "longitude": requested_location.longitude,
                    }
                    if requested_location
                    else None
                ),
                "response_location": {
                    "latitude": latitude,
                    "longitude": longitude,
                },
                "timezone": timezone_value,
                "daily": validate_daily_section(
                    record=record,
                    record_index=index,
                    daily_vars=daily_vars,
                    start_date_value=start_date_value,
                    end_date_value=end_date_value,
                    ensemble=ensemble,
                    collector=collector,
                ),
            }
        )

    return {
        "ok": collector.total_count == 0,
        "total_issue_count": collector.total_count,
        "kept_issue_count": len(collector.issues),
        "issues": collector.issues,
        "record_count": len(records),
        "expected_record_count": len(requested_locations),
        "record_summaries": record_summaries,
    }


def serialize_json(data: dict[str, Any], *, pretty: bool) -> str:
    return json.dumps(
        data,
        ensure_ascii=False,
        indent=2 if pretty else None,
        separators=None if pretty else (",", ":"),
    )


def add_runtime_overrides(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default="", help=f"API base URL (default env {ENV_BASE_URL}).")
    parser.add_argument("--api-key", default=None, help=f"Optional API key override (default env {ENV_API_KEY}).")
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
        "--max-locations-per-run",
        type=int,
        default=None,
        help=f"Safety cap for locations (default env {ENV_MAX_LOCATIONS_PER_RUN}).",
    )
    parser.add_argument(
        "--max-days-per-run",
        type=int,
        default=None,
        help=f"Safety cap for inclusive day range (default env {ENV_MAX_DAYS_PER_RUN}).",
    )
    parser.add_argument(
        "--max-daily-variables-per-run",
        type=int,
        default=None,
        help=f"Safety cap for repeated --daily-var (default env {ENV_MAX_DAILY_VARIABLES_PER_RUN}).",
    )
    parser.add_argument(
        "--max-retry-after-seconds",
        type=int,
        default=None,
        help=f"Fail when Retry-After exceeds this cap (default env {ENV_MAX_RETRY_AFTER_SECONDS}).",
    )
    parser.add_argument(
        "--default-timezone",
        default="",
        help=f"Default timezone when --timezone is unset (default env {ENV_DEFAULT_TIMEZONE}).",
    )
    parser.add_argument(
        "--default-cell-selection",
        default="",
        help=f"Default cell selection when --cell-selection is unset (default env {ENV_DEFAULT_CELL_SELECTION}).",
    )
    parser.add_argument(
        "--user-agent",
        default=None,
        help=f"HTTP User-Agent (default env {ENV_USER_AGENT}).",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Open-Meteo flood fetch helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Validate effective configuration.")
    add_runtime_overrides(check)
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch = sub.add_parser("fetch", help="Fetch flood data.")
    add_runtime_overrides(fetch)
    fetch.add_argument(
        "--location",
        action="append",
        default=[],
        help="Coordinate pair latitude,longitude. Repeat for multiple locations.",
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
        "--daily-var",
        action="append",
        default=[],
        help="Daily variable name. Repeat for multiple variables.",
    )
    fetch.add_argument(
        "--ensemble",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Request ensemble expansion when supported by the API.",
    )
    fetch.add_argument(
        "--cell-selection",
        choices=CELL_SELECTION_CHOICES,
        default="",
        help="Grid-cell selection strategy.",
    )
    fetch.add_argument(
        "--timezone",
        default="",
        help="Timezone parameter for Open-Meteo. Defaults to env-configured timezone.",
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
        help="Validate arguments/config and print request plan without remote calls.",
    )
    fetch.add_argument("--output", default="", help="Optional path for full JSON payload.")
    fetch.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        help="Log level for stderr/file logs.",
    )
    fetch.add_argument("--log-file", default="", help="Optional log file path.")
    fetch.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def command_check_config(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    payload = {
        "command": "check-config",
        "config": {
            "base_url": config.base_url,
            "api_key_set": bool(config.api_key),
            "api_key_masked": mask_secret(config.api_key),
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "retry_backoff_multiplier": config.retry_backoff_multiplier,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "max_locations_per_run": config.max_locations_per_run,
            "max_days_per_run": config.max_days_per_run,
            "max_daily_variables_per_run": config.max_daily_variables_per_run,
            "max_retry_after_seconds": config.max_retry_after_seconds,
            "default_timezone": config.default_timezone,
            "default_cell_selection": config.default_cell_selection,
            "user_agent": config.user_agent,
        },
    }
    print(serialize_json(payload, pretty=args.pretty))
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)
    logger = build_logger(args.log_level, args.log_file)

    locations = parse_locations(args.location)
    if len(locations) > config.max_locations_per_run:
        raise ValueError(
            f"Requested {len(locations)} locations, exceeds cap {config.max_locations_per_run}."
        )

    start_date_value = parse_date_arg("--start-date", args.start_date)
    end_date_value = parse_date_arg("--end-date", args.end_date)
    if end_date_value < start_date_value:
        raise ValueError("--end-date must be on or after --start-date.")

    day_count = (end_date_value - start_date_value).days + 1
    if day_count > config.max_days_per_run:
        raise ValueError(
            f"Requested {day_count} days, exceeds cap {config.max_days_per_run}. "
            "Reduce the window or raise OPEN_METEO_FLOOD_MAX_DAYS_PER_RUN explicitly."
        )

    daily_vars = unique_preserve_order(args.daily_var)
    if not daily_vars:
        raise ValueError("Specify at least one --daily-var.")
    if len(daily_vars) > config.max_daily_variables_per_run:
        raise ValueError(
            f"Requested {len(daily_vars)} daily variables, exceeds cap "
            f"{config.max_daily_variables_per_run}."
        )

    timezone_value = args.timezone.strip() or config.default_timezone
    if not timezone_value:
        raise ValueError("Effective timezone cannot be empty.")
    cell_selection = normalize_cell_selection(
        args.cell_selection.strip() or config.default_cell_selection,
        field_name="Cell selection",
    )

    query = build_request_query(
        config=config,
        locations=locations,
        start_date_value=start_date_value,
        end_date_value=end_date_value,
        daily_vars=daily_vars,
        timezone_value=timezone_value,
        cell_selection=cell_selection,
        ensemble=args.ensemble,
    )
    request_url = build_request_url(config.base_url, query)

    estimated_daily_points = day_count * len(daily_vars) * len(locations)
    estimated_daily_points_note = ""
    if args.ensemble and "river_discharge" in daily_vars:
        estimated_daily_points_note = (
            "Ensemble mode may add many river_discharge_memberNN series beyond requested variables."
        )

    request_meta = {
        "base_url": config.base_url,
        "request_url": redact_url(request_url),
        "requested_locations": [
            {"latitude": item.latitude, "longitude": item.longitude} for item in locations
        ],
        "location_count": len(locations),
        "start_date": start_date_value.isoformat(),
        "end_date": end_date_value.isoformat(),
        "day_count": day_count,
        "daily_variables": daily_vars,
        "timezone": timezone_value,
        "cell_selection": cell_selection,
        "ensemble": args.ensemble,
        "api_key_set": bool(config.api_key),
        "estimated_daily_points": estimated_daily_points,
        "estimated_daily_points_note": estimated_daily_points_note,
    }

    logger.info(
        "fetch-request locations=%d days=%d daily_vars=%d ensemble=%s dry_run=%s url=%s",
        len(locations),
        day_count,
        len(daily_vars),
        args.ensemble,
        args.dry_run,
        redact_url(request_url),
    )

    payload: dict[str, Any] = {
        "command": "fetch",
        "request": request_meta,
        "artifacts": {},
    }

    if args.dry_run:
        payload["dry_run"] = True
        print(serialize_json(payload, pretty=args.pretty))
        return 0

    client = RetryableHttpJsonClient(config, logger)
    response = client.get_json(request_url)
    records, response_shape = normalize_records(response.payload)

    validation_summary = validate_records(
        records=records,
        requested_locations=locations,
        start_date_value=start_date_value,
        end_date_value=end_date_value,
        daily_vars=daily_vars,
        ensemble=args.ensemble,
        max_validation_issues=args.max_validation_issues,
    )

    payload["dry_run"] = False
    payload["transport"] = {
        "status_code": response.status_code,
        "content_type": response.headers.get("content-type", ""),
        "byte_length": response.byte_length,
        "response_shape": response_shape,
        "request_url": redact_url(response.url),
    }
    payload["validation_summary"] = validation_summary
    payload["records"] = records

    logger.info(
        "fetch-complete records=%d issues=%d status=%s",
        len(records),
        validation_summary["total_issue_count"],
        response.status_code,
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
