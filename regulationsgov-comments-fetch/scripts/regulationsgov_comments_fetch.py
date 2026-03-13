#!/usr/bin/env python3
"""Fetch Regulations.gov v4 comments with retries, throttling, and schema checks."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "REGGOV_BASE_URL"
ENV_API_KEY = "REGGOV_API_KEY"
ENV_TIMEOUT_SECONDS = "REGGOV_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "REGGOV_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "REGGOV_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "REGGOV_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "REGGOV_MIN_REQUEST_INTERVAL_SECONDS"
ENV_PAGE_SIZE = "REGGOV_PAGE_SIZE"
ENV_MAX_PAGES_PER_RUN = "REGGOV_MAX_PAGES_PER_RUN"
ENV_MAX_RECORDS_PER_RUN = "REGGOV_MAX_RECORDS_PER_RUN"
ENV_MAX_RETRY_AFTER_SECONDS = "REGGOV_MAX_RETRY_AFTER_SECONDS"
ENV_USER_AGENT = "REGGOV_USER_AGENT"

DEFAULT_BASE_URL = "https://api.regulations.gov/v4"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 1.2
DEFAULT_PAGE_SIZE = 25
DEFAULT_MAX_PAGES_PER_RUN = 20
DEFAULT_MAX_RECORDS_PER_RUN = 2000
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_USER_AGENT = "regulationsgov-comments-fetch/1.0"

COMMENTS_PATH = "comments"
RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
REQUIRED_META_FIELDS: tuple[tuple[str, type], ...] = (
    ("hasNextPage", bool),
    ("hasPreviousPage", bool),
    ("numberOfElements", int),
    ("pageNumber", int),
    ("pageSize", int),
    ("totalElements", int),
    ("totalPages", int),
    ("firstPage", bool),
    ("lastPage", bool),
)
ISO_UTC_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
DEFAULT_MAX_VALIDATION_ISSUES = 30


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    api_key: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    page_size: int
    max_pages_per_run: int
    max_records_per_run: int
    max_retry_after_seconds: int
    user_agent: str


@dataclass(frozen=True)
class HttpJsonResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    payload: dict[str, Any]
    byte_length: int


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


def mask_api_key(value: str) -> str:
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def ensure_page_size(value: int) -> int:
    if value < 5 or value > 250:
        raise ValueError(f"Page size must be between 5 and 250, got: {value}")
    return value


def build_runtime_config(args: argparse.Namespace) -> RuntimeConfig:
    base_url = normalize_base_url(
        args.base_url if args.base_url else env_or_default(ENV_BASE_URL, DEFAULT_BASE_URL)
    )
    api_key = (
        args.api_key.strip()
        if getattr(args, "api_key", "").strip()
        else env_or_default(ENV_API_KEY, "")
    )
    if not api_key:
        raise ValueError(
            "API key is required. Set --api-key or environment variable REGGOV_API_KEY."
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
    page_size = ensure_page_size(
        parse_positive_int(
            "--page-size",
            str(
                args.page_size
                if args.page_size is not None
                else env_or_default(ENV_PAGE_SIZE, str(DEFAULT_PAGE_SIZE))
            ),
        )
    )
    max_pages_per_run = parse_positive_int(
        "--max-pages-per-run",
        str(
            args.max_pages_per_run
            if args.max_pages_per_run is not None
            else env_or_default(ENV_MAX_PAGES_PER_RUN, str(DEFAULT_MAX_PAGES_PER_RUN))
        ),
    )
    max_records_per_run = parse_positive_int(
        "--max-records-per-run",
        str(
            args.max_records_per_run
            if args.max_records_per_run is not None
            else env_or_default(ENV_MAX_RECORDS_PER_RUN, str(DEFAULT_MAX_RECORDS_PER_RUN))
        ),
    )
    max_retry_after_seconds = parse_non_negative_int(
        "--max-retry-after-seconds",
        str(
            args.max_retry_after_seconds
            if args.max_retry_after_seconds is not None
            else env_or_default(
                ENV_MAX_RETRY_AFTER_SECONDS, str(DEFAULT_MAX_RETRY_AFTER_SECONDS)
            )
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
        api_key=api_key,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_backoff_multiplier=retry_backoff_multiplier,
        min_request_interval_seconds=min_request_interval_seconds,
        page_size=page_size,
        max_pages_per_run=max_pages_per_run,
        max_records_per_run=max_records_per_run,
        max_retry_after_seconds=max_retry_after_seconds,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("regulationsgov-comments-fetch")
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


def is_int_not_bool(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def add_issue(
    issues: list[dict[str, Any]],
    issue_count: int,
    max_issues: int,
    issue: dict[str, Any],
) -> int:
    new_count = issue_count + 1
    if len(issues) < max_issues:
        issues.append(issue)
    return new_count


def parse_datetime_utc(raw: str, *, field_name: str, is_end: bool) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError(f"{field_name} cannot be empty.")

    formats = [
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    parsed: datetime | None = None
    for fmt in formats:
        try:
            parsed = datetime.strptime(text, fmt)
            break
        except ValueError:
            continue

    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError(
                f"Invalid datetime {raw!r} for {field_name}. "
                "Use YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD HH:MM:SS."
            ) from exc

    if parsed.tzinfo is None:
        if len(text) == 10:
            if is_end:
                parsed = datetime(parsed.year, parsed.month, parsed.day, 23, 59, 59)
            else:
                parsed = datetime(parsed.year, parsed.month, parsed.day, 0, 0, 0)
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def parse_date_only(raw: str, *, field_name: str) -> date:
    text = raw.strip()
    if not text:
        raise ValueError(f"{field_name} cannot be empty.")
    try:
        return datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid date {raw!r} for {field_name}. Use YYYY-MM-DD.") from exc


def render_query(base_url: str, path: str, query: dict[str, str]) -> str:
    encoded = parse.urlencode(query, doseq=False)
    if encoded:
        return f"{base_url}/{path}?{encoded}"
    return f"{base_url}/{path}"


def extract_rate_limit(headers: dict[str, str]) -> dict[str, Any]:
    info: dict[str, Any] = {}
    for key in ("x-ratelimit-limit", "x-ratelimit-remaining", "retry-after"):
        if key in headers:
            value = headers[key]
            if value.isdigit():
                info[key] = int(value)
            else:
                info[key] = value
    return info


def error_excerpt(payload: bytes) -> str:
    text = payload.decode("utf-8", errors="replace").strip()
    if not text:
        return ""
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return text[:400]

    if isinstance(obj, dict):
        errors = obj.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                title = str(first.get("title", "")).strip()
                detail = str(first.get("detail", "")).strip()
                if title and detail:
                    return f"{title}: {detail}"[:400]
                if title:
                    return title[:400]
        err = obj.get("error")
        if isinstance(err, dict):
            code = str(err.get("code", "")).strip()
            message = str(err.get("message", "")).strip()
            if code and message:
                return f"{code}: {message}"[:400]
            if message:
                return message[:400]

    return text[:400]


class RetryableHttpClient:
    def __init__(self, config: RuntimeConfig, logger: logging.Logger) -> None:
        self._cfg = config
        self._logger = logger
        self._last_request_monotonic: float | None = None

    def _throttle(self) -> None:
        if self._last_request_monotonic is None:
            return
        elapsed = time.monotonic() - self._last_request_monotonic
        sleep_seconds = self._cfg.min_request_interval_seconds - elapsed
        if sleep_seconds > 0:
            self._logger.debug("throttle-sleep=%.3fs", sleep_seconds)
            time.sleep(sleep_seconds)

    @staticmethod
    def _parse_retry_after(value: str | None) -> float | None:
        if value is None:
            return None
        text = value.strip()
        if not text:
            return None
        try:
            seconds = float(text)
        except ValueError:
            return None
        return seconds if seconds >= 0 else None

    def _compute_retry_delay(
        self,
        headers: dict[str, str],
        attempt: int,
    ) -> float:
        retry_after = self._parse_retry_after(headers.get("retry-after"))
        if retry_after is not None:
            if retry_after > self._cfg.max_retry_after_seconds:
                raise RuntimeError(
                    "Received Retry-After "
                    f"{retry_after}s which exceeds cap {self._cfg.max_retry_after_seconds}s. "
                    "Abort to avoid multi-hour blocking; retry later or increase max-retry-after cap."
                )
            return retry_after

        return self._cfg.retry_backoff_seconds * (
            self._cfg.retry_backoff_multiplier ** (attempt - 1)
        )

    def get_json(self, url: str) -> HttpJsonResponse:
        attempts = self._cfg.max_retries + 1

        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("User-Agent", self._cfg.user_agent)
            req.add_header("Accept", "application/vnd.api+json, application/json")
            req.add_header("X-Api-Key", self._cfg.api_key)

            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, url)
            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    payload_raw = resp.read()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    self._last_request_monotonic = time.monotonic()

                    status = int(getattr(resp, "status", 200))
                    rate_limit = extract_rate_limit(headers)
                    self._logger.info(
                        "http-ok status=%d bytes=%d url=%s rate_limit=%s",
                        status,
                        len(payload_raw),
                        url,
                        rate_limit,
                    )

                    content_type = headers.get("content-type", "")
                    if "json" not in content_type.lower():
                        raise RuntimeError(
                            f"Unexpected content-type {content_type!r} for {url}."
                        )

                    try:
                        text = payload_raw.decode("utf-8", errors="strict")
                    except UnicodeDecodeError as exc:
                        raise RuntimeError(
                            f"Response body is not valid UTF-8 for {url}: {exc}"
                        ) from exc

                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(
                            f"Response body is not valid JSON for {url}: {exc}"
                        ) from exc

                    if not isinstance(payload, dict):
                        raise RuntimeError(
                            f"Response JSON root must be object, got {type(payload).__name__}."
                        )

                    return HttpJsonResponse(
                        url=url,
                        status_code=status,
                        headers=headers,
                        payload=payload,
                        byte_length=len(payload_raw),
                    )
            except HTTPError as exc:
                self._last_request_monotonic = time.monotonic()
                status = int(exc.code)
                body = exc.read()
                headers = {k.lower(): v for k, v in exc.headers.items()}
                excerpt = error_excerpt(body)
                retriable = status in RETRIABLE_HTTP_CODES
                rate_limit = extract_rate_limit(headers)

                if retriable and attempt < attempts:
                    delay = self._compute_retry_delay(headers=headers, attempt=attempt)
                    self._logger.warning(
                        "http-retry status=%d delay=%.2fs attempt=%d/%d url=%s rate_limit=%s error=%s",
                        status,
                        delay,
                        attempt,
                        attempts,
                        url,
                        rate_limit,
                        excerpt,
                    )
                    time.sleep(delay)
                    continue

                raise RuntimeError(
                    f"HTTP {status} for {url}. rate_limit={rate_limit} error={excerpt!r}"
                ) from exc
            except (URLError, TimeoutError, ConnectionResetError) as exc:
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


def validate_comments_page(
    payload: dict[str, Any],
    *,
    max_issues: int,
) -> dict[str, Any]:
    issues: list[dict[str, Any]] = []
    issue_count = 0

    data = payload.get("data")
    meta = payload.get("meta")

    if not isinstance(data, list):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.data",
                "reason": "invalid_type",
                "expected": "array",
                "actual": type(data).__name__,
            },
        )
        data = []

    if not isinstance(meta, dict):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.meta",
                "reason": "invalid_type",
                "expected": "object",
                "actual": type(meta).__name__,
            },
        )
        meta = {}

    for key, expected_type in REQUIRED_META_FIELDS:
        value = meta.get(key)
        if value is None:
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"meta.{key}",
                    "reason": "missing_field",
                },
            )
            continue

        if expected_type is int:
            ok = is_int_not_bool(value)
        else:
            ok = isinstance(value, expected_type)

        if not ok:
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"meta.{key}",
                    "reason": "invalid_type",
                    "expected": expected_type.__name__,
                    "actual": type(value).__name__,
                },
            )

    for idx, item in enumerate(data):
        loc = f"data[{idx}]"
        if not isinstance(item, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": loc,
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(item).__name__,
                },
            )
            continue

        item_id = item.get("id")
        if not isinstance(item_id, str) or not item_id.strip():
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{loc}.id",
                    "reason": "invalid_id",
                    "actual": item_id,
                },
            )

        item_type = item.get("type")
        if item_type != "comments":
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{loc}.type",
                    "reason": "unexpected_value",
                    "expected": "comments",
                    "actual": item_type,
                },
            )

        attributes = item.get("attributes")
        if not isinstance(attributes, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{loc}.attributes",
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(attributes).__name__,
                },
            )
            continue

        for attr_key in ("postedDate", "lastModifiedDate"):
            raw = attributes.get(attr_key)
            if raw is None:
                continue
            if not isinstance(raw, str):
                issue_count = add_issue(
                    issues,
                    issue_count,
                    max_issues,
                    {
                        "location": f"{loc}.attributes.{attr_key}",
                        "reason": "invalid_type",
                        "expected": "string_or_null",
                        "actual": type(raw).__name__,
                    },
                )
                continue
            if raw and not ISO_UTC_PATTERN.match(raw):
                issue_count = add_issue(
                    issues,
                    issue_count,
                    max_issues,
                    {
                        "location": f"{loc}.attributes.{attr_key}",
                        "reason": "invalid_datetime_format",
                        "expected": "YYYY-MM-DDTHH:MM:SSZ",
                        "actual": raw,
                    },
                )

    return {
        "passed": issue_count == 0,
        "issue_count": issue_count,
        "reported_issue_count": len(issues),
        "issues": issues,
    }


def summarize_preview(item: dict[str, Any]) -> dict[str, Any]:
    attributes = item.get("attributes") if isinstance(item, dict) else None
    attrs: dict[str, Any] = attributes if isinstance(attributes, dict) else {}
    return {
        "id": item.get("id") if isinstance(item, dict) else None,
        "type": item.get("type") if isinstance(item, dict) else None,
        "agencyId": attrs.get("agencyId"),
        "postedDate": attrs.get("postedDate"),
        "lastModifiedDate": attrs.get("lastModifiedDate"),
        "title": attrs.get("title"),
    }


def serialize_meta_summary(meta: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key, _ in REQUIRED_META_FIELDS:
        summary[key] = meta.get(key)
    if "filters" in meta:
        summary["filters"] = meta.get("filters")
    return summary


def ensure_fetch_limits(
    *,
    max_pages: int,
    max_records: int,
    start_page: int,
    config: RuntimeConfig,
) -> None:
    if start_page < 1:
        raise ValueError("--start-page must be >= 1.")
    if max_pages < 1:
        raise ValueError("--max-pages must be >= 1.")
    if max_pages > config.max_pages_per_run:
        raise ValueError(
            f"--max-pages={max_pages} exceeds configured cap {config.max_pages_per_run} "
            f"(set by --max-pages-per-run or {ENV_MAX_PAGES_PER_RUN})."
        )
    if max_records < 0:
        raise ValueError("--max-records must be >= 0.")
    if max_records > config.max_records_per_run:
        raise ValueError(
            f"--max-records={max_records} exceeds configured cap {config.max_records_per_run} "
            f"(set by --max-records-per-run or {ENV_MAX_RECORDS_PER_RUN})."
        )


def build_filters(args: argparse.Namespace) -> tuple[dict[str, str], dict[str, Any], str]:
    filters: dict[str, str] = {}
    normalized: dict[str, Any] = {
        "filter_mode": args.filter_mode,
        "agency_id": args.agency_id or None,
        "comment_on_id": args.comment_on_id or None,
        "search_term": args.search_term or None,
    }

    if args.filter_mode == "last-modified":
        if not args.start_datetime or not args.end_datetime:
            raise ValueError(
                "--start-datetime and --end-datetime are required for --filter-mode last-modified."
            )
        start_dt = parse_datetime_utc(
            args.start_datetime,
            field_name="--start-datetime",
            is_end=False,
        )
        end_dt = parse_datetime_utc(
            args.end_datetime,
            field_name="--end-datetime",
            is_end=True,
        )
        if end_dt < start_dt:
            raise ValueError("--end-datetime must be >= --start-datetime.")

        filters["filter[lastModifiedDate][ge]"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        filters["filter[lastModifiedDate][le]"] = end_dt.strftime("%Y-%m-%d %H:%M:%S")
        normalized["start_utc"] = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        normalized["end_utc"] = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        default_sort = "lastModifiedDate"
    elif args.filter_mode == "posted":
        if not args.start_date or not args.end_date:
            raise ValueError(
                "--start-date and --end-date are required for --filter-mode posted."
            )
        start_d = parse_date_only(args.start_date, field_name="--start-date")
        end_d = parse_date_only(args.end_date, field_name="--end-date")
        if end_d < start_d:
            raise ValueError("--end-date must be >= --start-date.")

        filters["filter[postedDate][ge]"] = start_d.isoformat()
        filters["filter[postedDate][le]"] = end_d.isoformat()
        normalized["start_date"] = start_d.isoformat()
        normalized["end_date"] = end_d.isoformat()
        default_sort = "postedDate"
    else:
        raise ValueError(f"Unsupported --filter-mode: {args.filter_mode}")

    if args.agency_id:
        filters["filter[agencyId]"] = args.agency_id
    if args.comment_on_id:
        filters["filter[commentOnId]"] = args.comment_on_id
    if args.search_term:
        filters["filter[searchTerm]"] = args.search_term

    return filters, normalized, default_sort


def print_json(payload: dict[str, Any], pretty: bool) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
    )


def write_quarantine_issues(
    *,
    quarantine_dir: Path,
    page_number: int,
    issues: list[dict[str, Any]],
) -> Path | None:
    if not issues:
        return None
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    output_path = quarantine_dir / f"comments-page-{page_number:04d}.validation-issues.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for issue in issues:
            handle.write(json.dumps(issue, ensure_ascii=False))
            handle.write("\n")
    return output_path


def build_output_file_path(
    *,
    output_dir: Path,
    output_file: str,
    filter_mode: str,
    filter_start: str,
    filter_end: str,
) -> Path:
    if output_file.strip():
        return Path(output_file).expanduser().resolve()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    token = f"{filter_mode}_{filter_start}_{filter_end}".lower()
    token = re.sub(r"[^a-z0-9]+", "-", token).strip("-")
    filename = f"comments-{token}-{timestamp}.jsonl"
    return (output_dir / filename).resolve()


def save_records_jsonl(
    *,
    path: Path,
    records: list[dict[str, Any]],
    overwrite: bool,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise RuntimeError(f"Output file already exists: {path}")

    with path.open("w", encoding="utf-8") as handle:
        for item in records:
            handle.write(json.dumps(item, ensure_ascii=False))
            handle.write("\n")


def command_check_config(args: argparse.Namespace) -> int:
    config = build_runtime_config(args)

    payload = {
        "ok": True,
        "config": {
            "base_url": config.base_url,
            "api_key_masked": mask_api_key(config.api_key),
            "api_key_length": len(config.api_key),
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "retry_backoff_multiplier": config.retry_backoff_multiplier,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "page_size": config.page_size,
            "max_pages_per_run": config.max_pages_per_run,
            "max_records_per_run": config.max_records_per_run,
            "max_retry_after_seconds": config.max_retry_after_seconds,
            "user_agent": config.user_agent,
        },
        "source_urls": {
            "comments": f"{config.base_url}/{COMMENTS_PATH}",
        },
        "env_keys": {
            "base_url": ENV_BASE_URL,
            "api_key": ENV_API_KEY,
            "timeout_seconds": ENV_TIMEOUT_SECONDS,
            "max_retries": ENV_MAX_RETRIES,
            "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
            "retry_backoff_multiplier": ENV_RETRY_BACKOFF_MULTIPLIER,
            "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
            "page_size": ENV_PAGE_SIZE,
            "max_pages_per_run": ENV_MAX_PAGES_PER_RUN,
            "max_records_per_run": ENV_MAX_RECORDS_PER_RUN,
            "max_retry_after_seconds": ENV_MAX_RETRY_AFTER_SECONDS,
            "user_agent": ENV_USER_AGENT,
        },
    }
    print_json(payload, pretty=args.pretty)
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    logger = build_logger(level=args.log_level, log_file=args.log_file)
    config = build_runtime_config(args)

    if args.max_validation_issues < 1:
        raise ValueError("--max-validation-issues must be >= 1.")
    if args.preview_records < 0:
        raise ValueError("--preview-records must be >= 0.")

    ensure_fetch_limits(
        max_pages=args.max_pages,
        max_records=args.max_records,
        start_page=args.start_page,
        config=config,
    )

    filters, normalized_filters, default_sort = build_filters(args)
    sort_value = args.sort.strip() if args.sort.strip() else default_sort

    page_size = config.page_size
    common_query: dict[str, str] = {
        "page[size]": str(page_size),
        "sort": sort_value,
    }
    common_query.update(filters)

    if args.dry_run:
        payload = {
            "ok": True,
            "dry_run": True,
            "request_plan": {
                "base_url": config.base_url,
                "path": COMMENTS_PATH,
                "filter_mode": args.filter_mode,
                "filters": normalized_filters,
                "sort": sort_value,
                "page_size": page_size,
                "start_page": args.start_page,
                "max_pages": args.max_pages,
                "max_records": args.max_records,
                "include_records": args.include_records,
            },
            "sample_request_url": render_query(
                config.base_url,
                COMMENTS_PATH,
                {**common_query, "page[number]": str(args.start_page)},
            ),
        }
        print_json(payload, pretty=args.pretty)
        return 0

    client = RetryableHttpClient(config=config, logger=logger)

    records: list[dict[str, Any]] = []
    page_summaries: list[dict[str, Any]] = []
    issue_count_total = 0
    pages_with_issues = 0
    rate_limit_last: dict[str, Any] = {}

    pages_fetched = 0
    page_number = args.start_page
    stop_reason = "unknown"

    while True:
        if pages_fetched >= args.max_pages:
            stop_reason = "max_pages_reached"
            break

        if args.max_records > 0 and len(records) >= args.max_records:
            stop_reason = "max_records_reached"
            break

        query = dict(common_query)
        query["page[number]"] = str(page_number)
        url = render_query(config.base_url, COMMENTS_PATH, query)

        response = client.get_json(url)
        payload = response.payload
        validation = validate_comments_page(payload, max_issues=args.max_validation_issues)
        issue_count_total += validation["issue_count"]

        if validation["issue_count"] > 0:
            pages_with_issues += 1
            logger.warning(
                "page-validation-issues page=%d issues=%d",
                page_number,
                validation["issue_count"],
            )
            if args.quarantine_dir.strip():
                q_path = write_quarantine_issues(
                    quarantine_dir=Path(args.quarantine_dir).expanduser().resolve(),
                    page_number=page_number,
                    issues=validation["issues"],
                )
                validation["quarantine_path"] = str(q_path) if q_path else None
            else:
                validation["quarantine_path"] = None

            if args.fail_on_validation_error:
                raise RuntimeError(
                    f"Structure validation failed on page {page_number} "
                    f"(issues={validation['issue_count']})."
                )
        else:
            validation["quarantine_path"] = None

        data = payload.get("data") if isinstance(payload.get("data"), list) else []
        meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}

        selected_data = data
        if args.max_records > 0:
            remaining = args.max_records - len(records)
            if remaining < len(selected_data):
                selected_data = selected_data[:remaining]
                stop_reason = "max_records_reached"

        if args.include_records:
            records.extend(selected_data)

        preview = [summarize_preview(item) for item in data[: args.preview_records]]
        page_summary = {
            "requested_page": page_number,
            "response_url": response.url,
            "status_code": response.status_code,
            "byte_length": response.byte_length,
            "meta": serialize_meta_summary(meta),
            "record_count": len(data),
            "preview_records": preview,
            "validation": validation,
            "rate_limit": extract_rate_limit(response.headers),
        }
        rate_limit_last = page_summary["rate_limit"]

        page_summaries.append(page_summary)
        pages_fetched += 1
        logger.info(
            "page-fetched page=%d records=%d hasNextPage=%s",
            page_number,
            len(data),
            meta.get("hasNextPage"),
        )

        if stop_reason == "max_records_reached":
            break

        has_next = bool(meta.get("hasNextPage"))
        total_pages = meta.get("totalPages")
        next_page = page_number + 1

        if not has_next:
            stop_reason = "no_next_page"
            break

        if is_int_not_bool(total_pages) and next_page > int(total_pages):
            stop_reason = "reached_total_pages"
            break

        page_number = next_page

    if stop_reason == "unknown":
        stop_reason = "completed"

    output_file = None
    if args.save_response and args.include_records:
        filter_start = (
            normalized_filters.get("start_utc")
            or normalized_filters.get("start_date")
            or "start"
        )
        filter_end = (
            normalized_filters.get("end_utc")
            or normalized_filters.get("end_date")
            or "end"
        )
        output_file_path = build_output_file_path(
            output_dir=Path(args.output_dir).expanduser().resolve(),
            output_file=args.output_file,
            filter_mode=args.filter_mode,
            filter_start=str(filter_start),
            filter_end=str(filter_end),
        )
        save_records_jsonl(path=output_file_path, records=records, overwrite=args.overwrite)
        output_file = str(output_file_path)
        logger.info("records-saved path=%s count=%d", output_file_path, len(records))

    result = {
        "ok": True,
        "source": "regulationsgov-v4-comments",
        "filter_mode": args.filter_mode,
        "filters": normalized_filters,
        "sort": sort_value,
        "start_page": args.start_page,
        "page_size": page_size,
        "max_pages": args.max_pages,
        "max_records": args.max_records,
        "pages_fetched": pages_fetched,
        "records_fetched": len(records) if args.include_records else None,
        "records_included": args.include_records,
        "save_response": args.save_response,
        "output_file": output_file,
        "stop_reason": stop_reason,
        "page_summaries": page_summaries,
        "validation_summary": {
            "pages_with_issues": pages_with_issues,
            "total_issue_count": issue_count_total,
            "failed": pages_with_issues > 0,
        },
        "rate_limit_last": rate_limit_last,
    }

    if args.include_records:
        result["records"] = records

    print_json(result, pretty=args.pretty)
    return 0


def add_runtime_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--base-url",
        default="",
        help=f"Override base URL. Default: {DEFAULT_BASE_URL}",
    )
    parser.add_argument(
        "--api-key",
        default="",
        help=f"Override API key. Env: {ENV_API_KEY}.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=None,
        help=f"HTTP timeout seconds. Env: {ENV_TIMEOUT_SECONDS}.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=None,
        help=f"Retry count for retriable failures. Env: {ENV_MAX_RETRIES}.",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=None,
        help=f"Initial retry delay in seconds. Env: {ENV_RETRY_BACKOFF_SECONDS}.",
    )
    parser.add_argument(
        "--retry-backoff-multiplier",
        type=float,
        default=None,
        help=f"Backoff multiplier between retries. Env: {ENV_RETRY_BACKOFF_MULTIPLIER}.",
    )
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=None,
        help=f"Minimum interval between requests. Env: {ENV_MIN_REQUEST_INTERVAL_SECONDS}.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=None,
        help=f"Results per page (5-250). Env: {ENV_PAGE_SIZE}.",
    )
    parser.add_argument(
        "--max-pages-per-run",
        type=int,
        default=None,
        help=f"Safety cap for --max-pages. Env: {ENV_MAX_PAGES_PER_RUN}.",
    )
    parser.add_argument(
        "--max-records-per-run",
        type=int,
        default=None,
        help=f"Safety cap for --max-records. Env: {ENV_MAX_RECORDS_PER_RUN}.",
    )
    parser.add_argument(
        "--max-retry-after-seconds",
        type=int,
        default=None,
        help=f"Maximum Retry-After to honor (seconds). Env: {ENV_MAX_RETRY_AFTER_SECONDS}.",
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
        help="Log level for stderr/log-file.",
    )
    parser.add_argument(
        "--log-file",
        default="",
        help="Optional log file path.",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch Regulations.gov v4 comments with pagination, retries, and validation."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Show effective runtime config and source URLs.")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch = sub.add_parser("fetch", help="Fetch comments by time window and optional filters.")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument(
        "--filter-mode",
        choices=["last-modified", "posted"],
        default="last-modified",
        help="last-modified uses filter[lastModifiedDate]; posted uses filter[postedDate].",
    )
    fetch.add_argument(
        "--start-datetime",
        default="",
        help="UTC start datetime for last-modified mode (YYYY-MM-DDTHH:MM:SSZ).",
    )
    fetch.add_argument(
        "--end-datetime",
        default="",
        help="UTC end datetime for last-modified mode (YYYY-MM-DDTHH:MM:SSZ).",
    )
    fetch.add_argument(
        "--start-date",
        default="",
        help="Start date for posted mode (YYYY-MM-DD).",
    )
    fetch.add_argument(
        "--end-date",
        default="",
        help="End date for posted mode (YYYY-MM-DD).",
    )
    fetch.add_argument(
        "--agency-id",
        default="",
        help="Optional filter[agencyId] value.",
    )
    fetch.add_argument(
        "--comment-on-id",
        default="",
        help="Optional filter[commentOnId] value.",
    )
    fetch.add_argument(
        "--search-term",
        default="",
        help="Optional filter[searchTerm] value.",
    )
    fetch.add_argument(
        "--sort",
        default="",
        help="Optional sort override, e.g. lastModifiedDate or -lastModifiedDate.",
    )
    fetch.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="Page number to start fetching from.",
    )
    fetch.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Maximum page count to fetch for this run.",
    )
    fetch.add_argument(
        "--max-records",
        type=int,
        default=0,
        help="Maximum records to include (0 means unlimited within configured cap).",
    )
    fetch.add_argument(
        "--include-records",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include fetched records in stdout JSON output.",
    )
    fetch.add_argument(
        "--preview-records",
        type=int,
        default=2,
        help="How many preview records to keep per page summary.",
    )
    fetch.add_argument(
        "--max-validation-issues",
        type=int,
        default=DEFAULT_MAX_VALIDATION_ISSUES,
        help=(
            "Maximum validation issue entries recorded per page. "
            f"Default: {DEFAULT_MAX_VALIDATION_ISSUES}."
        ),
    )
    fetch.add_argument(
        "--fail-on-validation-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail command if page structure validation reports issues.",
    )
    fetch.add_argument(
        "--quarantine-dir",
        default="",
        help="Optional directory to save validation issues in JSONL files.",
    )
    fetch.add_argument(
        "--save-response",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save fetched records to local JSONL file.",
    )
    fetch.add_argument(
        "--output-dir",
        default="./data/regulationsgov-comments",
        help="Directory for saved JSONL files when --save-response is enabled.",
    )
    fetch.add_argument(
        "--output-file",
        default="",
        help="Optional explicit output JSONL path.",
    )
    fetch.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists.",
    )
    fetch.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate request plan and print query without API calls.",
    )
    fetch.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "check-config":
            return command_check_config(args)
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
