#!/usr/bin/env python3
"""Search YouTube videos by query with retries, throttling, enrichment, and validation."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "YOUTUBE_BASE_URL"
ENV_API_KEY = "YOUTUBE_API_KEY"
ENV_TIMEOUT_SECONDS = "YOUTUBE_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "YOUTUBE_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "YOUTUBE_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "YOUTUBE_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "YOUTUBE_MIN_REQUEST_INTERVAL_SECONDS"
ENV_SEARCH_PAGE_SIZE = "YOUTUBE_SEARCH_PAGE_SIZE"
ENV_MAX_SEARCH_PAGES_PER_RUN = "YOUTUBE_MAX_SEARCH_PAGES_PER_RUN"
ENV_MAX_SEARCH_RESULTS_PER_RUN = "YOUTUBE_MAX_SEARCH_RESULTS_PER_RUN"
ENV_MAX_VIDEO_DETAILS_PER_RUN = "YOUTUBE_MAX_VIDEO_DETAILS_PER_RUN"
ENV_MAX_RETRY_AFTER_SECONDS = "YOUTUBE_MAX_RETRY_AFTER_SECONDS"
ENV_USER_AGENT = "YOUTUBE_USER_AGENT"

DEFAULT_BASE_URL = "https://www.googleapis.com/youtube/v3"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.6
DEFAULT_SEARCH_PAGE_SIZE = 25
DEFAULT_MAX_SEARCH_PAGES_PER_RUN = 10
DEFAULT_MAX_SEARCH_RESULTS_PER_RUN = 250
DEFAULT_MAX_VIDEO_DETAILS_PER_RUN = 250
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_USER_AGENT = "youtube-video-search/1.0"

SEARCH_PATH = "search"
VIDEOS_PATH = "videos"
MAX_SEARCH_API_PAGE_SIZE = 50
MAX_VIDEO_IDS_PER_REQUEST = 50
DEFAULT_MAX_VALIDATION_ISSUES = 30
RETRIABLE_HTTP_CODES = {403, 429, 500, 502, 503, 504}
RETRIABLE_GOOGLE_REASONS = {
    "backendError",
    "internalError",
    "rateLimitExceeded",
    "userRateLimitExceeded",
}
NON_RETRIABLE_GOOGLE_REASONS = {
    "accessNotConfigured",
    "dailyLimitExceeded",
    "forbidden",
    "ipRefererBlocked",
    "keyExpired",
    "keyInvalid",
    "quotaExceeded",
}
SEARCH_KIND = "youtube#searchListResponse"
VIDEO_KIND = "youtube#videoListResponse"
SEARCH_ORDER_VALUES = {"date", "rating", "relevance", "title", "videoCount", "viewCount"}
SAFE_SEARCH_VALUES = {"moderate", "none", "strict"}
VIDEO_CAPTION_VALUES = {"any", "closedCaption", "none"}
VIDEO_DEFINITION_VALUES = {"any", "high", "standard"}
VIDEO_DIMENSION_VALUES = {"2d", "3d", "any"}
VIDEO_DURATION_VALUES = {"any", "long", "medium", "short"}
VIDEO_EMBEDDABLE_VALUES = {"any", "true"}
VIDEO_EVENT_TYPE_VALUES = {"completed", "live", "upcoming"}
VIDEO_LICENSE_VALUES = {"any", "creativeCommon", "youtube"}
VIDEO_PAID_PRODUCT_PLACEMENT_VALUES = {"any", "true"}
VIDEO_SYNDICATED_VALUES = {"any", "true"}
VIDEO_TYPE_VALUES = {"any", "episode", "movie"}


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    api_key: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    search_page_size: int
    max_search_pages_per_run: int
    max_search_results_per_run: int
    max_video_details_per_run: int
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
        raise ValueError(f"Base URL must start with http:// or https://, got: {normalized!r}")
    return normalized


def mask_api_key(value: str) -> str:
    if len(value) <= 8:
        return "*" * len(value)
    return f"{value[:4]}...{value[-4:]}"


def sanitize_filename_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")
    return token[:80] or "query"


def parse_datetime_flexible(raw: str, *, field_name: str, is_end: bool) -> datetime:
    text = raw.strip()
    if not text:
        raise ValueError(f"{field_name} cannot be empty.")

    direct_candidates = [text]
    if text.endswith("Z"):
        direct_candidates.append(text[:-1] + "+00:00")

    for candidate in direct_candidates:
        try:
            dt = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if dt.tzinfo is None:
            if len(text) == 10:
                hour = 23 if is_end else 0
                minute = 59 if is_end else 0
                second = 59 if is_end else 0
                dt = datetime(dt.year, dt.month, dt.day, hour, minute, second)
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if fmt == "%Y-%m-%d":
            if is_end:
                dt = datetime(dt.year, dt.month, dt.day, 23, 59, 59)
            else:
                dt = datetime(dt.year, dt.month, dt.day, 0, 0, 0)
        return dt.replace(tzinfo=timezone.utc)

    raise ValueError(
        f"Invalid datetime {raw!r} for {field_name}. Use YYYY-MM-DDTHH:MM:SSZ or YYYY-MM-DD."
    )


def format_rfc3339(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_response_datetime(raw: Any) -> datetime | None:
    if not isinstance(raw, str):
        return None
    text = raw.strip()
    if not text:
        return None
    candidates = [text]
    if text.endswith("Z"):
        candidates.append(text[:-1] + "+00:00")
    for candidate in candidates:
        try:
            dt = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def ensure_page_size(value: int) -> int:
    if value < 1 or value > MAX_SEARCH_API_PAGE_SIZE:
        raise ValueError(
            f"Search page size must be between 1 and {MAX_SEARCH_API_PAGE_SIZE}, got: {value}"
        )
    return value


def ensure_choice(value: str, *, field_name: str, allowed: set[str]) -> str:
    normalized = value.strip()
    if not normalized:
        return normalized
    if normalized not in allowed:
        allowed_values = ", ".join(sorted(allowed))
        raise ValueError(f"{field_name} must be one of: {allowed_values}. Got: {value!r}")
    return normalized


def ensure_region_code(value: str, *, field_name: str) -> str:
    normalized = value.strip()
    if not normalized:
        return normalized
    if not re.fullmatch(r"[A-Za-z]{2}", normalized):
        raise ValueError(f"{field_name} must be a 2-letter ISO code, got: {value!r}")
    return normalized.upper()


def ensure_non_empty_query(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError("--query is required and cannot be empty.")
    return normalized


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
            "API key is required. Set --api-key or environment variable YOUTUBE_API_KEY."
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
    search_page_size = ensure_page_size(
        parse_positive_int(
            "--search-page-size",
            str(
                args.search_page_size
                if args.search_page_size is not None
                else env_or_default(ENV_SEARCH_PAGE_SIZE, str(DEFAULT_SEARCH_PAGE_SIZE))
            ),
        )
    )
    max_search_pages_per_run = parse_positive_int(
        "--max-search-pages-per-run",
        str(
            args.max_search_pages_per_run
            if args.max_search_pages_per_run is not None
            else env_or_default(
                ENV_MAX_SEARCH_PAGES_PER_RUN, str(DEFAULT_MAX_SEARCH_PAGES_PER_RUN)
            )
        ),
    )
    max_search_results_per_run = parse_positive_int(
        "--max-search-results-per-run",
        str(
            args.max_search_results_per_run
            if args.max_search_results_per_run is not None
            else env_or_default(
                ENV_MAX_SEARCH_RESULTS_PER_RUN, str(DEFAULT_MAX_SEARCH_RESULTS_PER_RUN)
            )
        ),
    )
    max_video_details_per_run = parse_positive_int(
        "--max-video-details-per-run",
        str(
            args.max_video_details_per_run
            if args.max_video_details_per_run is not None
            else env_or_default(
                ENV_MAX_VIDEO_DETAILS_PER_RUN, str(DEFAULT_MAX_VIDEO_DETAILS_PER_RUN)
            )
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
        search_page_size=search_page_size,
        max_search_pages_per_run=max_search_pages_per_run,
        max_search_results_per_run=max_search_results_per_run,
        max_video_details_per_run=max_video_details_per_run,
        max_retry_after_seconds=max_retry_after_seconds,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("youtube-video-search")
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


def maybe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        try:
            return int(value)
        except ValueError:
            return None
    return None


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


def render_query(base_url: str, path: str, query: dict[str, str]) -> str:
    encoded = parse.urlencode(query, doseq=False)
    if encoded:
        return f"{base_url}/{path}?{encoded}"
    return f"{base_url}/{path}"


def parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        seconds = float(text)
    except ValueError:
        try:
            dt = parsedate_to_datetime(text)
        except (TypeError, ValueError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return max(0.0, (dt.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())
    return seconds if seconds >= 0 else None


def parse_google_error(payload: bytes) -> dict[str, Any]:
    text = payload.decode("utf-8", errors="replace").strip()
    parsed: Any = None
    if text:
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None

    reasons: list[str] = []
    message = ""
    status = ""

    if isinstance(parsed, dict):
        error = parsed.get("error")
        if isinstance(error, dict):
            message = str(error.get("message", "")).strip()
            status = str(error.get("status", "")).strip()
            errors = error.get("errors")
            if isinstance(errors, list):
                for entry in errors:
                    if not isinstance(entry, dict):
                        continue
                    reason = str(entry.get("reason", "")).strip()
                    if reason:
                        reasons.append(reason)
                    if not message:
                        message = str(entry.get("message", "")).strip()
        elif not message:
            message = str(parsed.get("message", "")).strip()

    excerpt = message or text[:400]
    return {
        "message": excerpt[:400],
        "reasons": reasons,
        "status": status,
        "raw_text": text[:400],
    }


def extract_quota_headers(headers: dict[str, str]) -> dict[str, str]:
    info: dict[str, str] = {}
    for key in (
        "x-goog-request-id",
        "x-guploader-uploadid",
        "x-ratelimit-limit",
        "x-ratelimit-remaining",
        "retry-after",
    ):
        if key in headers:
            info[key] = headers[key]
    return info


class GoogleApiClient:
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

    def _compute_retry_delay(self, headers: dict[str, str], attempt: int) -> float:
        retry_after = parse_retry_after(headers.get("retry-after"))
        if retry_after is not None:
            if retry_after > self._cfg.max_retry_after_seconds:
                raise RuntimeError(
                    "Received Retry-After "
                    f"{retry_after}s which exceeds cap {self._cfg.max_retry_after_seconds}s. "
                    "Abort to avoid blocking too long; retry later or increase the cap."
                )
            return retry_after
        return self._cfg.retry_backoff_seconds * (
            self._cfg.retry_backoff_multiplier ** (attempt - 1)
        )

    def get_json(self, path: str, query: dict[str, str]) -> HttpJsonResponse:
        full_query = dict(query)
        full_query["key"] = self._cfg.api_key
        url = render_query(self._cfg.base_url, path, full_query)
        safe_url = render_query(self._cfg.base_url, path, query)
        attempts = self._cfg.max_retries + 1

        for attempt in range(1, attempts + 1):
            self._throttle()
            req = request.Request(url, method="GET")
            req.add_header("User-Agent", self._cfg.user_agent)
            req.add_header("Accept", "application/json")
            self._logger.info("http-get attempt=%d/%d url=%s", attempt, attempts, safe_url)

            try:
                with request.urlopen(req, timeout=self._cfg.timeout_seconds) as resp:
                    payload_raw = resp.read()
                    headers = {k.lower(): v for k, v in resp.headers.items()}
                    self._last_request_monotonic = time.monotonic()

                    status = int(getattr(resp, "status", 200))
                    quota_headers = extract_quota_headers(headers)
                    self._logger.info(
                        "http-ok status=%d bytes=%d url=%s quota_headers=%s",
                        status,
                        len(payload_raw),
                        safe_url,
                        quota_headers,
                    )

                    content_type = headers.get("content-type", "")
                    if "json" not in content_type.lower():
                        raise RuntimeError(
                            f"Unexpected content-type {content_type!r} for {safe_url}."
                        )

                    try:
                        text = payload_raw.decode("utf-8", errors="strict")
                    except UnicodeDecodeError as exc:
                        raise RuntimeError(
                            f"Response body is not valid UTF-8 for {safe_url}: {exc}"
                        ) from exc

                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError as exc:
                        raise RuntimeError(
                            f"Response body is not valid JSON for {safe_url}: {exc}"
                        ) from exc

                    if not isinstance(payload, dict):
                        raise RuntimeError(
                            f"Response JSON root must be object, got {type(payload).__name__}."
                        )

                    return HttpJsonResponse(
                        url=safe_url,
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
                error_info = parse_google_error(body)
                reasons = set(error_info["reasons"])
                quota_headers = extract_quota_headers(headers)
                retriable = status in RETRIABLE_HTTP_CODES and not reasons.intersection(
                    NON_RETRIABLE_GOOGLE_REASONS
                )
                if reasons.intersection(RETRIABLE_GOOGLE_REASONS):
                    retriable = True

                if retriable and attempt < attempts:
                    delay = self._compute_retry_delay(headers=headers, attempt=attempt)
                    self._logger.warning(
                        "http-retry status=%d delay=%.2fs attempt=%d/%d url=%s reasons=%s quota_headers=%s error=%s",
                        status,
                        delay,
                        attempt,
                        attempts,
                        safe_url,
                        sorted(reasons),
                        quota_headers,
                        error_info["message"],
                    )
                    time.sleep(delay)
                    continue

                raise RuntimeError(
                    f"HTTP {status} for {safe_url}. reasons={sorted(reasons)} "
                    f"quota_headers={quota_headers} error={error_info['message']!r}"
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
                        safe_url,
                        exc,
                    )
                    time.sleep(delay)
                    continue
                raise RuntimeError(f"Request failed for {safe_url}: {exc}") from exc

        raise RuntimeError(f"Failed to fetch after retries: {safe_url}")


def chunked(items: Iterable[str], size: int) -> Iterable[list[str]]:
    batch: list[str] = []
    for item in items:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def validate_search_page(
    payload: dict[str, Any],
    *,
    page_number: int,
    max_issues: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], str | None, int | None]:
    issues: list[dict[str, Any]] = []
    issue_count = 0

    if payload.get("kind") != SEARCH_KIND:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.kind",
                "reason": "unexpected_value",
                "expected": SEARCH_KIND,
                "actual": payload.get("kind"),
            },
        )

    items = payload.get("items")
    if not isinstance(items, list):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.items",
                "reason": "invalid_type",
                "expected": "array",
                "actual": type(items).__name__,
            },
        )
        items = []

    page_info = payload.get("pageInfo")
    if not isinstance(page_info, dict):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.pageInfo",
                "reason": "invalid_type",
                "expected": "object",
                "actual": type(page_info).__name__,
            },
        )
        page_info = {}

    total_results = page_info.get("totalResults")
    if total_results is not None and not is_int_not_bool(total_results):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.pageInfo.totalResults",
                "reason": "invalid_type",
                "expected": "integer",
                "actual": type(total_results).__name__,
            },
        )

    results_per_page = page_info.get("resultsPerPage")
    if results_per_page is not None and not is_int_not_bool(results_per_page):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.pageInfo.resultsPerPage",
                "reason": "invalid_type",
                "expected": "integer",
                "actual": type(results_per_page).__name__,
            },
        )

    normalized_items: list[dict[str, Any]] = []
    for item_index, item in enumerate(items):
        item_location = f"root.items[{item_index}]"
        if not isinstance(item, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": item_location,
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(item).__name__,
                },
            )
            continue

        item_id = item.get("id")
        snippet = item.get("snippet")
        if not isinstance(item_id, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.id",
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(item_id).__name__,
                },
            )
            continue
        if not isinstance(snippet, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.snippet",
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(snippet).__name__,
                },
            )
            continue

        video_id = item_id.get("videoId")
        if not isinstance(video_id, str) or not video_id.strip():
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.id.videoId",
                    "reason": "missing_or_invalid",
                    "actual": video_id,
                },
            )
            continue

        published_at = snippet.get("publishedAt")
        if parse_response_datetime(published_at) is None:
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.snippet.publishedAt",
                    "reason": "invalid_datetime_format",
                    "actual": published_at,
                },
            )

        normalized_items.append(
            {
                "video_id": video_id,
                "search_page": page_number,
                "title": str(snippet.get("title", "")),
                "description": str(snippet.get("description", "")),
                "channel_id": str(snippet.get("channelId", "")),
                "channel_title": str(snippet.get("channelTitle", "")),
                "published_at": str(published_at or ""),
                "live_broadcast_content": str(snippet.get("liveBroadcastContent", "")),
                "thumbnails": snippet.get("thumbnails", {}),
            }
        )

    return (
        {
            "passed": issue_count == 0,
            "issue_count": issue_count,
            "reported_issue_count": len(issues),
            "issues": issues,
        },
        normalized_items,
        payload.get("nextPageToken") if isinstance(payload.get("nextPageToken"), str) else None,
        total_results if is_int_not_bool(total_results) else None,
    )


def validate_video_details_page(
    payload: dict[str, Any],
    *,
    requested_ids: list[str],
    max_issues: int,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    issues: list[dict[str, Any]] = []
    issue_count = 0

    if payload.get("kind") != VIDEO_KIND:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.kind",
                "reason": "unexpected_value",
                "expected": VIDEO_KIND,
                "actual": payload.get("kind"),
            },
        )

    items = payload.get("items")
    if not isinstance(items, list):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.items",
                "reason": "invalid_type",
                "expected": "array",
                "actual": type(items).__name__,
            },
        )
        items = []

    detail_map: dict[str, dict[str, Any]] = {}
    seen_ids: set[str] = set()
    for item_index, item in enumerate(items):
        item_location = f"root.items[{item_index}]"
        if not isinstance(item, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": item_location,
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(item).__name__,
                },
            )
            continue

        video_id = item.get("id")
        if not isinstance(video_id, str) or not video_id.strip():
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.id",
                    "reason": "missing_or_invalid",
                    "actual": video_id,
                },
            )
            continue

        if video_id in seen_ids:
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.id",
                    "reason": "duplicate_video_id",
                    "actual": video_id,
                },
            )
        seen_ids.add(video_id)

        snippet = item.get("snippet")
        statistics = item.get("statistics")
        content_details = item.get("contentDetails")
        status = item.get("status")
        live_streaming = item.get("liveStreamingDetails")

        if not isinstance(snippet, dict):
            snippet = {}
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.snippet",
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(item.get("snippet")).__name__,
                },
            )
        if not isinstance(statistics, dict):
            statistics = {}
        if not isinstance(content_details, dict):
            content_details = {}
        if not isinstance(status, dict):
            status = {}
        if not isinstance(live_streaming, dict):
            live_streaming = {}

        published_at = snippet.get("publishedAt")
        if published_at and parse_response_datetime(published_at) is None:
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{item_location}.snippet.publishedAt",
                    "reason": "invalid_datetime_format",
                    "actual": published_at,
                },
            )

        detail_map[video_id] = {
            "id": video_id,
            "title": str(snippet.get("title", "")),
            "description": str(snippet.get("description", "")),
            "channel_id": str(snippet.get("channelId", "")),
            "channel_title": str(snippet.get("channelTitle", "")),
            "published_at": str(published_at or ""),
            "default_language": str(snippet.get("defaultLanguage", "")),
            "default_audio_language": str(snippet.get("defaultAudioLanguage", "")),
            "category_id": str(snippet.get("categoryId", "")),
            "tags": snippet.get("tags", []),
            "statistics": {
                "view_count": maybe_int(statistics.get("viewCount")),
                "like_count": maybe_int(statistics.get("likeCount")),
                "comment_count": maybe_int(statistics.get("commentCount")),
            },
            "content_details": {
                "duration": str(content_details.get("duration", "")),
                "dimension": str(content_details.get("dimension", "")),
                "definition": str(content_details.get("definition", "")),
                "caption": str(content_details.get("caption", "")),
                "licensed_content": content_details.get("licensedContent"),
                "projection": str(content_details.get("projection", "")),
            },
            "status": {
                "privacy_status": str(status.get("privacyStatus", "")),
                "license": str(status.get("license", "")),
                "embeddable": status.get("embeddable"),
                "made_for_kids": status.get("madeForKids"),
                "self_declared_made_for_kids": status.get("selfDeclaredMadeForKids"),
            },
            "live_streaming_details": {
                "actual_start_time": str(live_streaming.get("actualStartTime", "")),
                "actual_end_time": str(live_streaming.get("actualEndTime", "")),
                "scheduled_start_time": str(live_streaming.get("scheduledStartTime", "")),
                "scheduled_end_time": str(live_streaming.get("scheduledEndTime", "")),
            },
        }

    missing_ids = [video_id for video_id in requested_ids if video_id not in detail_map]
    for video_id in missing_ids:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.items",
                "reason": "missing_requested_video_id",
                "video_id": video_id,
            },
        )

    return (
        {
            "passed": issue_count == 0,
            "issue_count": issue_count,
            "reported_issue_count": len(issues),
            "issues": issues,
        },
        detail_map,
    )


def write_quarantine_issues(
    *,
    quarantine_dir: Path,
    name: str,
    issues: list[dict[str, Any]],
) -> Path | None:
    if not issues:
        return None
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    output_path = quarantine_dir / f"{sanitize_filename_token(name)}.validation-issues.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for issue in issues:
            handle.write(json.dumps(issue, ensure_ascii=False))
            handle.write("\n")
    return output_path


def build_output_file_path(*, output_dir: Path, output_file: str, query: str) -> Path:
    if output_file.strip():
        return Path(output_file).expanduser().resolve()
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    token = sanitize_filename_token(query)[:40]
    filename = f"youtube-videos-{token}-{timestamp}.jsonl"
    return (output_dir / filename).resolve()


def save_records_jsonl(*, path: Path, records: list[dict[str, Any]], overwrite: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not overwrite:
        raise RuntimeError(f"Output file already exists: {path}")
    with path.open("w", encoding="utf-8") as handle:
        for item in records:
            handle.write(json.dumps(item, ensure_ascii=False))
            handle.write("\n")


def print_json(payload: dict[str, Any], pretty: bool) -> None:
    print(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2 if pretty else None,
            separators=None if pretty else (",", ":"),
        )
    )


def build_search_query(args: argparse.Namespace, page_size: int, page_token: str) -> dict[str, str]:
    query: dict[str, str] = {
        "part": "snippet",
        "type": "video",
        "q": args.query,
        "maxResults": str(page_size),
        "order": args.order,
    }
    if page_token:
        query["pageToken"] = page_token
    if args.channel_id.strip():
        query["channelId"] = args.channel_id.strip()
    if args.published_after:
        query["publishedAfter"] = args.published_after
    if args.published_before:
        query["publishedBefore"] = args.published_before
    if args.region_code:
        query["regionCode"] = args.region_code
    if args.relevance_language.strip():
        query["relevanceLanguage"] = args.relevance_language.strip()
    if args.safe_search:
        query["safeSearch"] = args.safe_search
    if args.video_caption:
        query["videoCaption"] = args.video_caption
    if args.video_definition:
        query["videoDefinition"] = args.video_definition
    if args.video_dimension:
        query["videoDimension"] = args.video_dimension
    if args.video_duration:
        query["videoDuration"] = args.video_duration
    if args.video_embeddable:
        query["videoEmbeddable"] = args.video_embeddable
    if args.video_event_type:
        query["eventType"] = args.video_event_type
    if args.video_license:
        query["videoLicense"] = args.video_license
    if args.video_paid_product_placement:
        query["videoPaidProductPlacement"] = args.video_paid_product_placement
    if args.video_syndicated:
        query["videoSyndicated"] = args.video_syndicated
    if args.video_type:
        query["videoType"] = args.video_type
    return query


def build_video_details_query(video_ids: list[str]) -> dict[str, str]:
    return {
        "part": "snippet,statistics,contentDetails,status,liveStreamingDetails",
        "id": ",".join(video_ids),
        "maxResults": str(len(video_ids)),
    }


def merged_video_record(
    *,
    search_item: dict[str, Any],
    detail: dict[str, Any] | None,
    query: str,
    order: str,
    client_side_filter_reasons: list[str],
) -> dict[str, Any]:
    base_statistics = {
        "view_count": None,
        "like_count": None,
        "comment_count": None,
    }
    base_content_details = {
        "duration": "",
        "dimension": "",
        "definition": "",
        "caption": "",
        "licensed_content": None,
        "projection": "",
    }
    base_status = {
        "privacy_status": "",
        "license": "",
        "embeddable": None,
        "made_for_kids": None,
        "self_declared_made_for_kids": None,
    }
    base_live_streaming = {
        "actual_start_time": "",
        "actual_end_time": "",
        "scheduled_start_time": "",
        "scheduled_end_time": "",
    }
    detail = detail or {}
    statistics = detail.get("statistics", base_statistics)
    content_details = detail.get("content_details", base_content_details)
    status = detail.get("status", base_status)
    live_streaming_details = detail.get("live_streaming_details", base_live_streaming)

    return {
        "video_id": search_item["video_id"],
        "query": query,
        "search_rank": search_item["search_rank"],
        "search_page": search_item["search_page"],
        "search_position": search_item["search_position"],
        "search_match": {
            "query": query,
            "order": order,
            "page": search_item["search_page"],
            "position": search_item["search_position"],
        },
        "video": {
            "id": search_item["video_id"],
            "title": detail.get("title") or search_item["title"],
            "description": detail.get("description") or search_item["description"],
            "channel_id": detail.get("channel_id") or search_item["channel_id"],
            "channel_title": detail.get("channel_title") or search_item["channel_title"],
            "published_at": detail.get("published_at") or search_item["published_at"],
            "default_language": detail.get("default_language", ""),
            "default_audio_language": detail.get("default_audio_language", ""),
            "category_id": detail.get("category_id", ""),
            "tags": detail.get("tags", []),
            "live_broadcast_content": search_item["live_broadcast_content"],
            "thumbnails": search_item["thumbnails"],
            "statistics": statistics,
            "content_details": content_details,
            "status": status,
            "live_streaming_details": live_streaming_details,
        },
        "client_side_filter_reasons": client_side_filter_reasons,
    }


def should_keep_record(record: dict[str, Any], args: argparse.Namespace) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    stats = record["video"]["statistics"]
    comment_count = stats.get("comment_count")
    view_count = stats.get("view_count")

    if args.skip_without_comments and (comment_count is None or comment_count <= 0):
        reasons.append("no_public_comments")
    if args.comment_count_min > 0 and (comment_count is None or comment_count < args.comment_count_min):
        reasons.append("comment_count_below_min")
    if args.view_count_min > 0 and (view_count is None or view_count < args.view_count_min):
        reasons.append("view_count_below_min")

    return not reasons, reasons


def enforce_run_caps(args: argparse.Namespace, config: RuntimeConfig) -> None:
    if args.max_pages > config.max_search_pages_per_run:
        raise ValueError(
            f"--max-pages {args.max_pages} exceeds YOUTUBE_MAX_SEARCH_PAGES_PER_RUN "
            f"{config.max_search_pages_per_run}."
        )
    if args.max_results > config.max_search_results_per_run:
        raise ValueError(
            f"--max-results {args.max_results} exceeds YOUTUBE_MAX_SEARCH_RESULTS_PER_RUN "
            f"{config.max_search_results_per_run}."
        )


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
            "search_page_size": config.search_page_size,
            "max_search_pages_per_run": config.max_search_pages_per_run,
            "max_search_results_per_run": config.max_search_results_per_run,
            "max_video_details_per_run": config.max_video_details_per_run,
            "max_retry_after_seconds": config.max_retry_after_seconds,
            "user_agent": config.user_agent,
        },
        "source_urls": {
            "search": f"{config.base_url}/{SEARCH_PATH}",
            "videos": f"{config.base_url}/{VIDEOS_PATH}",
        },
        "env_keys": {
            "base_url": ENV_BASE_URL,
            "api_key": ENV_API_KEY,
            "timeout_seconds": ENV_TIMEOUT_SECONDS,
            "max_retries": ENV_MAX_RETRIES,
            "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
            "retry_backoff_multiplier": ENV_RETRY_BACKOFF_MULTIPLIER,
            "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
            "search_page_size": ENV_SEARCH_PAGE_SIZE,
            "max_search_pages_per_run": ENV_MAX_SEARCH_PAGES_PER_RUN,
            "max_search_results_per_run": ENV_MAX_SEARCH_RESULTS_PER_RUN,
            "max_video_details_per_run": ENV_MAX_VIDEO_DETAILS_PER_RUN,
            "max_retry_after_seconds": ENV_MAX_RETRY_AFTER_SECONDS,
            "user_agent": ENV_USER_AGENT,
        },
    }
    print_json(payload, pretty=args.pretty)
    return 0


def command_search(args: argparse.Namespace) -> int:
    logger = build_logger(level=args.log_level, log_file=args.log_file)
    config = build_runtime_config(args)
    enforce_run_caps(args, config)

    if args.max_validation_issues < 1:
        raise ValueError("--max-validation-issues must be >= 1.")

    args.query = ensure_non_empty_query(args.query)
    args.order = ensure_choice(args.order, field_name="--order", allowed=SEARCH_ORDER_VALUES)
    args.safe_search = ensure_choice(
        args.safe_search, field_name="--safe-search", allowed=SAFE_SEARCH_VALUES
    )
    args.video_caption = ensure_choice(
        args.video_caption, field_name="--video-caption", allowed=VIDEO_CAPTION_VALUES
    )
    args.video_definition = ensure_choice(
        args.video_definition, field_name="--video-definition", allowed=VIDEO_DEFINITION_VALUES
    )
    args.video_dimension = ensure_choice(
        args.video_dimension, field_name="--video-dimension", allowed=VIDEO_DIMENSION_VALUES
    )
    args.video_duration = ensure_choice(
        args.video_duration, field_name="--video-duration", allowed=VIDEO_DURATION_VALUES
    )
    args.video_embeddable = ensure_choice(
        args.video_embeddable,
        field_name="--video-embeddable",
        allowed=VIDEO_EMBEDDABLE_VALUES,
    )
    args.video_event_type = ensure_choice(
        args.video_event_type,
        field_name="--video-event-type",
        allowed=VIDEO_EVENT_TYPE_VALUES,
    )
    args.video_license = ensure_choice(
        args.video_license, field_name="--video-license", allowed=VIDEO_LICENSE_VALUES
    )
    args.video_paid_product_placement = ensure_choice(
        args.video_paid_product_placement,
        field_name="--video-paid-product-placement",
        allowed=VIDEO_PAID_PRODUCT_PLACEMENT_VALUES,
    )
    args.video_syndicated = ensure_choice(
        args.video_syndicated, field_name="--video-syndicated", allowed=VIDEO_SYNDICATED_VALUES
    )
    args.video_type = ensure_choice(
        args.video_type, field_name="--video-type", allowed=VIDEO_TYPE_VALUES
    )
    args.region_code = ensure_region_code(args.region_code, field_name="--region-code")

    published_after_dt: datetime | None = None
    published_before_dt: datetime | None = None
    if args.published_after.strip():
        published_after_dt = parse_datetime_flexible(
            args.published_after, field_name="--published-after", is_end=False
        )
        args.published_after = format_rfc3339(published_after_dt)
    else:
        args.published_after = ""
    if args.published_before.strip():
        published_before_dt = parse_datetime_flexible(
            args.published_before, field_name="--published-before", is_end=True
        )
        args.published_before = format_rfc3339(published_before_dt)
    else:
        args.published_before = ""

    if published_after_dt and published_before_dt and published_after_dt >= published_before_dt:
        raise ValueError("--published-after must be earlier than --published-before.")

    need_video_details = (
        args.include_video_details
        or args.skip_without_comments
        or args.comment_count_min > 0
        or args.view_count_min > 0
    )

    if args.max_results < 1:
        raise ValueError("--max-results must be >= 1.")
    if args.max_pages < 1:
        raise ValueError("--max-pages must be >= 1.")
    if args.comment_count_min < 0:
        raise ValueError("--comment-count-min must be >= 0.")
    if args.view_count_min < 0:
        raise ValueError("--view-count-min must be >= 0.")

    page_size = min(args.page_size, config.search_page_size)
    if page_size < 1:
        raise ValueError("--page-size must be >= 1.")

    sample_query = build_search_query(args, page_size=page_size, page_token="")
    if args.dry_run:
        payload = {
            "ok": True,
            "dry_run": True,
            "request_plan": {
                "query": args.query,
                "order": args.order,
                "channel_id": args.channel_id or None,
                "published_after": args.published_after or None,
                "published_before": args.published_before or None,
                "region_code": args.region_code or None,
                "relevance_language": args.relevance_language or None,
                "safe_search": args.safe_search or None,
                "page_size_effective": page_size,
                "max_pages": args.max_pages,
                "max_results": args.max_results,
                "need_video_details": need_video_details,
                "client_side_filters": {
                    "skip_without_comments": args.skip_without_comments,
                    "comment_count_min": args.comment_count_min,
                    "view_count_min": args.view_count_min,
                },
            },
            "sample_request_url": render_query(config.base_url, SEARCH_PATH, sample_query),
        }
        print_json(payload, pretty=args.pretty)
        return 0

    client = GoogleApiClient(config=config, logger=logger)

    search_issue_count_total = 0
    detail_issue_count_total = 0
    duplicate_video_count = 0
    filtered_out_count = 0
    page_trace: list[dict[str, Any]] = []
    detail_trace: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    search_candidates: list[dict[str, Any]] = []
    seen_video_ids: set[str] = set()
    approx_total_results: int | None = None
    search_stop_reason = "max_pages_reached"
    page_token = ""

    for page_number in range(1, args.max_pages + 1):
        query = build_search_query(args, page_size=page_size, page_token=page_token)
        response = client.get_json(SEARCH_PATH, query)
        validation, normalized_items, next_page_token, total_results = validate_search_page(
            response.payload,
            page_number=page_number,
            max_issues=args.max_validation_issues,
        )
        search_issue_count_total += validation["issue_count"]

        if validation["issue_count"] > 0:
            logger.warning(
                "search-validation-issues page=%d issues=%d",
                page_number,
                validation["issue_count"],
            )
            if args.quarantine_dir.strip():
                write_quarantine_issues(
                    quarantine_dir=Path(args.quarantine_dir).expanduser().resolve(),
                    name=f"search-page-{page_number:04d}",
                    issues=validation["issues"],
                )

        kept_on_page = 0
        duplicates_on_page = 0
        for item_index, item in enumerate(normalized_items, start=1):
            item["search_position"] = item_index
            item["search_rank"] = len(search_candidates) + 1
            if item["video_id"] in seen_video_ids:
                duplicate_video_count += 1
                duplicates_on_page += 1
                continue
            seen_video_ids.add(item["video_id"])
            search_candidates.append(item)
            kept_on_page += 1
            if len(search_candidates) >= args.max_results:
                search_stop_reason = "max_results_reached"
                break

        page_trace.append(
            {
                "page_number": page_number,
                "items_returned": len(normalized_items),
                "items_kept": kept_on_page,
                "duplicate_video_count": duplicates_on_page,
                "next_page_token_present": bool(next_page_token),
                "byte_length": response.byte_length,
                "approx_total_results": total_results,
            }
        )

        logger.info(
            "search-page page=%d kept=%d duplicates=%d accumulated=%d next_page=%s",
            page_number,
            kept_on_page,
            duplicates_on_page,
            len(search_candidates),
            bool(next_page_token),
        )

        if total_results is not None:
            approx_total_results = total_results
        if len(search_candidates) >= args.max_results:
            break
        if not next_page_token:
            search_stop_reason = "no_next_page"
            break
        page_token = next_page_token

    if len(search_candidates) > config.max_video_details_per_run:
        raise ValueError(
            f"Collected {len(search_candidates)} candidate videos, exceeds configured detail cap "
            f"{config.max_video_details_per_run} (set by --max-video-details-per-run or "
            f"{ENV_MAX_VIDEO_DETAILS_PER_RUN})."
        )

    detail_map: dict[str, dict[str, Any]] = {}
    if need_video_details and search_candidates:
        for batch_index, batch in enumerate(
            chunked((item["video_id"] for item in search_candidates), MAX_VIDEO_IDS_PER_REQUEST),
            start=1,
        ):
            query = build_video_details_query(batch)
            response = client.get_json(VIDEOS_PATH, query)
            validation, batch_detail_map = validate_video_details_page(
                response.payload,
                requested_ids=batch,
                max_issues=args.max_validation_issues,
            )
            detail_issue_count_total += validation["issue_count"]
            detail_trace.append(
                {
                    "batch_index": batch_index,
                    "requested_ids": len(batch),
                    "received_ids": len(batch_detail_map),
                    "byte_length": response.byte_length,
                    "issue_count": validation["issue_count"],
                }
            )
            if validation["issue_count"] > 0:
                logger.warning(
                    "video-details-validation-issues batch=%d issues=%d",
                    batch_index,
                    validation["issue_count"],
                )
                if args.quarantine_dir.strip():
                    write_quarantine_issues(
                        quarantine_dir=Path(args.quarantine_dir).expanduser().resolve(),
                        name=f"video-details-batch-{batch_index:04d}",
                        issues=validation["issues"],
                    )
            detail_map.update(batch_detail_map)

    for item in search_candidates:
        record = merged_video_record(
            search_item=item,
            detail=detail_map.get(item["video_id"]),
            query=args.query,
            order=args.order,
            client_side_filter_reasons=[],
        )
        keep, filter_reasons = should_keep_record(record, args)
        record["client_side_filter_reasons"] = filter_reasons
        if not keep:
            filtered_out_count += 1
            logger.info(
                "video-filtered video_id=%s reasons=%s",
                item["video_id"],
                filter_reasons,
            )
            continue
        records.append(record)

    output_file_path: Path | None = None
    if args.save_records and records:
        output_dir = Path(args.output_dir).expanduser().resolve()
        output_file_path = build_output_file_path(
            output_dir=output_dir,
            output_file=args.output_file,
            query=args.query,
        )
        save_records_jsonl(path=output_file_path, records=records, overwrite=args.overwrite)

    estimated_quota_units = (len(page_trace) * 100) + len(detail_trace)
    payload = {
        "ok": not (args.fail_on_validation_error and (search_issue_count_total + detail_issue_count_total) > 0),
        "request": {
            "query": args.query,
            "order": args.order,
            "channel_id": args.channel_id or None,
            "published_after": args.published_after or None,
            "published_before": args.published_before or None,
            "region_code": args.region_code or None,
            "relevance_language": args.relevance_language or None,
            "safe_search": args.safe_search or None,
            "page_size": page_size,
            "max_pages": args.max_pages,
            "max_results": args.max_results,
            "need_video_details": need_video_details,
        },
        "search_summary": {
            "candidate_count": len(search_candidates),
            "record_count": len(records),
            "duplicate_video_count": duplicate_video_count,
            "filtered_out_count": filtered_out_count,
            "page_count": len(page_trace),
            "stop_reason": search_stop_reason,
            "approx_total_results": approx_total_results,
            "estimated_quota_units": estimated_quota_units,
        },
        "validation_summary": {
            "search_issue_count": search_issue_count_total,
            "detail_issue_count": detail_issue_count_total,
            "total_issue_count": search_issue_count_total + detail_issue_count_total,
        },
        "page_trace": page_trace,
        "detail_trace": detail_trace,
        "artifacts": {
            "output_jsonl": str(output_file_path) if output_file_path is not None else None,
        },
    }
    if args.include_records:
        payload["records"] = records

    print_json(payload, pretty=args.pretty)
    return 0 if payload["ok"] else 1


def add_runtime_config_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--base-url", default="", help="YouTube API base URL override.")
    parser.add_argument("--api-key", default="", help="YouTube Data API key override.")
    parser.add_argument("--timeout-seconds", type=int, default=None, help="HTTP timeout override.")
    parser.add_argument("--max-retries", type=int, default=None, help="Retry count override.")
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=None,
        help="Initial retry backoff in seconds.",
    )
    parser.add_argument(
        "--retry-backoff-multiplier",
        type=float,
        default=None,
        help="Exponential retry multiplier.",
    )
    parser.add_argument(
        "--min-request-interval-seconds",
        type=float,
        default=None,
        help="Minimum interval between requests.",
    )
    parser.add_argument(
        "--search-page-size",
        type=int,
        default=None,
        help="Configured maximum search page size from env override.",
    )
    parser.add_argument(
        "--max-search-pages-per-run",
        type=int,
        default=None,
        help="Configured maximum search pages per run override.",
    )
    parser.add_argument(
        "--max-search-results-per-run",
        type=int,
        default=None,
        help="Configured maximum search results per run override.",
    )
    parser.add_argument(
        "--max-video-details-per-run",
        type=int,
        default=None,
        help="Configured maximum enriched videos per run override.",
    )
    parser.add_argument(
        "--max-retry-after-seconds",
        type=int,
        default=None,
        help="Maximum Retry-After accepted before fail-fast.",
    )
    parser.add_argument("--user-agent", default=None, help="User-Agent header override.")


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for stderr/log file. Default: INFO.",
    )
    parser.add_argument("--log-file", default="", help="Optional log file path.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Search YouTube videos by query with retries, throttling, and validation."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Show effective runtime config and source URLs.")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    search = sub.add_parser("search", help="Search videos and optionally enrich them with details.")
    add_runtime_config_args(search)
    add_logging_args(search)
    search.add_argument("--query", required=True, help="Search query string sent to search.list.")
    search.add_argument("--channel-id", default="", help="Optional channel ID filter.")
    search.add_argument(
        "--published-after",
        default="",
        help="Optional earliest video publish time. Accepts YYYY-MM-DD or RFC3339.",
    )
    search.add_argument(
        "--published-before",
        default="",
        help="Optional latest video publish time. Accepts YYYY-MM-DD or RFC3339.",
    )
    search.add_argument(
        "--order",
        default="relevance",
        help="Search ordering. One of: date,rating,relevance,title,videoCount,viewCount.",
    )
    search.add_argument("--region-code", default="", help="Optional 2-letter region code.")
    search.add_argument(
        "--relevance-language",
        default="",
        help="Optional relevance language hint, e.g. en, zh-CN.",
    )
    search.add_argument(
        "--safe-search",
        default="moderate",
        help="SafeSearch mode. One of: moderate,none,strict.",
    )
    search.add_argument(
        "--video-caption",
        default="",
        help="Optional filter. One of: any,closedCaption,none.",
    )
    search.add_argument(
        "--video-definition",
        default="",
        help="Optional filter. One of: any,high,standard.",
    )
    search.add_argument(
        "--video-dimension",
        default="",
        help="Optional filter. One of: 2d,3d,any.",
    )
    search.add_argument(
        "--video-duration",
        default="",
        help="Optional filter. One of: any,long,medium,short.",
    )
    search.add_argument(
        "--video-embeddable",
        default="",
        help="Optional filter. One of: any,true.",
    )
    search.add_argument(
        "--video-event-type",
        default="",
        help="Optional broadcast filter. One of: completed,live,upcoming.",
    )
    search.add_argument(
        "--video-license",
        default="",
        help="Optional filter. One of: any,creativeCommon,youtube.",
    )
    search.add_argument(
        "--video-paid-product-placement",
        default="",
        help="Optional filter. One of: any,true.",
    )
    search.add_argument(
        "--video-syndicated",
        default="",
        help="Optional filter. One of: any,true.",
    )
    search.add_argument(
        "--video-type",
        default="",
        help="Optional filter. One of: any,episode,movie.",
    )
    search.add_argument(
        "--page-size",
        type=int,
        default=25,
        help="Per-page search request size. Effective value is min(--page-size, configured cap).",
    )
    search.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum search pages to request.",
    )
    search.add_argument(
        "--max-results",
        type=int,
        default=100,
        help="Maximum distinct video candidates to keep.",
    )
    search.add_argument(
        "--include-video-details",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Call videos.list to enrich candidates with statistics/content/status.",
    )
    search.add_argument(
        "--skip-without-comments",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Drop videos whose public statistics report zero/missing comment count.",
    )
    search.add_argument(
        "--comment-count-min",
        type=int,
        default=0,
        help="Client-side minimum comment count filter after videos.list enrichment.",
    )
    search.add_argument(
        "--view-count-min",
        type=int,
        default=0,
        help="Client-side minimum view count filter after videos.list enrichment.",
    )
    search.add_argument(
        "--include-records",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include normalized records in stdout JSON.",
    )
    search.add_argument(
        "--max-validation-issues",
        type=int,
        default=DEFAULT_MAX_VALIDATION_ISSUES,
        help=f"Maximum retained validation issues per page/batch. Default: {DEFAULT_MAX_VALIDATION_ISSUES}.",
    )
    search.add_argument(
        "--fail-on-validation-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exit non-zero if validation issues are found.",
    )
    search.add_argument(
        "--quarantine-dir",
        default="",
        help="Optional directory to save validation issue JSONL files.",
    )
    search.add_argument(
        "--save-records",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save normalized records into JSONL.",
    )
    search.add_argument(
        "--output-dir",
        default="./data/youtube-videos",
        help="Output directory for saved JSONL.",
    )
    search.add_argument("--output-file", default="", help="Optional explicit output JSONL path.")
    search.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it already exists.",
    )
    search.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate arguments and print a request plan without remote calls.",
    )
    search.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "check-config":
            return command_check_config(args)
        if args.command == "search":
            return command_search(args)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
