#!/usr/bin/env python3
"""Fetch YouTube public comments for video IDs with retries, throttling, and validation."""

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
ENV_COMMENTS_PAGE_SIZE = "YOUTUBE_COMMENTS_PAGE_SIZE"
ENV_MAX_VIDEOS_PER_RUN = "YOUTUBE_MAX_VIDEOS_PER_RUN"
ENV_MAX_THREAD_PAGES_PER_RUN = "YOUTUBE_MAX_THREAD_PAGES_PER_RUN"
ENV_MAX_REPLY_PAGES_PER_RUN = "YOUTUBE_MAX_REPLY_PAGES_PER_RUN"
ENV_MAX_THREADS_PER_RUN = "YOUTUBE_MAX_THREADS_PER_RUN"
ENV_MAX_COMMENTS_PER_RUN = "YOUTUBE_MAX_COMMENTS_PER_RUN"
ENV_MAX_RETRY_AFTER_SECONDS = "YOUTUBE_MAX_RETRY_AFTER_SECONDS"
ENV_USER_AGENT = "YOUTUBE_USER_AGENT"

DEFAULT_BASE_URL = "https://www.googleapis.com/youtube/v3"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 0.6
DEFAULT_COMMENTS_PAGE_SIZE = 100
DEFAULT_MAX_VIDEOS_PER_RUN = 50
DEFAULT_MAX_THREAD_PAGES_PER_RUN = 20
DEFAULT_MAX_REPLY_PAGES_PER_RUN = 40
DEFAULT_MAX_THREADS_PER_RUN = 1000
DEFAULT_MAX_COMMENTS_PER_RUN = 5000
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_USER_AGENT = "youtube-comments-fetch/1.0"

COMMENT_THREADS_PATH = "commentThreads"
COMMENTS_PATH = "comments"
MAX_COMMENTS_API_PAGE_SIZE = 100
YOUTUBE_VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")
RETRIABLE_HTTP_CODES = {403, 429, 500, 502, 503, 504}
RETRIABLE_GOOGLE_REASONS = {
    "backendError",
    "internalError",
    "rateLimitExceeded",
    "userRateLimitExceeded",
}
NON_RETRIABLE_GOOGLE_REASONS = {
    "accessNotConfigured",
    "commentNotFound",
    "commentsDisabled",
    "dailyLimitExceeded",
    "forbidden",
    "ipRefererBlocked",
    "keyExpired",
    "keyInvalid",
    "quotaExceeded",
    "videoNotFound",
}
COMMENT_THREAD_KIND = "youtube#commentThreadListResponse"
COMMENT_LIST_KIND = "youtube#commentListResponse"
TEXT_FORMAT_VALUES = {"html", "plainText"}
THREAD_ORDER_VALUES = {"relevance", "time"}
TIME_FIELD_VALUES = {"published", "updated"}
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
    comments_page_size: int
    max_videos_per_run: int
    max_thread_pages_per_run: int
    max_reply_pages_per_run: int
    max_threads_per_run: int
    max_comments_per_run: int
    max_retry_after_seconds: int
    user_agent: str


@dataclass(frozen=True)
class HttpJsonResponse:
    url: str
    status_code: int
    headers: dict[str, str]
    payload: dict[str, Any]
    byte_length: int


class ApiRequestError(RuntimeError):
    def __init__(
        self,
        *,
        status_code: int,
        url: str,
        reasons: list[str],
        message: str,
        quota_headers: dict[str, str],
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.url = url
        self.reasons = reasons
        self.message = message
        self.quota_headers = quota_headers


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
    return token[:80] or "youtube"


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
    if value < 1 or value > MAX_COMMENTS_API_PAGE_SIZE:
        raise ValueError(
            f"Comments page size must be between 1 and {MAX_COMMENTS_API_PAGE_SIZE}, got: {value}"
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
    comments_page_size = ensure_page_size(
        parse_positive_int(
            "--comments-page-size",
            str(
                args.comments_page_size
                if args.comments_page_size is not None
                else env_or_default(ENV_COMMENTS_PAGE_SIZE, str(DEFAULT_COMMENTS_PAGE_SIZE))
            ),
        )
    )
    max_videos_per_run = parse_positive_int(
        "--max-videos-per-run",
        str(
            args.max_videos_per_run
            if args.max_videos_per_run is not None
            else env_or_default(ENV_MAX_VIDEOS_PER_RUN, str(DEFAULT_MAX_VIDEOS_PER_RUN))
        ),
    )
    max_thread_pages_per_run = parse_positive_int(
        "--max-thread-pages-per-run",
        str(
            args.max_thread_pages_per_run
            if args.max_thread_pages_per_run is not None
            else env_or_default(
                ENV_MAX_THREAD_PAGES_PER_RUN, str(DEFAULT_MAX_THREAD_PAGES_PER_RUN)
            )
        ),
    )
    max_reply_pages_per_run = parse_positive_int(
        "--max-reply-pages-per-run",
        str(
            args.max_reply_pages_per_run
            if args.max_reply_pages_per_run is not None
            else env_or_default(
                ENV_MAX_REPLY_PAGES_PER_RUN, str(DEFAULT_MAX_REPLY_PAGES_PER_RUN)
            )
        ),
    )
    max_threads_per_run = parse_positive_int(
        "--max-threads-per-run",
        str(
            args.max_threads_per_run
            if args.max_threads_per_run is not None
            else env_or_default(ENV_MAX_THREADS_PER_RUN, str(DEFAULT_MAX_THREADS_PER_RUN))
        ),
    )
    max_comments_per_run = parse_positive_int(
        "--max-comments-per-run",
        str(
            args.max_comments_per_run
            if args.max_comments_per_run is not None
            else env_or_default(ENV_MAX_COMMENTS_PER_RUN, str(DEFAULT_MAX_COMMENTS_PER_RUN))
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
        comments_page_size=comments_page_size,
        max_videos_per_run=max_videos_per_run,
        max_thread_pages_per_run=max_thread_pages_per_run,
        max_reply_pages_per_run=max_reply_pages_per_run,
        max_threads_per_run=max_threads_per_run,
        max_comments_per_run=max_comments_per_run,
        max_retry_after_seconds=max_retry_after_seconds,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("youtube-comments-fetch")
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

                raise ApiRequestError(
                    status_code=status,
                    url=safe_url,
                    reasons=sorted(reasons),
                    message=error_info["message"],
                    quota_headers=quota_headers,
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


def maybe_valid_video_id(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    token = value.strip()
    if YOUTUBE_VIDEO_ID_PATTERN.fullmatch(token):
        return token
    return None


def get_nested_value(payload: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = payload
    for part in path:
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def extract_video_id_from_object(payload: Any) -> str | None:
    if isinstance(payload, str):
        return maybe_valid_video_id(payload)
    if not isinstance(payload, dict):
        return None

    candidate_paths = (
        ("video_id",),
        ("videoId",),
        ("video", "id"),
        ("video", "video_id"),
        ("id", "videoId"),
        ("search_result", "id", "videoId"),
    )
    for path in candidate_paths:
        value = get_nested_value(payload, path)
        candidate = maybe_valid_video_id(value)
        if candidate is not None:
            return candidate

    direct_id = maybe_valid_video_id(payload.get("id"))
    if direct_id is not None:
        return direct_id
    return None


def collect_video_ids(payload: Any, sink: list[str]) -> None:
    candidate = extract_video_id_from_object(payload)
    if candidate is not None:
        sink.append(candidate)

    if isinstance(payload, list):
        for item in payload:
            collect_video_ids(item, sink)
        return

    if not isinstance(payload, dict):
        return

    for key in ("records", "items", "videos", "data"):
        if key in payload:
            collect_video_ids(payload[key], sink)


def load_video_ids(
    *,
    inline_ids: list[str],
    file_paths: list[str],
    dedupe: bool,
) -> list[str]:
    collected: list[str] = []
    for value in inline_ids:
        candidate = maybe_valid_video_id(value)
        if candidate is None:
            raise ValueError(f"Invalid YouTube video ID: {value!r}")
        collected.append(candidate)

    for file_path in file_paths:
        path = Path(file_path).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"Video IDs file does not exist: {path}")

        suffix = path.suffix.lower()
        text = path.read_text(encoding="utf-8")
        if suffix == ".txt":
            for line_no, raw in enumerate(text.splitlines(), start=1):
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                candidate = maybe_valid_video_id(line)
                if candidate is None:
                    raise ValueError(f"Invalid video ID in {path}:{line_no}: {line!r}")
                collected.append(candidate)
            continue

        if suffix == ".jsonl":
            for line_no, raw in enumerate(text.splitlines(), start=1):
                line = raw.strip()
                if not line:
                    continue
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    payload = line
                line_candidates: list[str] = []
                collect_video_ids(payload, line_candidates)
                if not line_candidates:
                    raise ValueError(
                        f"No recognizable video ID found in {path}:{line_no}. "
                        "Expected plain video ID or JSON object containing video_id."
                    )
                collected.extend(line_candidates)
            continue

        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Unsupported file format for {path}. Use txt/json/jsonl.") from exc
        file_candidates: list[str] = []
        collect_video_ids(payload, file_candidates)
        if not file_candidates:
            raise ValueError(f"No recognizable video IDs found in {path}.")
        collected.extend(file_candidates)

    if not dedupe:
        return collected

    seen: set[str] = set()
    deduped: list[str] = []
    for video_id in collected:
        if video_id in seen:
            continue
        seen.add(video_id)
        deduped.append(video_id)
    return deduped


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


def build_output_file_path(*, output_dir: Path, output_file: str) -> Path:
    if output_file.strip():
        return Path(output_file).expanduser().resolve()
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    filename = f"youtube-comments-{timestamp}.jsonl"
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


def normalize_comment_resource(
    resource: Any,
    *,
    expected_video_id: str,
    thread_id: str,
    comment_type: str,
    parent_comment_id: str | None,
    location: str,
    max_issues: int,
    issues: list[dict[str, Any]],
    issue_count: int,
) -> tuple[dict[str, Any] | None, int]:
    if not isinstance(resource, dict):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": location,
                "reason": "invalid_type",
                "expected": "object",
                "actual": type(resource).__name__,
            },
        )
        return None, issue_count

    comment_id = resource.get("id")
    snippet = resource.get("snippet")
    if not isinstance(comment_id, str) or not comment_id.strip():
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": f"{location}.id",
                "reason": "missing_or_invalid",
                "actual": comment_id,
            },
        )
        return None, issue_count
    if not isinstance(snippet, dict):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": f"{location}.snippet",
                "reason": "invalid_type",
                "expected": "object",
                "actual": type(snippet).__name__,
            },
        )
        return None, issue_count

    video_id = str(snippet.get("videoId", "") or expected_video_id)
    if video_id != expected_video_id:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": f"{location}.snippet.videoId",
                "reason": "unexpected_video_id",
                "expected": expected_video_id,
                "actual": video_id,
            },
        )

    published_at = str(snippet.get("publishedAt", "") or "")
    updated_at = str(snippet.get("updatedAt", "") or "")
    if parse_response_datetime(published_at) is None:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": f"{location}.snippet.publishedAt",
                "reason": "invalid_datetime_format",
                "actual": published_at,
            },
        )
    if parse_response_datetime(updated_at) is None:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": f"{location}.snippet.updatedAt",
                "reason": "invalid_datetime_format",
                "actual": updated_at,
            },
        )

    author_channel_id_value = ""
    author_channel_id = snippet.get("authorChannelId")
    if isinstance(author_channel_id, dict):
        author_channel_id_value = str(author_channel_id.get("value", "") or "")

    actual_parent = snippet.get("parentId")
    if parent_comment_id is not None and actual_parent != parent_comment_id:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": f"{location}.snippet.parentId",
                "reason": "unexpected_parent_id",
                "expected": parent_comment_id,
                "actual": actual_parent,
            },
        )

    normalized = {
        "comment_id": comment_id,
        "thread_id": thread_id,
        "parent_comment_id": parent_comment_id,
        "video_id": video_id,
        "channel_id": str(snippet.get("channelId", "") or ""),
        "author_display_name": str(snippet.get("authorDisplayName", "") or ""),
        "author_channel_id": author_channel_id_value,
        "author_channel_url": str(snippet.get("authorChannelUrl", "") or ""),
        "published_at": published_at,
        "updated_at": updated_at,
        "text_display": str(snippet.get("textDisplay", "") or ""),
        "text_original": str(snippet.get("textOriginal", "") or ""),
        "like_count": maybe_int(snippet.get("likeCount")),
        "viewer_rating": str(snippet.get("viewerRating", "") or ""),
        "can_rate": snippet.get("canRate"),
        "comment_type": comment_type,
    }
    return normalized, issue_count


def validate_comment_threads_page(
    payload: dict[str, Any],
    *,
    expected_video_id: str,
    max_issues: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
    issues: list[dict[str, Any]] = []
    issue_count = 0

    if payload.get("kind") != COMMENT_THREAD_KIND:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.kind",
                "reason": "unexpected_value",
                "expected": COMMENT_THREAD_KIND,
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

    normalized_threads: list[dict[str, Any]] = []
    for item_index, item in enumerate(items):
        location = f"root.items[{item_index}]"
        if not isinstance(item, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": location,
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(item).__name__,
                },
            )
            continue

        thread_id = item.get("id")
        snippet = item.get("snippet")
        if not isinstance(thread_id, str) or not thread_id.strip():
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{location}.id",
                    "reason": "missing_or_invalid",
                    "actual": thread_id,
                },
            )
            continue
        if not isinstance(snippet, dict):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{location}.snippet",
                    "reason": "invalid_type",
                    "expected": "object",
                    "actual": type(snippet).__name__,
                },
            )
            continue

        thread_video_id = str(snippet.get("videoId", "") or "")
        if thread_video_id and thread_video_id != expected_video_id:
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"{location}.snippet.videoId",
                    "reason": "unexpected_video_id",
                    "expected": expected_video_id,
                    "actual": thread_video_id,
                },
            )

        total_reply_count = maybe_int(snippet.get("totalReplyCount"))
        if total_reply_count is None:
            total_reply_count = 0

        top_level_resource = snippet.get("topLevelComment")
        top_level_comment, issue_count = normalize_comment_resource(
            top_level_resource,
            expected_video_id=expected_video_id,
            thread_id=thread_id,
            comment_type="top_level",
            parent_comment_id=None,
            location=f"{location}.snippet.topLevelComment",
            max_issues=max_issues,
            issues=issues,
            issue_count=issue_count,
        )
        if top_level_comment is None:
            continue

        replies_payload = item.get("replies", {})
        embedded_replies: list[dict[str, Any]] = []
        if isinstance(replies_payload, dict):
            reply_items = replies_payload.get("comments")
            if isinstance(reply_items, list):
                for reply_index, reply_resource in enumerate(reply_items):
                    reply_comment, issue_count = normalize_comment_resource(
                        reply_resource,
                        expected_video_id=expected_video_id,
                        thread_id=thread_id,
                        comment_type="reply",
                        parent_comment_id=top_level_comment["comment_id"],
                        location=f"{location}.replies.comments[{reply_index}]",
                        max_issues=max_issues,
                        issues=issues,
                        issue_count=issue_count,
                    )
                    if reply_comment is not None:
                        embedded_replies.append(reply_comment)
            elif reply_items is not None:
                issue_count = add_issue(
                    issues,
                    issue_count,
                    max_issues,
                    {
                        "location": f"{location}.replies.comments",
                        "reason": "invalid_type",
                        "expected": "array",
                        "actual": type(reply_items).__name__,
                    },
                )

        normalized_threads.append(
            {
                "thread_id": thread_id,
                "video_id": expected_video_id,
                "can_reply": snippet.get("canReply"),
                "is_public": snippet.get("isPublic"),
                "total_reply_count": total_reply_count,
                "top_level_comment": top_level_comment,
                "embedded_replies": embedded_replies,
            }
        )

    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None and not isinstance(next_page_token, str):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.nextPageToken",
                "reason": "invalid_type",
                "expected": "string",
                "actual": type(next_page_token).__name__,
            },
        )
        next_page_token = None

    return (
        {
            "passed": issue_count == 0,
            "issue_count": issue_count,
            "reported_issue_count": len(issues),
            "issues": issues,
        },
        normalized_threads,
        next_page_token if isinstance(next_page_token, str) and next_page_token else None,
    )


def validate_reply_comments_page(
    payload: dict[str, Any],
    *,
    expected_video_id: str,
    thread_id: str,
    parent_comment_id: str,
    max_issues: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], str | None]:
    issues: list[dict[str, Any]] = []
    issue_count = 0

    if payload.get("kind") != COMMENT_LIST_KIND:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.kind",
                "reason": "unexpected_value",
                "expected": COMMENT_LIST_KIND,
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

    normalized_replies: list[dict[str, Any]] = []
    for item_index, item in enumerate(items):
        reply, issue_count = normalize_comment_resource(
            item,
            expected_video_id=expected_video_id,
            thread_id=thread_id,
            comment_type="reply",
            parent_comment_id=parent_comment_id,
            location=f"root.items[{item_index}]",
            max_issues=max_issues,
            issues=issues,
            issue_count=issue_count,
        )
        if reply is not None:
            normalized_replies.append(reply)

    next_page_token = payload.get("nextPageToken")
    if next_page_token is not None and not isinstance(next_page_token, str):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root.nextPageToken",
                "reason": "invalid_type",
                "expected": "string",
                "actual": type(next_page_token).__name__,
            },
        )
        next_page_token = None

    return (
        {
            "passed": issue_count == 0,
            "issue_count": issue_count,
            "reported_issue_count": len(issues),
            "issues": issues,
        },
        normalized_replies,
        next_page_token if isinstance(next_page_token, str) and next_page_token else None,
    )


def comment_timestamp(comment: dict[str, Any], *, time_field: str) -> datetime | None:
    key = "published_at" if time_field == "published" else "updated_at"
    return parse_response_datetime(comment.get(key))


def comment_in_window(
    comment: dict[str, Any],
    *,
    start_dt: datetime | None,
    end_dt: datetime | None,
    time_field: str,
) -> tuple[bool, str]:
    if start_dt is None and end_dt is None:
        return True, "no_window"
    ts = comment_timestamp(comment, time_field=time_field)
    if ts is None:
        return False, "missing_timestamp"
    if start_dt is not None and ts < start_dt:
        return False, "before_window"
    if end_dt is not None and ts >= end_dt:
        return False, "after_window"
    return True, "in_window"


def format_output_record(
    comment: dict[str, Any],
    *,
    time_field: str,
    source: dict[str, Any],
) -> dict[str, Any]:
    return {
        "video_id": comment["video_id"],
        "thread_id": comment["thread_id"],
        "comment_id": comment["comment_id"],
        "parent_comment_id": comment["parent_comment_id"],
        "comment_type": comment["comment_type"],
        "channel_id": comment["channel_id"],
        "author_display_name": comment["author_display_name"],
        "author_channel_id": comment["author_channel_id"],
        "author_channel_url": comment["author_channel_url"],
        "published_at": comment["published_at"],
        "updated_at": comment["updated_at"],
        "text_display": comment["text_display"],
        "text_original": comment["text_original"],
        "like_count": comment["like_count"],
        "viewer_rating": comment["viewer_rating"],
        "can_rate": comment["can_rate"],
        "time_field_used": time_field,
        "source": source,
    }


def build_thread_query(
    *,
    video_id: str,
    page_size: int,
    order: str,
    text_format: str,
    page_token: str,
    search_terms: str,
    include_replies: bool,
) -> dict[str, str]:
    query: dict[str, str] = {
        "part": "snippet,replies" if include_replies else "snippet",
        "videoId": video_id,
        "maxResults": str(page_size),
        "order": order,
        "textFormat": text_format,
    }
    if page_token:
        query["pageToken"] = page_token
    if search_terms.strip():
        query["searchTerms"] = search_terms.strip()
    return query


def build_reply_query(
    *,
    parent_comment_id: str,
    page_size: int,
    text_format: str,
    page_token: str,
) -> dict[str, str]:
    query: dict[str, str] = {
        "part": "snippet",
        "parentId": parent_comment_id,
        "maxResults": str(page_size),
        "textFormat": text_format,
    }
    if page_token:
        query["pageToken"] = page_token
    return query


def enforce_run_caps(args: argparse.Namespace, config: RuntimeConfig) -> None:
    if args.max_videos > config.max_videos_per_run:
        raise ValueError(
            f"--max-videos {args.max_videos} exceeds YOUTUBE_MAX_VIDEOS_PER_RUN "
            f"{config.max_videos_per_run}."
        )
    if args.max_thread_pages > config.max_thread_pages_per_run:
        raise ValueError(
            f"--max-thread-pages {args.max_thread_pages} exceeds YOUTUBE_MAX_THREAD_PAGES_PER_RUN "
            f"{config.max_thread_pages_per_run}."
        )
    if args.max_reply_pages > config.max_reply_pages_per_run:
        raise ValueError(
            f"--max-reply-pages {args.max_reply_pages} exceeds YOUTUBE_MAX_REPLY_PAGES_PER_RUN "
            f"{config.max_reply_pages_per_run}."
        )
    if args.max_threads > config.max_threads_per_run:
        raise ValueError(
            f"--max-threads {args.max_threads} exceeds YOUTUBE_MAX_THREADS_PER_RUN "
            f"{config.max_threads_per_run}."
        )
    if args.max_comments > config.max_comments_per_run:
        raise ValueError(
            f"--max-comments {args.max_comments} exceeds YOUTUBE_MAX_COMMENTS_PER_RUN "
            f"{config.max_comments_per_run}."
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
            "comments_page_size": config.comments_page_size,
            "max_videos_per_run": config.max_videos_per_run,
            "max_thread_pages_per_run": config.max_thread_pages_per_run,
            "max_reply_pages_per_run": config.max_reply_pages_per_run,
            "max_threads_per_run": config.max_threads_per_run,
            "max_comments_per_run": config.max_comments_per_run,
            "max_retry_after_seconds": config.max_retry_after_seconds,
            "user_agent": config.user_agent,
        },
        "source_urls": {
            "comment_threads": f"{config.base_url}/{COMMENT_THREADS_PATH}",
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
            "comments_page_size": ENV_COMMENTS_PAGE_SIZE,
            "max_videos_per_run": ENV_MAX_VIDEOS_PER_RUN,
            "max_thread_pages_per_run": ENV_MAX_THREAD_PAGES_PER_RUN,
            "max_reply_pages_per_run": ENV_MAX_REPLY_PAGES_PER_RUN,
            "max_threads_per_run": ENV_MAX_THREADS_PER_RUN,
            "max_comments_per_run": ENV_MAX_COMMENTS_PER_RUN,
            "max_retry_after_seconds": ENV_MAX_RETRY_AFTER_SECONDS,
            "user_agent": ENV_USER_AGENT,
        },
    }
    print_json(payload, pretty=args.pretty)
    return 0


def command_fetch(args: argparse.Namespace) -> int:
    logger = build_logger(level=args.log_level, log_file=args.log_file)
    config = build_runtime_config(args)
    enforce_run_caps(args, config)

    if args.max_validation_issues < 1:
        raise ValueError("--max-validation-issues must be >= 1.")

    args.order = ensure_choice(args.order, field_name="--order", allowed=THREAD_ORDER_VALUES)
    args.text_format = ensure_choice(
        args.text_format, field_name="--text-format", allowed=TEXT_FORMAT_VALUES
    )
    args.time_field = ensure_choice(
        args.time_field, field_name="--time-field", allowed=TIME_FIELD_VALUES
    )
    if args.max_videos < 1:
        raise ValueError("--max-videos must be >= 1.")
    if args.max_thread_pages < 1:
        raise ValueError("--max-thread-pages must be >= 1.")
    if args.max_reply_pages < 1:
        raise ValueError("--max-reply-pages must be >= 1.")
    if args.max_threads < 1:
        raise ValueError("--max-threads must be >= 1.")
    if args.max_comments < 1:
        raise ValueError("--max-comments must be >= 1.")

    start_dt: datetime | None = None
    end_dt: datetime | None = None
    if args.start_datetime.strip() or args.end_datetime.strip():
        if not args.start_datetime.strip() or not args.end_datetime.strip():
            raise ValueError("--start-datetime and --end-datetime must be provided together.")
        start_dt = parse_datetime_flexible(
            args.start_datetime, field_name="--start-datetime", is_end=False
        )
        end_dt = parse_datetime_flexible(
            args.end_datetime, field_name="--end-datetime", is_end=True
        )
        if start_dt >= end_dt:
            raise ValueError("--start-datetime must be earlier than --end-datetime.")
        args.start_datetime = format_rfc3339(start_dt)
        args.end_datetime = format_rfc3339(end_dt)
    else:
        args.start_datetime = ""
        args.end_datetime = ""

    page_size = min(args.page_size, config.comments_page_size)
    video_ids = load_video_ids(
        inline_ids=args.video_id,
        file_paths=args.video_ids_file,
        dedupe=args.dedupe,
    )
    if not video_ids:
        raise ValueError("No video IDs provided. Use --video-id or --video-ids-file.")
    selected_video_ids = video_ids[: args.max_videos]

    if args.dry_run:
        sample_video_id = selected_video_ids[0]
        thread_query = build_thread_query(
            video_id=sample_video_id,
            page_size=page_size,
            order=args.order,
            text_format=args.text_format,
            page_token="",
            search_terms=args.search_terms,
            include_replies=args.include_replies,
        )
        reply_query = build_reply_query(
            parent_comment_id="TOP_LEVEL_COMMENT_ID",
            page_size=page_size,
            text_format=args.text_format,
            page_token="",
        )
        payload = {
            "ok": True,
            "dry_run": True,
            "request_plan": {
                "selected_video_count": len(selected_video_ids),
                "order": args.order,
                "text_format": args.text_format,
                "time_field": args.time_field,
                "start_datetime": args.start_datetime or None,
                "end_datetime": args.end_datetime or None,
                "include_replies": args.include_replies,
                "search_terms": args.search_terms or None,
                "page_size_effective": page_size,
                "max_thread_pages": args.max_thread_pages,
                "max_reply_pages": args.max_reply_pages,
                "max_threads": args.max_threads,
                "max_comments": args.max_comments,
                "dedupe": args.dedupe,
            },
            "sample_thread_request_url": render_query(
                config.base_url, COMMENT_THREADS_PATH, thread_query
            ),
            "sample_reply_request_url": render_query(config.base_url, COMMENTS_PATH, reply_query),
            "sample_video_ids": selected_video_ids[:5],
        }
        print_json(payload, pretty=args.pretty)
        return 0

    client = GoogleApiClient(config=config, logger=logger)

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    page_trace: list[dict[str, Any]] = []
    reply_trace: list[dict[str, Any]] = []
    video_summaries: list[dict[str, Any]] = []
    seen_comment_ids: set[str] = set()

    validation_issue_count = 0
    duplicate_comment_count = 0
    outside_window_count = 0
    missing_timestamp_count = 0
    top_level_count = 0
    reply_count = 0
    global_thread_pages_used = 0
    global_reply_pages_used = 0
    global_threads_seen = 0
    reply_fetch_partial_thread_count = 0

    for video_index, video_id in enumerate(selected_video_ids, start=1):
        if global_threads_seen >= args.max_threads or len(records) >= args.max_comments:
            break

        logger.info("video-start index=%d/%d video_id=%s", video_index, len(selected_video_ids), video_id)
        video_summary = {
            "video_id": video_id,
            "video_index": video_index,
            "thread_pages_used": 0,
            "reply_pages_used": 0,
            "threads_seen": 0,
            "comments_kept": 0,
            "top_level_kept": 0,
            "reply_kept": 0,
            "failure_count": 0,
            "issue_count": 0,
            "stop_reason": "completed",
        }

        thread_page_token = ""
        while True:
            if global_thread_pages_used >= args.max_thread_pages:
                video_summary["stop_reason"] = "max_thread_pages_reached"
                break
            if global_threads_seen >= args.max_threads:
                video_summary["stop_reason"] = "max_threads_reached"
                break
            if len(records) >= args.max_comments:
                video_summary["stop_reason"] = "max_comments_reached"
                break

            thread_query = build_thread_query(
                video_id=video_id,
                page_size=page_size,
                order=args.order,
                text_format=args.text_format,
                page_token=thread_page_token,
                search_terms=args.search_terms,
                include_replies=args.include_replies,
            )
            try:
                response = client.get_json(COMMENT_THREADS_PATH, thread_query)
            except ApiRequestError as exc:
                failure = {
                    "video_id": video_id,
                    "stage": "commentThreads.list",
                    "status_code": exc.status_code,
                    "reasons": exc.reasons,
                    "error": exc.message,
                    "url": exc.url,
                }
                failures.append(failure)
                video_summary["failure_count"] += 1
                if "commentsDisabled" in exc.reasons:
                    video_summary["stop_reason"] = "comments_disabled"
                    logger.warning("video-skip video_id=%s reason=comments_disabled", video_id)
                    break
                if "videoNotFound" in exc.reasons:
                    video_summary["stop_reason"] = "video_not_found"
                    logger.warning("video-skip video_id=%s reason=video_not_found", video_id)
                    break
                if args.fail_on_video_error:
                    raise
                video_summary["stop_reason"] = "video_error"
                logger.warning(
                    "video-error video_id=%s status=%d reasons=%s error=%s",
                    video_id,
                    exc.status_code,
                    exc.reasons,
                    exc.message,
                )
                break

            global_thread_pages_used += 1
            video_summary["thread_pages_used"] += 1
            validation, threads, next_page_token = validate_comment_threads_page(
                response.payload,
                expected_video_id=video_id,
                max_issues=args.max_validation_issues,
            )
            validation_issue_count += validation["issue_count"]
            video_summary["issue_count"] += validation["issue_count"]

            if validation["issue_count"] > 0:
                logger.warning(
                    "thread-validation-issues video_id=%s page=%d issues=%d",
                    video_id,
                    video_summary["thread_pages_used"],
                    validation["issue_count"],
                )
                if args.quarantine_dir.strip():
                    write_quarantine_issues(
                        quarantine_dir=Path(args.quarantine_dir).expanduser().resolve(),
                        name=f"thread-page-{video_id}-{video_summary['thread_pages_used']:04d}",
                        issues=validation["issues"],
                    )

            page_all_older_than_start = bool(threads) and start_dt is not None
            page_kept = 0
            for thread in threads:
                if global_threads_seen >= args.max_threads or len(records) >= args.max_comments:
                    video_summary["stop_reason"] = (
                        "max_comments_reached" if len(records) >= args.max_comments else "max_threads_reached"
                    )
                    break

                global_threads_seen += 1
                video_summary["threads_seen"] += 1
                top_comment = thread["top_level_comment"]
                top_ts = comment_timestamp(top_comment, time_field=args.time_field)
                if start_dt is not None and args.time_field == "published" and args.order == "time":
                    if top_ts is None or top_ts >= start_dt:
                        page_all_older_than_start = False
                else:
                    page_all_older_than_start = False

                in_window, window_reason = comment_in_window(
                    top_comment,
                    start_dt=start_dt,
                    end_dt=end_dt,
                    time_field=args.time_field,
                )
                if in_window:
                    if top_comment["comment_id"] in seen_comment_ids:
                        duplicate_comment_count += 1
                    else:
                        seen_comment_ids.add(top_comment["comment_id"])
                        record = format_output_record(
                            top_comment,
                            time_field=args.time_field,
                            source={
                                "video_index": video_index,
                                "thread_page_number": video_summary["thread_pages_used"],
                                "reply_page_number": None,
                                "order": args.order,
                                "search_terms": args.search_terms or None,
                            },
                        )
                        records.append(record)
                        page_kept += 1
                        top_level_count += 1
                        video_summary["comments_kept"] += 1
                        video_summary["top_level_kept"] += 1
                else:
                    if window_reason == "missing_timestamp":
                        missing_timestamp_count += 1
                    else:
                        outside_window_count += 1

                if not args.include_replies or thread["total_reply_count"] <= 0:
                    continue

                replies_to_process: list[dict[str, Any]] = []
                reply_fetch_stop_reason = "not_requested"
                embedded_replies = thread["embedded_replies"]
                if thread["total_reply_count"] == len(embedded_replies):
                    replies_to_process = embedded_replies
                    reply_fetch_stop_reason = "embedded_complete"
                else:
                    reply_page_token = ""
                    reply_pages_for_thread = 0
                    replies_to_process = []
                    while True:
                        if global_reply_pages_used >= args.max_reply_pages:
                            reply_fetch_stop_reason = "max_reply_pages_reached"
                            reply_fetch_partial_thread_count += 1
                            break
                        reply_query = build_reply_query(
                            parent_comment_id=top_comment["comment_id"],
                            page_size=page_size,
                            text_format=args.text_format,
                            page_token=reply_page_token,
                        )
                        try:
                            reply_response = client.get_json(COMMENTS_PATH, reply_query)
                        except ApiRequestError as exc:
                            failure = {
                                "video_id": video_id,
                                "thread_id": thread["thread_id"],
                                "stage": "comments.list",
                                "status_code": exc.status_code,
                                "reasons": exc.reasons,
                                "error": exc.message,
                                "url": exc.url,
                            }
                            failures.append(failure)
                            video_summary["failure_count"] += 1
                            if args.fail_on_video_error:
                                raise
                            reply_fetch_stop_reason = "reply_fetch_error"
                            break

                        global_reply_pages_used += 1
                        video_summary["reply_pages_used"] += 1
                        reply_pages_for_thread += 1
                        reply_validation, reply_comments, reply_next_page = validate_reply_comments_page(
                            reply_response.payload,
                            expected_video_id=video_id,
                            thread_id=thread["thread_id"],
                            parent_comment_id=top_comment["comment_id"],
                            max_issues=args.max_validation_issues,
                        )
                        validation_issue_count += reply_validation["issue_count"]
                        video_summary["issue_count"] += reply_validation["issue_count"]
                        replies_to_process.extend(reply_comments)
                        reply_trace.append(
                            {
                                "video_id": video_id,
                                "thread_id": thread["thread_id"],
                                "parent_comment_id": top_comment["comment_id"],
                                "reply_page_number": reply_pages_for_thread,
                                "reply_count": len(reply_comments),
                                "byte_length": reply_response.byte_length,
                                "issue_count": reply_validation["issue_count"],
                            }
                        )
                        if reply_validation["issue_count"] > 0 and args.quarantine_dir.strip():
                            write_quarantine_issues(
                                quarantine_dir=Path(args.quarantine_dir).expanduser().resolve(),
                                name=(
                                    f"reply-page-{video_id}-{thread['thread_id']}"
                                    f"-{reply_pages_for_thread:04d}"
                                ),
                                issues=reply_validation["issues"],
                            )
                        if not reply_next_page:
                            reply_fetch_stop_reason = "no_next_page"
                            break
                        if len(records) >= args.max_comments:
                            reply_fetch_stop_reason = "max_comments_reached"
                            break
                        reply_page_token = reply_next_page

                for reply in replies_to_process:
                    if len(records) >= args.max_comments:
                        video_summary["stop_reason"] = "max_comments_reached"
                        break
                    in_window, window_reason = comment_in_window(
                        reply,
                        start_dt=start_dt,
                        end_dt=end_dt,
                        time_field=args.time_field,
                    )
                    if not in_window:
                        if window_reason == "missing_timestamp":
                            missing_timestamp_count += 1
                        else:
                            outside_window_count += 1
                        continue
                    if reply["comment_id"] in seen_comment_ids:
                        duplicate_comment_count += 1
                        continue
                    seen_comment_ids.add(reply["comment_id"])
                    records.append(
                        format_output_record(
                            reply,
                            time_field=args.time_field,
                            source={
                                "video_index": video_index,
                                "thread_page_number": video_summary["thread_pages_used"],
                                "reply_page_number": None,
                                "order": args.order,
                                "search_terms": args.search_terms or None,
                                "reply_fetch_stop_reason": reply_fetch_stop_reason,
                            },
                        )
                    )
                    reply_count += 1
                    page_kept += 1
                    video_summary["comments_kept"] += 1
                    video_summary["reply_kept"] += 1

            page_trace.append(
                {
                    "video_id": video_id,
                    "thread_page_number": video_summary["thread_pages_used"],
                    "thread_count": len(threads),
                    "kept_comment_count": page_kept,
                    "byte_length": response.byte_length,
                    "issue_count": validation["issue_count"],
                    "next_page_token_present": bool(next_page_token),
                }
            )

            logger.info(
                "thread-page video_id=%s page=%d threads=%d kept=%d next_page=%s",
                video_id,
                video_summary["thread_pages_used"],
                len(threads),
                page_kept,
                bool(next_page_token),
            )

            if video_summary["stop_reason"] != "completed":
                break
            if not next_page_token:
                video_summary["stop_reason"] = "no_next_page"
                break
            if (
                args.stop_when_all_older
                and start_dt is not None
                and args.order == "time"
                and args.time_field == "published"
                and page_all_older_than_start
            ):
                video_summary["stop_reason"] = "all_top_level_comments_older_than_start"
                break
            thread_page_token = next_page_token

        video_summaries.append(video_summary)

    video_success_count = sum(
        1
        for summary in video_summaries
        if summary["failure_count"] == 0
        and summary["stop_reason"] not in {"comments_disabled", "video_not_found", "video_error"}
    )
    video_failure_count = sum(
        1
        for summary in video_summaries
        if summary["failure_count"] > 0
        and summary["stop_reason"] not in {"comments_disabled", "video_not_found"}
    )
    video_skipped_count = sum(
        1 for summary in video_summaries if summary["stop_reason"] in {"comments_disabled", "video_not_found"}
    )

    output_file_path: Path | None = None
    if args.save_records and records:
        output_dir = Path(args.output_dir).expanduser().resolve()
        output_file_path = build_output_file_path(output_dir=output_dir, output_file=args.output_file)
        save_records_jsonl(path=output_file_path, records=records, overwrite=args.overwrite)

    payload = {
        "ok": not (
            (args.fail_on_validation_error and validation_issue_count > 0)
            or (args.fail_on_video_error and failures)
        ),
        "request": {
            "selected_video_count": len(selected_video_ids),
            "start_datetime": args.start_datetime or None,
            "end_datetime": args.end_datetime or None,
            "time_field": args.time_field,
            "order": args.order,
            "text_format": args.text_format,
            "include_replies": args.include_replies,
            "search_terms": args.search_terms or None,
            "page_size": page_size,
            "max_thread_pages": args.max_thread_pages,
            "max_reply_pages": args.max_reply_pages,
            "max_threads": args.max_threads,
            "max_comments": args.max_comments,
        },
        "fetch_summary": {
            "record_count": len(records),
            "top_level_count": top_level_count,
            "reply_count": reply_count,
            "duplicate_comment_count": duplicate_comment_count,
            "outside_window_count": outside_window_count,
            "missing_timestamp_count": missing_timestamp_count,
            "video_success_count": video_success_count,
            "video_failure_count": video_failure_count,
            "video_skipped_count": video_skipped_count,
            "thread_pages_used": global_thread_pages_used,
            "reply_pages_used": global_reply_pages_used,
            "threads_seen": global_threads_seen,
            "reply_fetch_partial_thread_count": reply_fetch_partial_thread_count,
            "reply_window_completeness": (
                "best_effort" if args.include_replies and start_dt is not None else "not_applicable"
            ),
        },
        "validation_summary": {
            "total_issue_count": validation_issue_count,
        },
        "video_summaries": video_summaries,
        "page_trace": page_trace,
        "reply_trace": reply_trace,
        "failures": failures,
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
        "--comments-page-size",
        type=int,
        default=None,
        help="Configured maximum comment page size from env override.",
    )
    parser.add_argument(
        "--max-videos-per-run",
        type=int,
        default=None,
        help="Configured maximum videos per run override.",
    )
    parser.add_argument(
        "--max-thread-pages-per-run",
        type=int,
        default=None,
        help="Configured maximum thread pages per run override.",
    )
    parser.add_argument(
        "--max-reply-pages-per-run",
        type=int,
        default=None,
        help="Configured maximum reply pages per run override.",
    )
    parser.add_argument(
        "--max-threads-per-run",
        type=int,
        default=None,
        help="Configured maximum threads per run override.",
    )
    parser.add_argument(
        "--max-comments-per-run",
        type=int,
        default=None,
        help="Configured maximum comments per run override.",
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
        description="Fetch YouTube public comment threads and replies for video IDs."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Show effective runtime config and source URLs.")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch = sub.add_parser("fetch", help="Fetch public comments for video IDs from args/files.")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument(
        "--video-id",
        action="append",
        default=[],
        help="YouTube video ID (repeatable).",
    )
    fetch.add_argument(
        "--video-ids-file",
        action="append",
        default=[],
        help=(
            "Path to file containing video IDs. Supports txt/json/jsonl. "
            "Also accepts youtube-video-search JSON/JSONL outputs."
        ),
    )
    fetch.add_argument(
        "--dedupe",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Deduplicate IDs while keeping first-seen order.",
    )
    fetch.add_argument(
        "--start-datetime",
        default="",
        help="Optional UTC window start. Accepts YYYY-MM-DD or RFC3339.",
    )
    fetch.add_argument(
        "--end-datetime",
        default="",
        help="Optional UTC window end (exclusive). Accepts YYYY-MM-DD or RFC3339.",
    )
    fetch.add_argument(
        "--time-field",
        default="published",
        help="Filter field: published or updated.",
    )
    fetch.add_argument(
        "--include-replies",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fetch and emit replies as well as top-level comments.",
    )
    fetch.add_argument(
        "--search-terms",
        default="",
        help="Optional YouTube searchTerms filter for top-level comment threads.",
    )
    fetch.add_argument(
        "--order",
        default="time",
        help="Thread ordering. One of: relevance,time.",
    )
    fetch.add_argument(
        "--text-format",
        default="plainText",
        help="Comment text format. One of: html,plainText.",
    )
    fetch.add_argument(
        "--page-size",
        type=int,
        default=100,
        help="Per-page request size. Effective value is min(--page-size, configured cap).",
    )
    fetch.add_argument(
        "--max-videos",
        type=int,
        default=10,
        help="Maximum selected videos to process.",
    )
    fetch.add_argument(
        "--max-thread-pages",
        type=int,
        default=10,
        help="Maximum top-level comment-thread pages to request across the run.",
    )
    fetch.add_argument(
        "--max-reply-pages",
        type=int,
        default=20,
        help="Maximum reply pages to request across the run.",
    )
    fetch.add_argument(
        "--max-threads",
        type=int,
        default=500,
        help="Maximum top-level threads to inspect across the run.",
    )
    fetch.add_argument(
        "--max-comments",
        type=int,
        default=2000,
        help="Maximum in-window comments to emit across the run.",
    )
    fetch.add_argument(
        "--stop-when-all-older",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "When using --order time and --time-field published, stop a video's crawl after a page "
            "whose top-level comments are all older than --start-datetime."
        ),
    )
    fetch.add_argument(
        "--include-records",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include normalized comment records in stdout JSON.",
    )
    fetch.add_argument(
        "--max-validation-issues",
        type=int,
        default=DEFAULT_MAX_VALIDATION_ISSUES,
        help=f"Maximum retained validation issues per page. Default: {DEFAULT_MAX_VALIDATION_ISSUES}.",
    )
    fetch.add_argument(
        "--fail-on-validation-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exit non-zero if validation issues are found.",
    )
    fetch.add_argument(
        "--fail-on-video-error",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Exit non-zero on the first video-level fetch error.",
    )
    fetch.add_argument(
        "--quarantine-dir",
        default="",
        help="Optional directory to save validation issue JSONL files.",
    )
    fetch.add_argument(
        "--save-records",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save normalized comment records into JSONL.",
    )
    fetch.add_argument(
        "--output-dir",
        default="./data/youtube-comments",
        help="Output directory for saved JSONL.",
    )
    fetch.add_argument("--output-file", default="", help="Optional explicit output JSONL path.")
    fetch.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it already exists.",
    )
    fetch.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate input and print a request plan without remote calls.",
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
