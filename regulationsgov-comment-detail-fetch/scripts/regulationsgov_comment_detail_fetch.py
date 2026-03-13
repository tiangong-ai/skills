#!/usr/bin/env python3
"""Fetch Regulations.gov v4 comment details by comment IDs with retries and validation."""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib import parse, request
from urllib.error import HTTPError, URLError

ENV_BASE_URL = "REGGOV_BASE_URL"
ENV_API_KEY = "REGGOV_API_KEY"
ENV_TIMEOUT_SECONDS = "REGGOV_TIMEOUT_SECONDS"
ENV_MAX_RETRIES = "REGGOV_MAX_RETRIES"
ENV_RETRY_BACKOFF_SECONDS = "REGGOV_RETRY_BACKOFF_SECONDS"
ENV_RETRY_BACKOFF_MULTIPLIER = "REGGOV_RETRY_BACKOFF_MULTIPLIER"
ENV_MIN_REQUEST_INTERVAL_SECONDS = "REGGOV_MIN_REQUEST_INTERVAL_SECONDS"
ENV_MAX_COMMENT_IDS_PER_RUN = "REGGOV_MAX_COMMENT_IDS_PER_RUN"
ENV_MAX_RETRY_AFTER_SECONDS = "REGGOV_MAX_RETRY_AFTER_SECONDS"
ENV_USER_AGENT = "REGGOV_USER_AGENT"

DEFAULT_BASE_URL = "https://api.regulations.gov/v4"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 4
DEFAULT_RETRY_BACKOFF_SECONDS = 1.5
DEFAULT_RETRY_BACKOFF_MULTIPLIER = 2.0
DEFAULT_MIN_REQUEST_INTERVAL_SECONDS = 1.2
DEFAULT_MAX_COMMENT_IDS_PER_RUN = 300
DEFAULT_MAX_RETRY_AFTER_SECONDS = 120
DEFAULT_USER_AGENT = "regulationsgov-comment-detail-fetch/1.0"
DEFAULT_MAX_VALIDATION_ISSUES = 30

RETRIABLE_HTTP_CODES = {429, 500, 502, 503, 504}
RESOURCE_TYPE_COMMENT = "comments"
ISO_UTC_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")


@dataclass(frozen=True)
class RuntimeConfig:
    base_url: str
    api_key: str
    timeout_seconds: int
    max_retries: int
    retry_backoff_seconds: float
    retry_backoff_multiplier: float
    min_request_interval_seconds: float
    max_comment_ids_per_run: int
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


def sanitize_filename_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_.-]+", "-", value).strip("-")
    return token or "unknown"


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
    max_comment_ids_per_run = parse_positive_int(
        "--max-comment-ids-per-run",
        str(
            args.max_comment_ids_per_run
            if args.max_comment_ids_per_run is not None
            else env_or_default(
                ENV_MAX_COMMENT_IDS_PER_RUN, str(DEFAULT_MAX_COMMENT_IDS_PER_RUN)
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
        max_comment_ids_per_run=max_comment_ids_per_run,
        max_retry_after_seconds=max_retry_after_seconds,
        user_agent=user_agent,
    )


def build_logger(level: str, log_file: str) -> logging.Logger:
    logger = logging.getLogger("regulationsgov-comment-detail-fetch")
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


def render_query(base_url: str, path: str, query: dict[str, str]) -> str:
    encoded = parse.urlencode(query, doseq=False)
    if encoded:
        return f"{base_url}/{path}?{encoded}"
    return f"{base_url}/{path}"


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


def iter_objects_from_any(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        return
    if isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield item


def extract_comment_id_from_object(obj: dict[str, Any]) -> str | None:
    for key in ("id", "comment_id", "commentId"):
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def load_comment_ids_from_json(path: Path) -> list[str]:
    obj = json.loads(path.read_text(encoding="utf-8"))
    ids: list[str] = []

    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, str) and item.strip():
                ids.append(item.strip())
            elif isinstance(item, dict):
                cid = extract_comment_id_from_object(item)
                if cid:
                    ids.append(cid)
        return ids

    if isinstance(obj, dict):
        for candidate in (obj.get("records"), obj.get("data")):
            if isinstance(candidate, list):
                for row in candidate:
                    if isinstance(row, str) and row.strip():
                        ids.append(row.strip())
                    elif isinstance(row, dict):
                        cid = extract_comment_id_from_object(row)
                        if cid:
                            ids.append(cid)
                if ids:
                    return ids
            if isinstance(candidate, dict):
                cid = extract_comment_id_from_object(candidate)
                if cid:
                    return [cid]

        cid = extract_comment_id_from_object(obj)
        if cid:
            return [cid]

    return ids


def load_comment_ids_from_text(path: Path) -> list[str]:
    ids: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_no, raw in enumerate(handle, start=1):
            line = raw.strip()
            if not line or line.startswith("#"):
                continue

            if line.startswith("{") or line.startswith("["):
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    ids.append(line)
                    continue

                for obj in iter_objects_from_any(parsed):
                    cid = extract_comment_id_from_object(obj)
                    if cid:
                        ids.append(cid)
                if isinstance(parsed, str) and parsed.strip():
                    ids.append(parsed.strip())
                continue

            if "\t" in line:
                line = line.split("\t", 1)[0].strip()
            if not line:
                continue

            ids.append(line)

    return ids


def load_comment_ids(
    *,
    inline_ids: list[str],
    file_paths: list[str],
    dedupe: bool,
) -> list[str]:
    ids: list[str] = []

    for raw in inline_ids:
        value = raw.strip()
        if value:
            ids.append(value)

    for raw_path in file_paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"Comment ID file does not exist: {path}")
        if not path.is_file():
            raise ValueError(f"Comment ID path is not a file: {path}")

        suffix = path.suffix.lower()
        if suffix == ".json":
            file_ids = load_comment_ids_from_json(path)
        else:
            file_ids = load_comment_ids_from_text(path)

        ids.extend(file_ids)

    cleaned = [item for item in (x.strip() for x in ids) if item]
    if not dedupe:
        return cleaned

    deduped: list[str] = []
    seen: set[str] = set()
    for item in cleaned:
        if item in seen:
            continue
        deduped.append(item)
        seen.add(item)
    return deduped


def extract_comment_resource(payload: dict[str, Any]) -> dict[str, Any] | None:
    if isinstance(payload.get("data"), dict):
        return payload["data"]
    if isinstance(payload.get("id"), str) and isinstance(payload.get("type"), str):
        return payload
    return None


def validate_comment_detail(
    payload: dict[str, Any],
    *,
    expected_comment_id: str,
    max_issues: int,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    issues: list[dict[str, Any]] = []
    issue_count = 0

    resource = extract_comment_resource(payload)
    if resource is None:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "root",
                "reason": "missing_comment_resource",
                "expected": "payload.data object or root object with id/type",
            },
        )
        return {
            "passed": issue_count == 0,
            "issue_count": issue_count,
            "reported_issue_count": len(issues),
            "issues": issues,
        }, None

    comment_id = resource.get("id")
    if not isinstance(comment_id, str) or not comment_id.strip():
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "resource.id",
                "reason": "invalid_id",
                "actual": comment_id,
            },
        )
    elif comment_id.strip() != expected_comment_id:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "resource.id",
                "reason": "id_mismatch",
                "expected": expected_comment_id,
                "actual": comment_id.strip(),
            },
        )

    resource_type = resource.get("type")
    if resource_type != RESOURCE_TYPE_COMMENT:
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "resource.type",
                "reason": "unexpected_value",
                "expected": RESOURCE_TYPE_COMMENT,
                "actual": resource_type,
            },
        )

    attributes = resource.get("attributes")
    if not isinstance(attributes, dict):
        issue_count = add_issue(
            issues,
            issue_count,
            max_issues,
            {
                "location": "resource.attributes",
                "reason": "invalid_type",
                "expected": "object",
                "actual": type(attributes).__name__,
            },
        )
        attributes = {}

    for field_name in ("postedDate", "modifyDate", "receiveDate"):
        value = attributes.get(field_name)
        if value is None:
            continue
        if not isinstance(value, str):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"resource.attributes.{field_name}",
                    "reason": "invalid_type",
                    "expected": "string_or_null",
                    "actual": type(value).__name__,
                },
            )
            continue
        if value and not ISO_UTC_PATTERN.match(value):
            issue_count = add_issue(
                issues,
                issue_count,
                max_issues,
                {
                    "location": f"resource.attributes.{field_name}",
                    "reason": "invalid_datetime_format",
                    "expected": "YYYY-MM-DDTHH:MM:SSZ",
                    "actual": value,
                },
            )

    return {
        "passed": issue_count == 0,
        "issue_count": issue_count,
        "reported_issue_count": len(issues),
        "issues": issues,
    }, resource


def write_quarantine_issues(
    *,
    quarantine_dir: Path,
    comment_id: str,
    issues: list[dict[str, Any]],
) -> Path | None:
    if not issues:
        return None
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    token = sanitize_filename_token(comment_id)
    output_path = quarantine_dir / f"comment-{token}.validation-issues.jsonl"
    with output_path.open("w", encoding="utf-8") as handle:
        for issue in issues:
            handle.write(json.dumps(issue, ensure_ascii=False))
            handle.write("\n")
    return output_path


def build_output_file_path(
    *,
    output_dir: Path,
    output_file: str,
) -> Path:
    if output_file.strip():
        return Path(output_file).expanduser().resolve()
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    filename = f"comment-details-{timestamp}.jsonl"
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
            "api_key_masked": mask_api_key(config.api_key),
            "api_key_length": len(config.api_key),
            "timeout_seconds": config.timeout_seconds,
            "max_retries": config.max_retries,
            "retry_backoff_seconds": config.retry_backoff_seconds,
            "retry_backoff_multiplier": config.retry_backoff_multiplier,
            "min_request_interval_seconds": config.min_request_interval_seconds,
            "max_comment_ids_per_run": config.max_comment_ids_per_run,
            "max_retry_after_seconds": config.max_retry_after_seconds,
            "user_agent": config.user_agent,
        },
        "source_urls": {
            "comment_detail_template": f"{config.base_url}/comments/{{commentId}}",
        },
        "env_keys": {
            "base_url": ENV_BASE_URL,
            "api_key": ENV_API_KEY,
            "timeout_seconds": ENV_TIMEOUT_SECONDS,
            "max_retries": ENV_MAX_RETRIES,
            "retry_backoff_seconds": ENV_RETRY_BACKOFF_SECONDS,
            "retry_backoff_multiplier": ENV_RETRY_BACKOFF_MULTIPLIER,
            "min_request_interval_seconds": ENV_MIN_REQUEST_INTERVAL_SECONDS,
            "max_comment_ids_per_run": ENV_MAX_COMMENT_IDS_PER_RUN,
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
    if args.max_comments < 0:
        raise ValueError("--max-comments must be >= 0.")

    comment_ids = load_comment_ids(
        inline_ids=args.comment_id,
        file_paths=args.comment_ids_file,
        dedupe=args.dedupe,
    )
    if not comment_ids:
        raise ValueError("No comment IDs provided. Use --comment-id or --comment-ids-file.")

    if len(comment_ids) > config.max_comment_ids_per_run:
        raise ValueError(
            f"Collected {len(comment_ids)} IDs, exceeds configured cap {config.max_comment_ids_per_run} "
            f"(set by --max-comment-ids-per-run or {ENV_MAX_COMMENT_IDS_PER_RUN})."
        )

    selected_ids = comment_ids
    if args.max_comments > 0:
        if args.max_comments > config.max_comment_ids_per_run:
            raise ValueError(
                f"--max-comments={args.max_comments} exceeds configured cap "
                f"{config.max_comment_ids_per_run}"
            )
        selected_ids = comment_ids[: args.max_comments]

    logger.info(
        "comment-ids selected=%d total_collected=%d dedupe=%s",
        len(selected_ids),
        len(comment_ids),
        args.dedupe,
    )

    include_value = args.include.strip()
    if include_value and include_value not in {"attachments"}:
        raise ValueError("--include currently supports only 'attachments'.")

    if args.dry_run:
        sample_id = selected_ids[0]
        sample_path = f"comments/{parse.quote(sample_id, safe='')}"
        query = {"include": include_value} if include_value else {}
        payload = {
            "ok": True,
            "dry_run": True,
            "request_plan": {
                "base_url": config.base_url,
                "path_template": "comments/{commentId}",
                "selected_count": len(selected_ids),
                "max_comments": args.max_comments,
                "include": include_value or None,
                "dedupe": args.dedupe,
                "fail_on_item_error": args.fail_on_item_error,
                "fail_on_validation_error": args.fail_on_validation_error,
            },
            "sample_request_url": render_query(config.base_url, sample_path, query),
            "sample_ids": selected_ids[:5],
        }
        print_json(payload, pretty=args.pretty)
        return 0

    client = RetryableHttpClient(config=config, logger=logger)

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    issue_count_total = 0
    records_with_issues = 0
    rate_limit_last: dict[str, Any] = {}

    for index, comment_id in enumerate(selected_ids, start=1):
        path = f"comments/{parse.quote(comment_id, safe='')}"
        query = {"include": include_value} if include_value else {}
        url = render_query(config.base_url, path, query)

        logger.info("detail-fetch index=%d/%d comment_id=%s", index, len(selected_ids), comment_id)
        try:
            response = client.get_json(url)
            validation, detail_resource = validate_comment_detail(
                response.payload,
                expected_comment_id=comment_id,
                max_issues=args.max_validation_issues,
            )
            issue_count_total += validation["issue_count"]

            if validation["issue_count"] > 0:
                records_with_issues += 1
                logger.warning(
                    "detail-validation-issues comment_id=%s issues=%d",
                    comment_id,
                    validation["issue_count"],
                )
                if args.quarantine_dir.strip():
                    q_path = write_quarantine_issues(
                        quarantine_dir=Path(args.quarantine_dir).expanduser().resolve(),
                        comment_id=comment_id,
                        issues=validation["issues"],
                    )
                    validation["quarantine_path"] = str(q_path) if q_path else None
                else:
                    validation["quarantine_path"] = None

                if args.fail_on_validation_error:
                    raise RuntimeError(
                        f"Validation failed for comment {comment_id} "
                        f"(issues={validation['issue_count']})."
                    )
            else:
                validation["quarantine_path"] = None

            detail_summary = {
                "comment_id": comment_id,
                "response_url": response.url,
                "status_code": response.status_code,
                "byte_length": response.byte_length,
                "rate_limit": extract_rate_limit(response.headers),
                "validation": validation,
                "detail": detail_resource if args.include_records else None,
            }
            rate_limit_last = detail_summary["rate_limit"]
            records.append(detail_summary)
        except Exception as exc:  # noqa: BLE001
            failure = {
                "comment_id": comment_id,
                "error": str(exc),
            }
            failures.append(failure)
            logger.error("detail-fetch-failed comment_id=%s error=%s", comment_id, exc)
            if args.fail_on_item_error:
                raise RuntimeError(
                    f"Detail fetch failed for comment_id={comment_id}: {exc}"
                ) from exc

    output_file = None
    if args.save_response and args.include_records:
        output_file_path = build_output_file_path(
            output_dir=Path(args.output_dir).expanduser().resolve(),
            output_file=args.output_file,
        )
        save_records_jsonl(path=output_file_path, records=records, overwrite=args.overwrite)
        output_file = str(output_file_path)
        logger.info("detail-records-saved path=%s count=%d", output_file_path, len(records))

    result = {
        "ok": len(failures) == 0,
        "source": "regulationsgov-v4-comment-detail",
        "requested_count": len(selected_ids),
        "success_count": len(records),
        "failure_count": len(failures),
        "records_with_validation_issues": records_with_issues,
        "validation_issue_count": issue_count_total,
        "include": include_value or None,
        "records_included": args.include_records,
        "save_response": args.save_response,
        "output_file": output_file,
        "failures": failures,
        "rate_limit_last": rate_limit_last,
        "records": records,
    }

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
        help=f"Initial retry delay seconds. Env: {ENV_RETRY_BACKOFF_SECONDS}.",
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
        "--max-comment-ids-per-run",
        type=int,
        default=None,
        help=f"Safety cap for IDs per run. Env: {ENV_MAX_COMMENT_IDS_PER_RUN}.",
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
        description="Fetch Regulations.gov v4 comment details by comment IDs."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check-config", help="Show effective runtime config and source URLs.")
    add_runtime_config_args(check)
    check.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")

    fetch = sub.add_parser("fetch", help="Fetch comment details for IDs from args/files.")
    add_runtime_config_args(fetch)
    add_logging_args(fetch)
    fetch.add_argument(
        "--comment-id",
        action="append",
        default=[],
        help="Comment ID (repeatable).",
    )
    fetch.add_argument(
        "--comment-ids-file",
        action="append",
        default=[],
        help=(
            "Path to file containing IDs. Supports txt/jsonl (one ID or one JSON object per line) "
            "and json (array/object). Repeatable."
        ),
    )
    fetch.add_argument(
        "--dedupe",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Deduplicate IDs while keeping first-seen order.",
    )
    fetch.add_argument(
        "--max-comments",
        type=int,
        default=0,
        help="Limit number of selected IDs (0 means all selected IDs).",
    )
    fetch.add_argument(
        "--include",
        default="",
        help="Optional include query parameter; currently only 'attachments'.",
    )
    fetch.add_argument(
        "--include-records",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include fetched detail objects in stdout JSON output.",
    )
    fetch.add_argument(
        "--max-validation-issues",
        type=int,
        default=DEFAULT_MAX_VALIDATION_ISSUES,
        help=f"Maximum validation issues retained per comment. Default: {DEFAULT_MAX_VALIDATION_ISSUES}.",
    )
    fetch.add_argument(
        "--fail-on-validation-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Fail if detail validation reports issues.",
    )
    fetch.add_argument(
        "--fail-on-item-error",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Fail immediately when one ID fetch fails.",
    )
    fetch.add_argument(
        "--quarantine-dir",
        default="",
        help="Optional directory to save validation issue JSONL files.",
    )
    fetch.add_argument(
        "--save-response",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Save successful detail wrapper records into JSONL.",
    )
    fetch.add_argument(
        "--output-dir",
        default="./data/regulationsgov-comment-details",
        help="Output directory for saved JSONL.",
    )
    fetch.add_argument(
        "--output-file",
        default="",
        help="Optional explicit output JSONL path.",
    )
    fetch.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if exists.",
    )
    fetch.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate input and print request plan without remote calls.",
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
