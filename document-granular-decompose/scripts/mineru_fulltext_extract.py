#!/usr/bin/env python3
"""Extract fulltext from TianGong MinerU with images API."""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import uuid
from pathlib import Path
from urllib import parse, request
from urllib.error import HTTPError, URLError

DEFAULT_API_PATH = "/mineru_with_images"
ENV_AUTH_TOKEN = "UNSTRUCTURED_AUTH_TOKEN"
ENV_PROVIDER = "UNSTRUCTURED_PROVIDER"
ENV_MODEL = "UNSTRUCTURED_MODEL"
ENV_API_BASE_URL = "UNSTRUCTURED_API_BASE_URL"
SUPPORTED_FILE_TYPES = (
    ".bmp",
    ".doc",
    ".docm",
    ".docx",
    ".dot",
    ".dotx",
    ".gif",
    ".jp2",
    ".jpeg",
    ".jpg",
    ".markdown",
    ".md",
    ".odp",
    ".odt",
    ".pdf",
    ".png",
    ".pot",
    ".potx",
    ".pps",
    ".ppsx",
    ".ppt",
    ".pptm",
    ".pptx",
    ".tiff",
    ".webp",
    ".xls",
    ".xlsm",
    ".xlsx",
    ".xlt",
    ".xltx",
)
OFFICE_FILE_TYPES = (
    ".doc",
    ".docm",
    ".docx",
    ".dot",
    ".dotx",
    ".odp",
    ".odt",
    ".pot",
    ".potx",
    ".pps",
    ".ppsx",
    ".ppt",
    ".pptm",
    ".pptx",
    ".xls",
    ".xlsm",
    ".xlsx",
    ".xlt",
    ".xltx",
)
SUPPORTED_FILE_TYPES_SET = set(SUPPORTED_FILE_TYPES)
SUPPORTED_FILE_TYPES_TEXT = ", ".join(SUPPORTED_FILE_TYPES)
OFFICE_FILE_TYPES_TEXT = ", ".join(OFFICE_FILE_TYPES)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload a file to /mineru_with_images and print fulltext only."
    )
    parser.add_argument("--file", required=True, help="Local file path to parse.")
    parser.add_argument(
        "--api-url",
        default="",
        help=(
            "Optional full endpoint URL override. "
            "If omitted, require UNSTRUCTURED_API_BASE_URL and append /mineru_with_images."
        ),
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional output path to save fulltext. Defaults to stdout.",
    )
    parser.add_argument("--timeout", type=int, default=600, help="Request timeout in seconds.")
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification (debug use only).",
    )
    return parser.parse_args()


def env_required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def env_optional(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def validate_file_type(file_path: Path) -> None:
    suffix = file_path.suffix.lower()
    if not suffix:
        raise ValueError(
            "File must include an extension. "
            f"Supported file types: {SUPPORTED_FILE_TYPES_TEXT}."
        )
    if suffix not in SUPPORTED_FILE_TYPES_SET:
        raise ValueError(
            f"Unsupported file type '{suffix}'. "
            f"Supported file types: {SUPPORTED_FILE_TYPES_TEXT}."
        )


def resolve_api_url(api_url_arg: str) -> str:
    explicit = api_url_arg.strip()
    if explicit:
        return explicit

    base_url = os.environ.get(ENV_API_BASE_URL, "").strip()
    if not base_url:
        raise ValueError(
            f"Missing required environment variable: {ENV_API_BASE_URL} "
            "(for example: https://your-unstructured-host:7770)"
        )
    return base_url.rstrip("/") + DEFAULT_API_PATH


def with_return_txt_true(api_url: str) -> str:
    parsed = parse.urlsplit(api_url)
    query = parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [(k, v) for k, v in query if k != "return_txt"]
    query.append(("return_txt", "true"))
    return parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, parse.urlencode(query), parsed.fragment)
    )


def build_multipart_body(
    fields: dict[str, str], file_field: str, file_path: Path
) -> tuple[bytes, str]:
    boundary = f"----tiangong-skill-{uuid.uuid4().hex}"
    boundary_bytes = boundary.encode("utf-8")
    body = bytearray()

    for key, value in fields.items():
        body.extend(b"--" + boundary_bytes + b"\r\n")
        body.extend(
            f'Content-Disposition: form-data; name="{key}"\r\n\r\n{value}\r\n'.encode("utf-8")
        )

    file_bytes = file_path.read_bytes()
    body.extend(b"--" + boundary_bytes + b"\r\n")
    body.extend(
        (
            f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{file_path.name}"\r\n'
        ).encode("utf-8")
    )
    body.extend(b"Content-Type: application/octet-stream\r\n\r\n")
    body.extend(file_bytes)
    body.extend(b"\r\n")
    body.extend(b"--" + boundary_bytes + b"--\r\n")

    return bytes(body), boundary


def parse_api_error(error: HTTPError) -> str:
    try:
        payload = error.read().decode("utf-8", errors="replace")
    except Exception:
        payload = ""
    if not payload:
        return f"HTTP {error.code}: {error.reason}"
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return f"HTTP {error.code}: {payload}"
    detail = data.get("detail")
    if detail:
        return f"HTTP {error.code}: {detail}"
    return f"HTTP {error.code}: {payload}"


def request_fulltext(
    api_url: str,
    file_path: Path,
    token: str,
    provider: str | None,
    model: str | None,
    timeout: int,
    insecure: bool,
) -> str:
    url = with_return_txt_true(api_url)
    fields: dict[str, str] = {}
    if provider:
        fields["provider"] = provider
    if model:
        fields["model"] = model
    body, boundary = build_multipart_body(
        fields=fields,
        file_field="file",
        file_path=file_path,
    )
    req = request.Request(url, data=body, method="POST")
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/json")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")

    context = None
    if insecure:
        context = ssl._create_unverified_context()

    try:
        with request.urlopen(req, timeout=timeout, context=context) as response:
            payload_text = response.read().decode("utf-8")
    except HTTPError as exc:
        raise RuntimeError(parse_api_error(exc)) from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed: {exc}") from exc
    except TimeoutError as exc:
        raise RuntimeError("Request timed out.") from exc

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("API did not return valid JSON.") from exc

    txt = payload.get("txt")
    if isinstance(txt, str) and txt.strip():
        return txt.strip()

    result = payload.get("result")
    if isinstance(result, list):
        chunks: list[str] = []
        for item in result:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str) and text.strip():
                    chunks.append(text.strip())
        if chunks:
            return "\n\n".join(chunks)

    raise RuntimeError("No fulltext content found in API response ('txt' or 'result[].text').")


def main() -> int:
    args = parse_args()
    try:
        api_url = resolve_api_url(args.api_url)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    file_path = Path(args.file).expanduser().resolve()
    if not file_path.exists():
        print(f"File not found: {file_path}", file=sys.stderr)
        return 2
    if not file_path.is_file():
        print(f"Not a regular file: {file_path}", file=sys.stderr)
        return 2
    try:
        validate_file_type(file_path)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        print(f"Office formats supported: {OFFICE_FILE_TYPES_TEXT}.", file=sys.stderr)
        return 2

    try:
        token = env_required(ENV_AUTH_TOKEN)
        provider = env_optional(ENV_PROVIDER)
        model = env_optional(ENV_MODEL)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    try:
        fulltext = request_fulltext(
            api_url=api_url,
            file_path=file_path,
            token=token,
            provider=provider,
            model=model,
            timeout=max(1, args.timeout),
            insecure=bool(args.insecure),
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(fulltext, encoding="utf-8")
        print(str(output_path))
    else:
        print(fulltext)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
