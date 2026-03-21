#!/usr/bin/env python3
"""Upload local files to Dify and run the 智水大师信息萃取 workflow."""

from __future__ import annotations

import argparse
import json
import mimetypes
import os
import random
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, NoReturn


DEFAULT_USER = "codex-dify-water-info-extract"
DEFAULT_ENV_FILE = Path(__file__).resolve().parent.parent / ".env.workflow.local"
DEFAULT_MAX_FILE_SIZE_MB = 500
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"}
DOCUMENT_EXTENSIONS = {
    ".txt",
    ".md",
    ".pdf",
    ".doc",
    ".docx",
    ".rtf",
    ".csv",
    ".tsv",
    ".json",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload local transcript documents and photos to Dify, then run the "
            "智水大师信息萃取 workflow in blocking mode."
        )
    )
    parser.add_argument(
        "--raw-script",
        action="append",
        default=[],
        metavar="PATH",
        help="Local transcript/document file for the raw_scripts input. Repeatable.",
    )
    parser.add_argument(
        "--photo",
        action="append",
        default=[],
        metavar="PATH",
        help="Local image file for the photos input. Repeatable.",
    )
    parser.add_argument(
        "--filename",
        help=(
            "Optional text value for the workflow's filename input. When provided, "
            "it is sent as inputs.filename."
        ),
    )
    parser.add_argument(
        "--inputs-json",
        help=(
            "Optional JSON file with extra workflow inputs. If it already contains "
            "raw_scripts or photos, uploaded files are appended."
        ),
    )
    parser.add_argument(
        "--env-file",
        help=(
            "Optional dotenv-style file with Dify workflow settings. Defaults to "
            f"{DEFAULT_ENV_FILE} when that file exists."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        help=(
            "Dify API base URL including /v1. Defaults to DIFY_WORKFLOW_API_BASE_URL "
            "or DIFY_API_BASE_URL."
        ),
    )
    parser.add_argument(
        "--api-key",
        help=(
            "Dify app API key. Defaults to DIFY_WORKFLOW_API_KEY or DIFY_API_KEY."
        ),
    )
    parser.add_argument(
        "--user",
        help=(
            "User identifier sent to Dify. Defaults to DIFY_WORKFLOW_USER or "
            f"'{DEFAULT_USER}'."
        ),
    )
    parser.add_argument(
        "--output-file",
        help="Optional file path to write the JSON response.",
    )
    parser.add_argument(
        "--upload-retries",
        type=int,
        default=3,
        help="Number of retries for transient upload/network failures. Default: 3.",
    )
    parser.add_argument(
        "--workflow-retries",
        type=int,
        default=0,
        help=(
            "Number of retries for /workflows/run. Default: 0 to avoid duplicate "
            "workflow executions."
        ),
    )
    parser.add_argument(
        "--retry-delay-seconds",
        type=float,
        default=3.0,
        help="Seconds to wait between retry attempts. Default: 3.0.",
    )
    parser.add_argument(
        "--connect-timeout-seconds",
        type=int,
        default=30,
        help="curl connect timeout in seconds. Default: 30.",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=1800,
        help="curl max request time in seconds. Default: 1800 (30 minutes).",
    )
    parser.add_argument(
        "--state-file",
        help=(
            "Optional JSON file for progress/state snapshots. Helpful for debugging "
            "or recovering after interrupted runs."
        ),
    )
    parser.add_argument(
        "--max-file-size-mb",
        type=int,
        default=DEFAULT_MAX_FILE_SIZE_MB,
        help="Reject local files larger than this size before upload. Default: 500 MB.",
    )
    parser.add_argument(
        "--skip-file-validation",
        action="store_true",
        help="Skip local extension/MIME/signature validation before upload.",
    )
    parser.add_argument(
        "--print-outputs-only",
        action="store_true",
        help="Print only workflow data.outputs when available.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and print the planned requests without calling Dify.",
    )
    return parser.parse_args()


def fail(message: str, *, exit_code: int = 1) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(exit_code)


def log(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def maybe_write_json(path_str: str | None, payload: Any) -> None:
    if not path_str:
        return
    write_json(path_str, payload)


def require_setting(explicit_value: str | None, env_names: list[str], cli_flag: str) -> str:
    if explicit_value:
        return explicit_value
    for env_name in env_names:
        value = os.getenv(env_name)
        if value:
            return value
    env_display = " or ".join(env_names)
    fail(f"Missing setting. Provide {cli_flag} or set {env_display}.")


def load_json_file(path_str: str) -> Any:
    path = Path(path_str).expanduser().resolve()
    if not path.is_file():
        fail(f"JSON file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        fail(f"Invalid JSON in {path}: {exc}")


def load_env_file(path_str: str) -> None:
    path = Path(path_str).expanduser().resolve()
    if not path.is_file():
        fail(f"Env file not found: {path}")

    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            fail(f"Invalid env assignment at {path}:{line_number}")
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            fail(f"Empty env key at {path}:{line_number}")
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ[key] = value


def ensure_input_map(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        fail("--inputs-json must contain a JSON object at the top level")
    return value


def resolve_files(paths: list[str], label: str) -> list[Path]:
    resolved: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path).expanduser().resolve()
        if not path.is_file():
            fail(f"{label} file not found: {path}")
        resolved.append(path)
    return resolved


def is_likely_text_bytes(data: bytes) -> bool:
    if not data:
        return True
    if b"\x00" in data:
        return False
    try:
        data.decode("utf-8")
        return True
    except UnicodeDecodeError:
        return False


def detect_mime_from_content(file_path: Path) -> str | None:
    header = file_path.read_bytes()[:4096]
    lower_header = header.lower()

    if header.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if header.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if header.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(header) >= 12 and header[:4] == b"RIFF" and header[8:12] == b"WEBP":
        return "image/webp"
    if b"<svg" in lower_header:
        return "image/svg+xml"
    if header.startswith(b"%PDF-"):
        return "application/pdf"
    if header.startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"):
        return "application/msword"
    if header.startswith(b"PK\x03\x04"):
        suffix = file_path.suffix.lower()
        if suffix == ".docx":
            return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        return "application/zip"
    if lower_header.startswith(b"{\\rtf"):
        return "application/rtf"
    if is_likely_text_bytes(header):
        return "text/plain"
    return None


def guessed_mime(file_path: Path) -> str | None:
    guessed, _ = mimetypes.guess_type(file_path.name)
    return guessed


def expected_extensions_for_label(label: str) -> set[str]:
    if label == "photos":
        return IMAGE_EXTENSIONS
    if label == "raw_scripts":
        return DOCUMENT_EXTENSIONS
    return set()


def mime_matches_label(label: str, mime: str | None) -> bool:
    if mime is None:
        return False
    if label == "photos":
        return mime.startswith("image/")
    if label == "raw_scripts":
        return (
            mime.startswith("text/")
            or mime in {
                "application/pdf",
                "application/msword",
                "application/rtf",
                "application/json",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/zip",
            }
        )
    return False


def validate_local_file(file_path: Path, label: str, max_size_bytes: int) -> None:
    file_size = file_path.stat().st_size
    if file_size <= 0:
        fail(f"{label} file is empty: {file_path}")
    if file_size > max_size_bytes:
        fail(
            f"{label} file exceeds size limit ({file_size} bytes > {max_size_bytes} bytes): "
            f"{file_path}"
        )

    suffix = file_path.suffix.lower()
    allowed_extensions = expected_extensions_for_label(label)
    if suffix not in allowed_extensions:
        allowed_display = ", ".join(sorted(allowed_extensions))
        fail(
            f"{label} file extension is not allowed for this skill: {file_path} "
            f"(got {suffix or '<none>'}; allowed: {allowed_display})"
        )

    guessed = guessed_mime(file_path)
    detected = detect_mime_from_content(file_path)

    if guessed is not None and not mime_matches_label(label, guessed):
        fail(f"{label} file has unexpected MIME from extension mapping ({guessed}): {file_path}")
    if detected is None:
        fail(f"{label} file signature/content could not be validated: {file_path}")
    if not mime_matches_label(label, detected):
        fail(f"{label} file content does not match expected type ({detected}): {file_path}")
    if guessed is not None and label == "photos" and guessed != detected:
        fail(
            f"{label} file extension/MIME and content disagree ({guessed} vs {detected}): "
            f"{file_path}"
        )


def api_base(base_url: str) -> str:
    return base_url.rstrip("/")


def is_retryable_curl_exit_code(return_code: int) -> bool:
    return return_code in {5, 6, 7, 18, 28, 35, 52, 55, 56}


def run_curl_json(
    command: list[str],
    *,
    label: str,
    retries: int,
    retry_delay_seconds: float,
    timeout_seconds: int,
) -> Any:
    attempts = max(1, retries + 1)
    last_error = "curl failed"

    for attempt in range(1, attempts + 1):
        result = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
        )
        if result.returncode == 0:
            try:
                return json.loads(result.stdout)
            except json.JSONDecodeError as exc:
                fail(f"{label}: expected JSON response from Dify, got parse error: {exc}")

        parts = [part for part in [result.stderr.strip(), result.stdout.strip()] if part]
        last_error = "\n".join(parts) if parts else "curl failed"
        if result.returncode == 28:
            last_error = (
                f"{last_error}\n{label}: request exceeded the configured timeout of "
                f"{timeout_seconds} seconds."
            )

        if attempt >= attempts or not is_retryable_curl_exit_code(result.returncode):
            fail(f"{label}: {last_error}")

        delay = retry_delay_seconds * (2 ** (attempt - 1))
        delay += random.uniform(0, min(1.0, retry_delay_seconds))
        log(
            f"{label}: transient curl failure (exit {result.returncode}), "
            f"retrying in {delay:.1f}s [{attempt}/{attempts - 1}]"
        )
        time.sleep(delay)

    fail(f"{label}: {last_error}")


def upload_file(
    *,
    base_url: str,
    api_key: str,
    user: str,
    file_path: Path,
    retries: int,
    retry_delay_seconds: float,
    connect_timeout_seconds: int,
    request_timeout_seconds: int,
) -> dict[str, Any]:
    endpoint = f"{api_base(base_url)}/files/upload"
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        "--connect-timeout",
        str(connect_timeout_seconds),
        "--max-time",
        str(request_timeout_seconds),
        "--request",
        "POST",
        endpoint,
        "--header",
        f"Authorization: Bearer {api_key}",
        "--form",
        f"user={user}",
        "--form",
        f"file=@{file_path}",
    ]
    response = run_curl_json(
        command,
        label=f"upload {file_path.name}",
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=request_timeout_seconds,
    )
    if not isinstance(response, dict):
        fail("Upload response must be a JSON object")
    file_id = response.get("id")
    if not isinstance(file_id, str) or not file_id:
        fail("Upload response is missing file id")
    return response


def build_file_input(upload_response: dict[str, Any], file_type: str) -> dict[str, str]:
    upload_file_id = upload_response["id"]
    return {
        "transfer_method": "local_file",
        "upload_file_id": upload_file_id,
        "type": file_type,
    }


def append_file_inputs(inputs: dict[str, Any], key: str, objects: list[dict[str, str]]) -> None:
    if not objects:
        return
    existing = inputs.get(key)
    if existing is None:
        inputs[key] = list(objects)
        return
    if not isinstance(existing, list):
        fail(f"inputs.{key} must be a list when provided through --inputs-json")
    existing.extend(objects)


def set_text_input(inputs: dict[str, Any], key: str, value: str | None) -> None:
    if value is None:
        return
    inputs[key] = value


def run_workflow(
    *,
    base_url: str,
    api_key: str,
    user: str,
    inputs: dict[str, Any],
    retries: int,
    retry_delay_seconds: float,
    connect_timeout_seconds: int,
    request_timeout_seconds: int,
) -> Any:
    endpoint = f"{api_base(base_url)}/workflows/run"
    payload = {
        "inputs": inputs,
        "response_mode": "blocking",
        "user": user,
    }
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        "--connect-timeout",
        str(connect_timeout_seconds),
        "--max-time",
        str(request_timeout_seconds),
        "--request",
        "POST",
        endpoint,
        "--header",
        "Content-Type: application/json",
        "--header",
        f"Authorization: Bearer {api_key}",
        "--data",
        json.dumps(payload, ensure_ascii=False),
    ]
    return run_curl_json(
        command,
        label="run workflow",
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        timeout_seconds=request_timeout_seconds,
    )


def workflow_outputs(response: Any) -> Any:
    if not isinstance(response, dict):
        return response
    data = response.get("data")
    if not isinstance(data, dict):
        return response
    outputs = data.get("outputs")
    if outputs is None:
        return response
    return outputs


def write_json(path_str: str, payload: Any) -> None:
    path = Path(path_str).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    temp_path.replace(path)


def main() -> None:
    args = parse_args()

    env_file = args.env_file
    if env_file:
        load_env_file(env_file)
    elif DEFAULT_ENV_FILE.is_file():
        load_env_file(str(DEFAULT_ENV_FILE))

    base_url = require_setting(
        args.api_base_url,
        ["DIFY_WORKFLOW_API_BASE_URL", "DIFY_API_BASE_URL"],
        "--api-base-url",
    )
    api_key = require_setting(
        args.api_key,
        ["DIFY_WORKFLOW_API_KEY", "DIFY_API_KEY"],
        "--api-key",
    )
    user = args.user or os.getenv("DIFY_WORKFLOW_USER") or DEFAULT_USER

    raw_script_paths = resolve_files(args.raw_script, "raw_scripts")
    photo_paths = resolve_files(args.photo, "photos")
    inputs = ensure_input_map(load_json_file(args.inputs_json) if args.inputs_json else None)
    max_size_bytes = args.max_file_size_mb * 1024 * 1024

    if not args.skip_file_validation:
        for path in raw_script_paths:
            validate_local_file(path, "raw_scripts", max_size_bytes)
        for path in photo_paths:
            validate_local_file(path, "photos", max_size_bytes)

    set_text_input(inputs, "filename", args.filename)

    if not raw_script_paths and not photo_paths and not inputs:
        fail(
            "No workflow inputs provided. Pass at least one --raw-script, --photo, "
            "or a non-empty --inputs-json."
        )

    if args.dry_run:
        plan = {
            "api_base_url": api_base(base_url),
            "user": user,
            "upload_endpoint": f"{api_base(base_url)}/files/upload",
            "workflow_endpoint": f"{api_base(base_url)}/workflows/run",
            "raw_scripts": [str(path) for path in raw_script_paths],
            "photos": [str(path) for path in photo_paths],
            "filename": args.filename,
            "inputs_json": inputs,
            "response_mode": "blocking",
            "upload_retries": args.upload_retries,
            "workflow_retries": args.workflow_retries,
            "retry_delay_seconds": args.retry_delay_seconds,
            "connect_timeout_seconds": args.connect_timeout_seconds,
            "request_timeout_seconds": args.request_timeout_seconds,
            "state_file": args.state_file,
            "max_file_size_mb": args.max_file_size_mb,
            "skip_file_validation": args.skip_file_validation,
        }
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return

    state: dict[str, Any] = {
        "started_at": int(time.time()),
        "api_base_url": api_base(base_url),
        "user": user,
        "raw_scripts": [str(path) for path in raw_script_paths],
        "photos": [str(path) for path in photo_paths],
        "filename": args.filename,
        "upload_results": {"raw_scripts": [], "photos": []},
        "workflow": {
            "status": "pending",
            "request_timeout_seconds": args.request_timeout_seconds,
            "workflow_retries": args.workflow_retries,
        },
    }
    maybe_write_json(args.state_file, state)

    log(
        "Starting Dify workflow run: "
        f"{len(raw_script_paths)} raw_scripts, {len(photo_paths)} photos."
    )

    raw_script_objects = []
    for path in raw_script_paths:
        log(f"Uploading raw_scripts file: {path.name}")
        raw_script_objects.append(
            build_file_input(
                upload_file(
                    base_url=base_url,
                    api_key=api_key,
                    user=user,
                    file_path=path,
                    retries=args.upload_retries,
                    retry_delay_seconds=args.retry_delay_seconds,
                    connect_timeout_seconds=args.connect_timeout_seconds,
                    request_timeout_seconds=args.request_timeout_seconds,
                ),
                "document",
            )
        )
        state["upload_results"]["raw_scripts"].append(
            {
                "path": str(path),
                "upload_file_id": raw_script_objects[-1]["upload_file_id"],
            }
        )
        maybe_write_json(args.state_file, state)

    photo_objects = []
    for path in photo_paths:
        log(f"Uploading photos file: {path.name}")
        photo_objects.append(
            build_file_input(
                upload_file(
                    base_url=base_url,
                    api_key=api_key,
                    user=user,
                    file_path=path,
                    retries=args.upload_retries,
                    retry_delay_seconds=args.retry_delay_seconds,
                    connect_timeout_seconds=args.connect_timeout_seconds,
                    request_timeout_seconds=args.request_timeout_seconds,
                ),
                "image",
            )
        )
        state["upload_results"]["photos"].append(
            {
                "path": str(path),
                "upload_file_id": photo_objects[-1]["upload_file_id"],
            }
        )
        maybe_write_json(args.state_file, state)

    append_file_inputs(inputs, "raw_scripts", raw_script_objects)
    append_file_inputs(inputs, "photos", photo_objects)

    state["workflow"]["status"] = "running"
    state["workflow"]["request_started_at"] = int(time.time())
    state["workflow"]["inputs"] = inputs
    maybe_write_json(args.state_file, state)

    log(
        "Running workflow in blocking mode. "
        f"Timeout threshold: {args.request_timeout_seconds // 60} minutes."
    )
    try:
        response = run_workflow(
            base_url=base_url,
            api_key=api_key,
            user=user,
            inputs=inputs,
            retries=args.workflow_retries,
            retry_delay_seconds=args.retry_delay_seconds,
            connect_timeout_seconds=args.connect_timeout_seconds,
            request_timeout_seconds=args.request_timeout_seconds,
        )
    except SystemExit:
        state["workflow"]["status"] = "failed"
        state["workflow"]["failed_at"] = int(time.time())
        maybe_write_json(args.state_file, state)
        raise

    state["workflow"]["status"] = "succeeded"
    state["workflow"]["finished_at"] = int(time.time())
    maybe_write_json(args.state_file, state)
    payload = workflow_outputs(response) if args.print_outputs_only else response

    if args.output_file:
        log(f"Writing response JSON to: {args.output_file}")
        write_json(args.output_file, payload)

    print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
