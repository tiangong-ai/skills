#!/usr/bin/env python3
"""Upload a local file through a published Dify pipeline and optionally write metadata."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, NoReturn

TERMINAL_INDEXING_STATUSES = {
    "cancelled",
    "completed",
    "error",
    "failed",
    "stopped",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload a file to a published Dify pipeline knowledge base, poll "
            "indexing status, and optionally update document metadata."
        )
    )
    parser.add_argument("--file", required=True, help="Local file path to upload")
    parser.add_argument(
        "--inputs-json",
        help="Optional JSON file for the pipeline 'inputs' object. Defaults to {}.",
    )
    parser.add_argument(
        "--data-json",
        dest="data_json",
        help="Deprecated alias for --inputs-json.",
    )
    parser.add_argument(
        "--metadata-json",
        help=(
            "Optional JSON file for metadata values. Supports a flat object of "
            "name->value pairs or a list of {name|id, value} objects."
        ),
    )
    parser.add_argument(
        "--api-base-url",
        help="Dify API base URL. Defaults to env DIFY_API_BASE_URL.",
    )
    parser.add_argument(
        "--dataset-id",
        help="Dify dataset ID. Defaults to env DIFY_DATASET_ID.",
    )
    parser.add_argument(
        "--api-key",
        help="Dify API key. Defaults to env DIFY_API_KEY.",
    )
    parser.add_argument(
        "--pipeline-start-node-id",
        help=(
            "Optional published pipeline start node id. Defaults to env "
            "DIFY_PIPELINE_START_NODE_ID or live datasource discovery."
        ),
    )
    parser.add_argument(
        "--pipeline-datasource-type",
        help=(
            "Optional pipeline datasource type. Defaults to env "
            "DIFY_PIPELINE_DATASOURCE_TYPE or live datasource discovery."
        ),
    )
    parser.add_argument(
        "--response-mode",
        help=(
            "Pipeline response_mode. Defaults to env "
            "DIFY_PIPELINE_RESPONSE_MODE or 'blocking'."
        ),
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        help=(
            "Seconds between indexing-status polls. Defaults to env "
            "DIFY_POLL_INTERVAL_SECONDS or 2."
        ),
    )
    parser.add_argument(
        "--poll-timeout-seconds",
        type=float,
        help=(
            "Maximum seconds to wait for completed indexing. Defaults to env "
            "DIFY_POLL_TIMEOUT_SECONDS or 300."
        ),
    )
    parser.add_argument(
        "--metadata-page-size",
        type=int,
        default=100,
        help="Page size for listing dataset metadata fields. Default: 100",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and print the planned requests without calling Dify",
    )
    return parser.parse_args()


def load_dotenv_if_exists() -> None:
    candidates = [
        Path.cwd() / ".env",
        Path(__file__).resolve().parent.parent / ".env",
        Path(__file__).resolve().parents[2] / ".env",
    ]
    seen: set[Path] = set()

    for env_path in candidates:
        if env_path in seen or not env_path.is_file():
            continue
        seen.add(env_path)

        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if len(value) >= 2 and (
                (value[0] == '"' and value[-1] == '"')
                or (value[0] == "'" and value[-1] == "'")
            ):
                value = value[1:-1]
            if value == "":
                continue
            if key not in os.environ:
                os.environ[key] = value


def fail(message: str, *, exit_code: int = 1) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(exit_code)


def require_setting(
    explicit_value: str | None,
    *,
    env_name: str,
    cli_flag: str,
    fallback: str | None = None,
) -> str:
    value = explicit_value or os.getenv(env_name)
    if value:
        return value
    if fallback is not None:
        return fallback
    fail(f"Missing setting. Provide {cli_flag} or set {env_name}.")


def optional_setting(explicit_value: str | None, *, env_name: str) -> str | None:
    value = explicit_value or os.getenv(env_name)
    if value:
        return value
    return None


def load_json_file(path_str: str) -> Any:
    path = Path(path_str).expanduser().resolve()
    if not path.is_file():
        fail(f"JSON file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        fail(f"Invalid JSON in {path}: {exc}")


def default_pipeline_inputs() -> dict[str, Any]:
    return {}


def normalize_metadata_input(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, dict):
        return [{"name": name, "value": value} for name, value in raw.items()]

    if isinstance(raw, list):
        items: list[dict[str, Any]] = []
        for index, item in enumerate(raw, start=1):
            if not isinstance(item, dict):
                fail(f"metadata list item {index} must be an object")
            if "value" not in item:
                fail(f"metadata list item {index} must include 'value'")
            if "name" not in item and "id" not in item:
                fail(f"metadata list item {index} must include 'name' or 'id'")
            items.append(item)
        return items

    fail("metadata JSON must be an object or a list")


def api_base(base_url: str) -> str:
    return base_url.rstrip("/")


def print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))


def coerce_nonempty_str(value: Any, *, field_name: str) -> str:
    if value is None:
        fail(f"Response is missing {field_name}")
    text = str(value).strip()
    if not text:
        fail(f"Response is missing {field_name}")
    return text


def coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return int(stripped)
        except ValueError:
            return None
    return None


def require_positive_float(value: float, *, flag_name: str) -> float:
    if value <= 0:
        fail(f"{flag_name} must be greater than 0")
    return value


def resolve_float_setting(
    explicit_value: float | None,
    *,
    env_name: str,
    flag_name: str,
    fallback: float,
) -> float:
    if explicit_value is not None:
        return require_positive_float(explicit_value, flag_name=flag_name)

    raw = os.getenv(env_name, "").strip()
    if raw:
        try:
            return require_positive_float(float(raw), flag_name=env_name)
        except ValueError:
            fail(f"{env_name} must be a number, got: {raw!r}")

    return require_positive_float(fallback, flag_name=flag_name)


def run_curl_json(command: list[str]) -> Any:
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        text=True,
    )
    if result.returncode != 0:
        parts = [part for part in [result.stderr.strip(), result.stdout.strip()] if part]
        message = "\n".join(parts) if parts else "curl failed"
        fail(message)

    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        fail(f"Expected JSON response from Dify, got parse error: {exc}")


def discover_datasource_plugin(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    requested_start_node_id: str | None,
    requested_datasource_type: str | None,
) -> tuple[dict[str, Any], Any]:
    endpoint = f"{api_base(base_url)}/datasets/{dataset_id}/pipeline/datasource-plugins"
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        "--get",
        endpoint,
        "--header",
        f"Authorization: Bearer {api_key}",
        "--data-urlencode",
        "is_published=true",
    ]
    response = run_curl_json(command)

    raw_items: list[Any]
    if isinstance(response, list):
        raw_items = response
    elif isinstance(response, dict):
        if isinstance(response.get("data"), list):
            raw_items = response["data"]
        elif isinstance(response.get("datasource_plugins"), list):
            raw_items = response["datasource_plugins"]
        else:
            fail("Datasource discovery response is missing a datasource plugin list")
    else:
        fail("Datasource discovery response must be a JSON array or object")

    items = [item for item in raw_items if isinstance(item, dict)]
    if not items:
        fail("No published datasource plugin found for this dataset")

    candidates = items
    if requested_start_node_id:
        candidates = [
            item for item in candidates if str(item.get("node_id", "")).strip() == requested_start_node_id
        ]
    if requested_datasource_type:
        candidates = [
            item
            for item in candidates
            if str(item.get("datasource_type", "")).strip() == requested_datasource_type
        ]

    if not candidates:
        filters: list[str] = []
        if requested_start_node_id:
            filters.append(f"start_node_id={requested_start_node_id}")
        if requested_datasource_type:
            filters.append(f"datasource_type={requested_datasource_type}")
        suffix = f" matching {' and '.join(filters)}" if filters else ""
        fail(f"No published datasource plugin found{suffix}")

    selected: dict[str, Any]
    if len(candidates) == 1:
        selected = candidates[0]
    else:
        local_file_candidates = [
            item
            for item in candidates
            if str(item.get("datasource_type", "")).strip() == "local_file"
        ]
        if len(local_file_candidates) == 1:
            selected = local_file_candidates[0]
        else:
            fail(
                "Multiple published datasource plugins match this dataset. "
                "Provide --pipeline-start-node-id and/or --pipeline-datasource-type."
            )

    datasource_type = requested_datasource_type or str(selected.get("datasource_type", "")).strip()
    start_node_id = requested_start_node_id or str(selected.get("node_id", "")).strip()
    if not datasource_type:
        fail("Published datasource plugin is missing datasource_type")
    if not start_node_id:
        fail("Published datasource plugin is missing node_id")

    return (
        {
            "datasource_type": datasource_type,
            "plugin_id": selected.get("plugin_id"),
            "provider_name": selected.get("provider_name"),
            "start_node_id": start_node_id,
            "title": selected.get("title"),
            "user_input_variables": selected.get("user_input_variables"),
        },
        response,
    )


def upload_pipeline_file(
    *,
    base_url: str,
    api_key: str,
    file_path: Path,
) -> dict[str, Any]:
    endpoint = f"{api_base(base_url)}/datasets/pipeline/file-upload"
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        "--request",
        "POST",
        endpoint,
        "--header",
        f"Authorization: Bearer {api_key}",
        "--form",
        f"file=@{file_path}",
    ]
    response = run_curl_json(command)
    if not isinstance(response, dict):
        fail("Pipeline file-upload response must be a JSON object")
    return response


def run_pipeline(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    inputs: dict[str, Any],
    datasource_type: str,
    file_id: str,
    file_name: str,
    start_node_id: str,
    response_mode: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    endpoint = f"{api_base(base_url)}/datasets/{dataset_id}/pipeline/run"
    payload = {
        "inputs": inputs,
        "datasource_type": datasource_type,
        "datasource_info_list": [
            {
                "related_id": file_id,
                "name": file_name,
                "transfer_method": datasource_type,
            }
        ],
        "start_node_id": start_node_id,
        "is_published": True,
        "response_mode": response_mode,
    }
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        "--request",
        "POST",
        endpoint,
        "--header",
        "Content-Type: application/json",
        "--header",
        f"Authorization: Bearer {api_key}",
        "--data-binary",
        json.dumps(payload, ensure_ascii=False),
    ]
    response = run_curl_json(command)
    if not isinstance(response, dict):
        fail("Pipeline run response must be a JSON object")
    return response, payload


def extract_batch_and_document_id(run_response: dict[str, Any]) -> tuple[str, str]:
    batch = coerce_nonempty_str(run_response.get("batch"), field_name="batch")

    documents = run_response.get("documents")
    if isinstance(documents, list):
        for item in documents:
            if isinstance(item, dict) and item.get("id") is not None:
                return batch, coerce_nonempty_str(item.get("id"), field_name="documents[0].id")

    document = run_response.get("document")
    if isinstance(document, dict) and document.get("id") is not None:
        return batch, coerce_nonempty_str(document.get("id"), field_name="document.id")

    fail("Pipeline run response is missing documents[0].id")


def get_indexing_status(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    batch: str,
) -> dict[str, Any]:
    endpoint = f"{api_base(base_url)}/datasets/{dataset_id}/documents/{batch}/indexing-status"
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        endpoint,
        "--header",
        f"Authorization: Bearer {api_key}",
    ]
    response = run_curl_json(command)
    if not isinstance(response, dict):
        fail("Indexing-status response must be a JSON object")
    return response


def poll_indexing_status(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    batch: str,
    interval_seconds: float,
    timeout_seconds: float,
) -> tuple[dict[str, Any], bool]:
    deadline = time.monotonic() + timeout_seconds
    while True:
        response = get_indexing_status(
            base_url=base_url,
            dataset_id=dataset_id,
            api_key=api_key,
            batch=batch,
        )
        payload = unwrap_indexing_status_payload(response)
        status = str(payload.get("indexing_status", "")).strip().lower()
        if status in TERMINAL_INDEXING_STATUSES:
            return response, False
        if time.monotonic() >= deadline:
            return response, True
        time.sleep(interval_seconds)


def get_document_details(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    document_id: str,
) -> dict[str, Any]:
    endpoint = f"{api_base(base_url)}/datasets/{dataset_id}/documents/{document_id}"
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        "--get",
        endpoint,
        "--header",
        f"Authorization: Bearer {api_key}",
        "--data-urlencode",
        "metadata=without",
    ]
    response = run_curl_json(command)
    if not isinstance(response, dict):
        fail("Document detail response must be a JSON object")
    return response


def list_metadata_fields(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    page_size: int,
) -> list[dict[str, Any]]:
    endpoint = f"{api_base(base_url)}/datasets/{dataset_id}/metadata"
    page = 1
    fields: list[dict[str, Any]] = []

    while True:
        command = [
            "curl",
            "-sS",
            "--fail-with-body",
            "--location",
            "--get",
            endpoint,
            "--header",
            f"Authorization: Bearer {api_key}",
            "--data-urlencode",
            f"page={page}",
            "--data-urlencode",
            f"limit={page_size}",
        ]
        response = run_curl_json(command)
        if not isinstance(response, dict):
            fail("Metadata list response must be a JSON object")

        page_items = response.get("data")
        if isinstance(page_items, list):
            fields.extend(item for item in page_items if isinstance(item, dict))

            total = response.get("total")
            if not page_items:
                break
            if isinstance(total, int) and len(fields) >= total:
                break
            if len(page_items) < page_size:
                break
            page += 1
            continue

        doc_metadata = response.get("doc_metadata")
        if isinstance(doc_metadata, list):
            fields.extend(item for item in doc_metadata if isinstance(item, dict))
            break

        fail("Metadata list response is missing 'data' or 'doc_metadata' array")

    return fields


def resolve_metadata_items(
    *,
    requested_items: list[dict[str, Any]],
    available_fields: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    by_name: dict[str, dict[str, Any]] = {}
    duplicate_names: set[str] = set()

    for field in available_fields:
        field_id = field.get("id")
        field_name = field.get("name")
        if isinstance(field_id, str):
            by_id[field_id] = field
        if isinstance(field_name, str):
            if field_name in by_name:
                duplicate_names.add(field_name)
            else:
                by_name[field_name] = field

    if duplicate_names:
        names = ", ".join(sorted(duplicate_names))
        fail(f"Dataset metadata contains duplicate field names; use ids instead: {names}")

    resolved: list[dict[str, Any]] = []
    unknown_names: list[str] = []
    unknown_ids: list[str] = []

    for item in requested_items:
        if "id" in item:
            field = by_id.get(str(item["id"]))
            if field is None:
                unknown_ids.append(str(item["id"]))
                continue
        else:
            name = str(item["name"])
            field = by_name.get(name)
            if field is None:
                unknown_names.append(name)
                continue

        resolved.append(
            {
                "id": field["id"],
                "name": field.get("name"),
                "value": item["value"],
            }
        )

    if unknown_names or unknown_ids:
        parts = []
        if unknown_names:
            parts.append("unknown metadata names: " + ", ".join(sorted(unknown_names)))
        if unknown_ids:
            parts.append("unknown metadata ids: " + ", ".join(sorted(unknown_ids)))
        known_names = sorted(name for name in by_name if isinstance(name, str))
        if known_names:
            parts.append("available names: " + ", ".join(known_names))
        fail("; ".join(parts))

    return resolved


def write_document_metadata(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    document_id: str,
    metadata_items: list[dict[str, Any]],
) -> dict[str, Any]:
    endpoint = f"{api_base(base_url)}/datasets/{dataset_id}/documents/metadata"
    payload_items = [
        {
            "id": item["id"],
            "name": item["name"],
            "value": item["value"],
        }
        for item in metadata_items
    ]
    payload = {
        "operation_data": [
            {
                "document_id": document_id,
                "metadata_list": payload_items,
            }
        ]
    }
    command = [
        "curl",
        "-sS",
        "--fail-with-body",
        "--location",
        "--request",
        "POST",
        endpoint,
        "--header",
        "Content-Type: application/json",
        "--header",
        f"Authorization: Bearer {api_key}",
        "--data-binary",
        json.dumps(payload, ensure_ascii=False),
    ]
    response = run_curl_json(command)
    if not isinstance(response, dict):
        fail("Metadata response must be a JSON object")
    return response


def unwrap_document_payload(document_response: dict[str, Any]) -> dict[str, Any]:
    for key in ("document", "data"):
        nested = document_response.get(key)
        if isinstance(nested, dict):
            return nested
    return document_response


def unwrap_indexing_status_payload(indexing_status_response: dict[str, Any]) -> dict[str, Any]:
    data = indexing_status_response.get("data")
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                return item
    return indexing_status_response


def build_validation_issues(
    *,
    indexing_status_response: dict[str, Any],
    indexing_timed_out: bool,
    timeout_seconds: float,
    document_response: dict[str, Any],
) -> tuple[list[str], list[str]]:
    issues: list[str] = []
    warnings: list[str] = []

    indexing_payload = unwrap_indexing_status_payload(indexing_status_response)
    indexing_status = str(indexing_payload.get("indexing_status", "")).strip()
    indexing_error = indexing_payload.get("error")
    total_segments = coerce_int(indexing_payload.get("total_segments"))

    document_payload = unwrap_document_payload(document_response)
    segment_count = coerce_int(document_payload.get("segment_count"))
    tokens = coerce_int(document_payload.get("tokens"))

    if indexing_timed_out:
        issues.append(
            f"indexing-status polling timed out after {timeout_seconds:g} seconds"
        )
    elif indexing_status != "completed":
        message = f"indexing_status is {indexing_status!r}, expected 'completed'"
        if indexing_error:
            message += f" (error: {indexing_error})"
        issues.append(message)

    if total_segments is None:
        issues.append("indexing-status response is missing total_segments")
    elif total_segments <= 0:
        issues.append(f"indexing-status total_segments is {total_segments}, expected > 0")

    if segment_count is None:
        issues.append("document detail response is missing segment_count")
    elif segment_count <= 0:
        issues.append(f"document segment_count is {segment_count}, expected > 0")

    if tokens is None:
        warnings.append("document detail response is missing tokens")
    elif tokens <= 0:
        issues.append(f"document tokens is {tokens}, expected > 0")

    return issues, warnings


def main() -> None:
    load_dotenv_if_exists()
    args = parse_args()

    file_path = Path(args.file).expanduser().resolve()
    if not file_path.is_file():
        fail(f"Upload file not found: {file_path}")

    base_url = require_setting(
        args.api_base_url,
        env_name="DIFY_API_BASE_URL",
        cli_flag="--api-base-url",
        fallback="https://api.dify.ai/v1" if args.dry_run else None,
    )
    dataset_id = require_setting(
        args.dataset_id,
        env_name="DIFY_DATASET_ID",
        cli_flag="--dataset-id",
        fallback="dataset-id" if args.dry_run else None,
    )
    api_key = require_setting(
        args.api_key,
        env_name="DIFY_API_KEY",
        cli_flag="--api-key",
        fallback="dry-run-api-key" if args.dry_run else None,
    )

    if args.metadata_page_size <= 0:
        fail("--metadata-page-size must be greater than 0")

    poll_interval_seconds = resolve_float_setting(
        args.poll_interval_seconds,
        env_name="DIFY_POLL_INTERVAL_SECONDS",
        flag_name="--poll-interval-seconds",
        fallback=2.0,
    )
    poll_timeout_seconds = resolve_float_setting(
        args.poll_timeout_seconds,
        env_name="DIFY_POLL_TIMEOUT_SECONDS",
        flag_name="--poll-timeout-seconds",
        fallback=300.0,
    )

    inputs_json_path = args.inputs_json or args.data_json
    pipeline_inputs = default_pipeline_inputs()
    if inputs_json_path:
        loaded_inputs = load_json_file(inputs_json_path)
        if not isinstance(loaded_inputs, dict):
            fail("pipeline inputs JSON must be an object")
        pipeline_inputs = loaded_inputs

    metadata_requested: list[dict[str, Any]] = []
    if args.metadata_json:
        metadata_requested = normalize_metadata_input(load_json_file(args.metadata_json))

    pipeline_start_node_id = optional_setting(
        args.pipeline_start_node_id,
        env_name="DIFY_PIPELINE_START_NODE_ID",
    )
    pipeline_datasource_type = optional_setting(
        args.pipeline_datasource_type,
        env_name="DIFY_PIPELINE_DATASOURCE_TYPE",
    )
    response_mode = require_setting(
        args.response_mode,
        env_name="DIFY_PIPELINE_RESPONSE_MODE",
        cli_flag="--response-mode",
        fallback="blocking",
    )

    discovery_endpoint = (
        f"{api_base(base_url)}/datasets/{dataset_id}/pipeline/datasource-plugins?is_published=true"
    )

    if args.dry_run:
        print_json(
            {
                "dry_run": True,
                "endpoints": {
                    "datasource_discovery": discovery_endpoint,
                    "document_detail": f"{api_base(base_url)}/datasets/{dataset_id}/documents/{{document_id}}?metadata=without",
                    "file_upload": f"{api_base(base_url)}/datasets/pipeline/file-upload",
                    "indexing_status": f"{api_base(base_url)}/datasets/{dataset_id}/documents/{{batch}}/indexing-status",
                    "metadata_list": f"{api_base(base_url)}/datasets/{dataset_id}/metadata",
                    "metadata_update": f"{api_base(base_url)}/datasets/{dataset_id}/documents/metadata",
                    "pipeline_run": f"{api_base(base_url)}/datasets/{dataset_id}/pipeline/run",
                },
                "file": {
                    "name": file_path.name,
                    "path": str(file_path),
                    "size_bytes": file_path.stat().st_size,
                },
                "metadata_requested": metadata_requested,
                "pipeline_config": {
                    "datasource_discovery": "runtime",
                    "requested_datasource_type": pipeline_datasource_type,
                    "requested_start_node_id": pipeline_start_node_id,
                    "response_mode": response_mode,
                },
                "pipeline_inputs": pipeline_inputs,
                "poll_interval_seconds": poll_interval_seconds,
                "poll_timeout_seconds": poll_timeout_seconds,
            }
        )
        return

    published_pipeline, discovery_response = discover_datasource_plugin(
        base_url=base_url,
        dataset_id=dataset_id,
        api_key=api_key,
        requested_start_node_id=pipeline_start_node_id,
        requested_datasource_type=pipeline_datasource_type,
    )
    datasource_type = str(published_pipeline["datasource_type"])
    start_node_id = str(published_pipeline["start_node_id"])

    if datasource_type != "local_file":
        fail(
            "This skill only supports published pipeline datasource_type=local_file. "
            f"Discovered: {datasource_type!r}"
        )

    file_upload_response = upload_pipeline_file(
        base_url=base_url,
        api_key=api_key,
        file_path=file_path,
    )
    file_id = coerce_nonempty_str(file_upload_response.get("id"), field_name="file_upload.id")
    file_name = coerce_nonempty_str(
        file_upload_response.get("name") or file_path.name,
        field_name="file_upload.name",
    )

    pipeline_run_response, pipeline_run_request = run_pipeline(
        base_url=base_url,
        dataset_id=dataset_id,
        api_key=api_key,
        inputs=pipeline_inputs,
        datasource_type=datasource_type,
        file_id=file_id,
        file_name=file_name,
        start_node_id=start_node_id,
        response_mode=response_mode,
    )
    batch, document_id = extract_batch_and_document_id(pipeline_run_response)

    indexing_status_response, indexing_timed_out = poll_indexing_status(
        base_url=base_url,
        dataset_id=dataset_id,
        api_key=api_key,
        batch=batch,
        interval_seconds=poll_interval_seconds,
        timeout_seconds=poll_timeout_seconds,
    )

    metadata_response: dict[str, Any] | None = None
    metadata_applied: list[dict[str, Any]] = []
    if metadata_requested:
        available_fields = list_metadata_fields(
            base_url=base_url,
            dataset_id=dataset_id,
            api_key=api_key,
            page_size=args.metadata_page_size,
        )
        metadata_applied = resolve_metadata_items(
            requested_items=metadata_requested,
            available_fields=available_fields,
        )
        metadata_response = write_document_metadata(
            base_url=base_url,
            dataset_id=dataset_id,
            api_key=api_key,
            document_id=document_id,
            metadata_items=metadata_applied,
        )

    document_response = get_document_details(
        base_url=base_url,
        dataset_id=dataset_id,
        api_key=api_key,
        document_id=document_id,
    )

    validation_issues, validation_warnings = build_validation_issues(
        indexing_status_response=indexing_status_response,
        indexing_timed_out=indexing_timed_out,
        timeout_seconds=poll_timeout_seconds,
        document_response=document_response,
    )

    print_json(
        {
            "batch": batch,
            "dataset_id": dataset_id,
            "discovery_response": discovery_response,
            "document_id": document_id,
            "document_response": document_response,
            "file_upload_response": file_upload_response,
            "indexing_status_response": indexing_status_response,
            "metadata_applied": metadata_applied,
            "metadata_response": metadata_response,
            "pipeline_config": {
                "datasource_type": datasource_type,
                "response_mode": response_mode,
                "start_node_id": start_node_id,
            },
            "pipeline_run_request": pipeline_run_request,
            "pipeline_run_response": pipeline_run_response,
            "validation": {
                "issues": validation_issues,
                "ok": not validation_issues,
                "warnings": validation_warnings,
            },
        }
    )

    if validation_issues:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
