#!/usr/bin/env python3
"""Upload a local file to a Dify dataset and optionally write document metadata."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, NoReturn


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Upload a file to an existing Dify dataset and optionally update "
            "document metadata using existing dataset metadata fields."
        )
    )
    parser.add_argument("--file", required=True, help="Local file path to upload")
    parser.add_argument(
        "--data-json",
        help="Optional JSON file for the upload 'data' form field",
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


def load_json_file(path_str: str) -> Any:
    path = Path(path_str).expanduser().resolve()
    if not path.is_file():
        fail(f"JSON file not found: {path}")
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except json.JSONDecodeError as exc:
        fail(f"Invalid JSON in {path}: {exc}")


def default_upload_data() -> dict[str, Any]:
    return {
        "indexing_technique": "high_quality",
        "process_rule": {
            "mode": "automatic",
        },
    }


def normalize_metadata_input(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, dict):
        items: list[dict[str, Any]] = []
        for name, value in raw.items():
            items.append({"name": name, "value": value})
        return items

    if isinstance(raw, list):
        items = []
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


def api_base(base_url: str) -> str:
    return base_url.rstrip("/")


def upload_document(
    *,
    base_url: str,
    dataset_id: str,
    api_key: str,
    file_path: Path,
    upload_data: dict[str, Any],
) -> dict[str, Any]:
    endpoint = f"{api_base(base_url)}/datasets/{dataset_id}/document/create-by-file"
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
        f"data={json.dumps(upload_data, ensure_ascii=False)}",
        "--form",
        f"file=@{file_path}",
    ]
    response = run_curl_json(command)
    if not isinstance(response, dict):
        fail("Upload response must be a JSON object")
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


def print_json(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> None:
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

    upload_data = default_upload_data()
    if args.data_json:
        loaded_data = load_json_file(args.data_json)
        if not isinstance(loaded_data, dict):
            fail("upload data JSON must be an object")
        upload_data = loaded_data

    metadata_requested: list[dict[str, Any]] = []
    if args.metadata_json:
        metadata_requested = normalize_metadata_input(load_json_file(args.metadata_json))

    if args.dry_run:
        print_json(
            {
                "dry_run": True,
                "endpoints": {
                    "upload": f"{api_base(base_url)}/datasets/{dataset_id}/document/create-by-file",
                    "metadata_list": f"{api_base(base_url)}/datasets/{dataset_id}/metadata",
                    "metadata_update": f"{api_base(base_url)}/datasets/{dataset_id}/documents/metadata",
                },
                "file": {
                    "name": file_path.name,
                    "path": str(file_path),
                    "size_bytes": file_path.stat().st_size,
                },
                "upload_data": upload_data,
                "metadata_requested": metadata_requested,
            }
        )
        return

    upload_response = upload_document(
        base_url=base_url,
        dataset_id=dataset_id,
        api_key=api_key,
        file_path=file_path,
        upload_data=upload_data,
    )

    document = upload_response.get("document")
    if not isinstance(document, dict):
        fail("Upload response is missing 'document' object")

    document_id = document.get("id")
    if not isinstance(document_id, str) or not document_id:
        fail("Upload response is missing document.id")

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

    print_json(
        {
            "dataset_id": dataset_id,
            "document_id": document_id,
            "metadata_applied": metadata_applied,
            "metadata_response": metadata_response,
            "upload_response": upload_response,
        }
    )


if __name__ == "__main__":
    main()
