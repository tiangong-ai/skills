#!/bin/bash
# Course fulltext wrapper over Tiangong AI CLI.
# Usage: ./course_fulltext_fetch.sh '{"document_id": "...", "tags": "..."}' [output_file]

set -euo pipefail

JSON_INPUT="${1:-}"
OUTPUT_FILE_ARG="${2:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI_COMMAND=()

if [ -n "${TIANGONG_AI_CLI:-}" ]; then
    read -r -a CLI_COMMAND <<< "$TIANGONG_AI_CLI"
elif [ -n "${TIANGONG_AI_CLI_BIN:-}" ]; then
    CLI_COMMAND=("$TIANGONG_AI_CLI_BIN")
else
    CLI_COMMAND=(npx @tiangong-ai/cli@latest)
fi

if [ -z "$JSON_INPUT" ]; then
    echo "Usage: ./course_fulltext_fetch.sh '<json>' [output_file]" >&2
    echo "" >&2
    echo "Input fields:" >&2
    echo "  document_id or documentId: required course document id" >&2
    echo "  tags or tag: required course tag, for example thu_humanities" >&2
    echo "  output_file: optional output path; second argument takes precedence" >&2
    echo "  json: true/false" >&2
    echo "  bucket, prefix, region: optional CLI overrides" >&2
    echo "  env_file: optional dotenv file to load before calling the CLI" >&2
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is required" >&2
    exit 1
fi

if ! echo "$JSON_INPUT" | jq empty 2>/dev/null; then
    echo "Error: Invalid JSON input" >&2
    exit 1
fi

jq_value() {
    local expression="$1"
    echo "$JSON_INPUT" | jq -r "$expression"
}

jq_bool() {
    local key="$1"
    [ "$(echo "$JSON_INPUT" | jq -r ".${key} // false")" = "true" ]
}

load_env_file() {
    local env_file="$1"
    [ -f "$env_file" ] || return 0
    while IFS= read -r line || [ -n "$line" ]; do
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"
        [[ -z "$line" || "$line" == \#* || "$line" != *=* ]] && continue
        line="${line#export }"
        local key="${line%%=*}"
        local value="${line#*=}"
        key="${key#"${key%%[![:space:]]*}"}"
        key="${key%"${key##*[![:space:]]}"}"
        value="${value#"${value%%[![:space:]]*}"}"
        value="${value%"${value##*[![:space:]]}"}"
        [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || continue
        [ -n "${!key+x}" ] && continue
        value="${value%\"}"
        value="${value#\"}"
        value="${value%\'}"
        value="${value#\'}"
        export "$key=$value"
    done < "$env_file"
}

DOCUMENT_ID=$(jq_value '.document_id // .documentId // empty')
TAGS=$(jq_value '(.tags // .tag // empty) | if type == "array" then (.[0] // "") else . end')
OUTPUT_FILE="${OUTPUT_FILE_ARG:-$(jq_value '.output_file // empty')}"
ENV_FILE=$(jq_value '.env_file // empty')

if [ -n "$ENV_FILE" ]; then
    load_env_file "$ENV_FILE"
fi

if [ -z "$DOCUMENT_ID" ]; then
    echo "Error: document_id or documentId is required" >&2
    exit 1
fi

if [ -z "$TAGS" ]; then
    echo "Error: tags or tag is required" >&2
    exit 1
fi

ARGS=(kb course fulltext --document-id "$DOCUMENT_ID" --tags "$TAGS")

value_arg() {
    local json_key="$1"
    local cli_flag="$2"
    local value
    value=$(jq_value ".${json_key} // empty")
    if [ -n "$value" ]; then
        ARGS+=("$cli_flag" "$value")
    fi
}

value_arg "bucket" "--bucket"
value_arg "prefix" "--prefix"
value_arg "region" "--region"

if jq_bool "json"; then
    ARGS+=(--json)
fi

run_cli() {
    if [[ "${CLI_COMMAND[0]}" == *.js ]]; then
        node "${CLI_COMMAND[@]}" "${ARGS[@]}"
    else
        "${CLI_COMMAND[@]}" "${ARGS[@]}"
    fi
}

if [ -n "$OUTPUT_FILE" ]; then
    TMP_OUTPUT="${OUTPUT_FILE}.tmp.$$"
    if run_cli > "$TMP_OUTPUT"; then
        mv "$TMP_OUTPUT" "$OUTPUT_FILE"
        echo "Full text saved to: $OUTPUT_FILE"
    else
        STATUS=$?
        cat "$TMP_OUTPUT" >&2 || true
        rm -f "$TMP_OUTPUT"
        exit "$STATUS"
    fi
else
    run_cli
fi
