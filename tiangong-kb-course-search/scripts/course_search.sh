#!/bin/bash
# Course-source search wrapper over Tiangong AI CLI.
# Usage: ./course_search.sh '{"query": "topic", ...}' [output_file]

set -euo pipefail

JSON_INPUT="${1:-}"
OUTPUT_FILE="${2:-}"
CLI="${TIANGONG_AI_CLI:-tiangong-ai}"

if [ -z "$JSON_INPUT" ]; then
    echo "Usage: ./course_search.sh '<json>' [output_file]" >&2
    echo "" >&2
    echo "Input fields:" >&2
    echo "  query or input: convenience query text" >&2
    echo "  request_file or input_file: JSON request body to forward unchanged" >&2
    echo "  sources: optional compatibility field; only course/default is accepted" >&2
    echo "  dry_run: true/false" >&2
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
    local key="$1"
    echo "$JSON_INPUT" | jq -r ".${key} // empty"
}

jq_bool() {
    local key="$1"
    [ "$(echo "$JSON_INPUT" | jq -r ".${key} // false")" = "true" ]
}

SOURCE_INPUT=$(echo "$JSON_INPUT" | jq -r '
    if (.sources | type) == "array" then
        [.sources[]] | join(",")
    else
        .sources // "default"
    end
')

case "$SOURCE_INPUT" in
    ""|"default"|"course")
        ;;
    *)
        echo "Error: tiangong-kb-course-search searches only the course source; use the edu or textbook skill for other sources" >&2
        exit 2
        ;;
esac

REQUEST_FILE=$(echo "$JSON_INPUT" | jq -r '.request_file // .input_file // empty')
QUERY=$(echo "$JSON_INPUT" | jq -r '.query // .input // empty')
TMP_REQUEST_FILE=""

cleanup() {
    if [ -n "$TMP_REQUEST_FILE" ]; then
        rm -f "$TMP_REQUEST_FILE"
    fi
}
trap cleanup EXIT

if [ -z "$REQUEST_FILE" ]; then
    if echo "$JSON_INPUT" | jq -e 'has("datefilter")' >/dev/null; then
        echo "Error: inline datefilter is not supported for tiangong-kb-course-search; use indexed metadata filters such as tags/raw_relative_path" >&2
        exit 2
    fi
    if echo "$JSON_INPUT" | jq -e 'has("filter") or has("topK") or has("extK")' >/dev/null; then
        if [ -z "$QUERY" ]; then
            echo "Error: 'query' or 'input' field is required when using inline raw payload fields" >&2
            exit 1
        fi
        TMP_REQUEST_FILE=$(mktemp "${TMPDIR:-/tmp}/tiangong-kb-course.XXXXXX.json")
        echo "$JSON_INPUT" | jq '{
            query: (.query // .input)
        }
        + (if has("filter") then {filter: .filter} else {} end)
        + (if has("topK") then {topK: .topK} elif has("top_k") then {topK: .top_k} else {} end)
        + (if has("extK") then {extK: .extK} elif has("ext_k") then {extK: .ext_k} else {} end)' > "$TMP_REQUEST_FILE"
        REQUEST_FILE="$TMP_REQUEST_FILE"
    fi
fi

ARGS=(education search --sources course --json)

if [ -n "$REQUEST_FILE" ]; then
    ARGS+=(--input "$REQUEST_FILE")
else
    if [ -z "$QUERY" ]; then
        echo "Error: 'query', 'input', 'request_file', or 'input_file' field is required" >&2
        exit 1
    fi
    ARGS+=(--query "$QUERY")
fi

value_arg() {
    local json_key="$1"
    local cli_flag="$2"
    local value
    value=$(jq_value "$json_key")
    if [ -n "$value" ]; then
        ARGS+=("$cli_flag" "$value")
    fi
}

value_arg "api_base_url" "--api-base-url"
value_arg "api_key" "--api-key"
value_arg "bearer_token" "--bearer-token"
value_arg "course_api_key" "--course-api-key"
value_arg "course_url" "--course-url"
value_arg "region" "--region"
value_arg "timeout" "--timeout"

if [ -z "$(jq_value "bearer_token")" ] && [ -n "$(jq_value "api_key")" ]; then
    ARGS+=(--bearer-token "$(jq_value "api_key")")
fi

if [ -z "$REQUEST_FILE" ]; then
    value_arg "top_k" "--top-k"
    value_arg "ext_k" "--ext-k"
fi

if jq_bool "dry_run"; then
    ARGS+=(--dry-run)
fi

if [ -n "$OUTPUT_FILE" ]; then
    TMP_OUTPUT="${OUTPUT_FILE}.tmp.$$"
    if "$CLI" "${ARGS[@]}" > "$TMP_OUTPUT"; then
        mv "$TMP_OUTPUT" "$OUTPUT_FILE"
        echo "Results saved to: $OUTPUT_FILE"
    else
        STATUS=$?
        cat "$TMP_OUTPUT" >&2 || true
        rm -f "$TMP_OUTPUT"
        exit "$STATUS"
    fi
else
    "$CLI" "${ARGS[@]}"
fi
