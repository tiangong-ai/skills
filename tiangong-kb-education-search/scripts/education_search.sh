#!/bin/bash
# Education search wrapper over Tiangong AI CLI.
# Usage: ./education_search.sh '{"query": "topic", ...}' [output_file]

set -euo pipefail

JSON_INPUT="${1:-}"
OUTPUT_FILE="${2:-}"
CLI="${TIANGONG_AI_CLI:-tiangong-ai}"

if [ -z "$JSON_INPUT" ]; then
    echo "Usage: ./education_search.sh '<json>' [output_file]"
    echo ""
    echo "Input fields:"
    echo "  query or input: convenience query text"
    echo "  request_file or input_file: JSON request body to forward unchanged"
    echo "  sources: array, comma-separated string, or preset: default, all"
    echo "  dry_run: true/false"
    exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
    echo "Error: jq is required"
    exit 1
fi

if ! echo "$JSON_INPUT" | jq empty 2>/dev/null; then
    echo "Error: Invalid JSON input"
    exit 1
fi

write_output() {
    if [ -n "$OUTPUT_FILE" ]; then
        cat > "$OUTPUT_FILE"
        echo "Results saved to: $OUTPUT_FILE"
    else
        cat
    fi
}

jq_value() {
    local key="$1"
    echo "$JSON_INPUT" | jq -r ".${key} // empty"
}

jq_bool() {
    local key="$1"
    [ "$(echo "$JSON_INPUT" | jq -r ".${key} // false")" = "true" ]
}

SOURCES=$(echo "$JSON_INPUT" | jq -r '
    if (.sources | type) == "array" then
        [.sources[]] | join(",")
    else
        .sources // "default"
    end
')

REQUEST_FILE=$(echo "$JSON_INPUT" | jq -r '.request_file // .input_file // empty')
QUERY=$(echo "$JSON_INPUT" | jq -r '.query // .input // empty')

ARGS=(education search --sources "$SOURCES" --json)

if [ -n "$REQUEST_FILE" ]; then
    ARGS+=(--input "$REQUEST_FILE")
else
    if [ -z "$QUERY" ]; then
        echo "Error: 'query', 'input', 'request_file', or 'input_file' field is required"
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
value_arg "edu_api_key" "--edu-api-key"
value_arg "textbook_api_key" "--textbook-api-key"
value_arg "course_url" "--course-url"
value_arg "edu_url" "--edu-url"
value_arg "textbook_url" "--textbook-url"
value_arg "region" "--region"
value_arg "timeout" "--timeout"

if [ -z "$REQUEST_FILE" ]; then
    value_arg "top_k" "--top-k"
    value_arg "ext_k" "--ext-k"
fi

if jq_bool "dry_run"; then
    ARGS+=(--dry-run)
fi

"$CLI" "${ARGS[@]}" | write_output
