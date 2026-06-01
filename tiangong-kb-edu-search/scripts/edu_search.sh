#!/bin/bash
# Edu-source search wrapper over Tiangong AI CLI.
# Usage: ./edu_search.sh '{"query": "topic", ...}' [output_file]

set -euo pipefail

JSON_INPUT="${1:-}"
OUTPUT_FILE="${2:-}"
CLI_COMMAND=()
if [ -n "${TIANGONG_AI_CLI:-}" ]; then
    read -r -a CLI_COMMAND <<< "$TIANGONG_AI_CLI"
elif [ -n "${TIANGONG_AI_CLI_BIN:-}" ]; then
    CLI_COMMAND=("$TIANGONG_AI_CLI_BIN")
else
    CLI_COMMAND=(npx @tiangong-ai/cli@latest)
fi

if [ -z "$JSON_INPUT" ]; then
    echo "Usage: ./edu_search.sh '<json>' [output_file]" >&2
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

load_env_near_file() {
    local input_file="$1"
    local dir
    dir="$(cd "$(dirname "$input_file")" 2>/dev/null && pwd || true)"
    [ -n "$dir" ] && load_env_file "$dir/.env"
}

SOURCE_INPUT=$(echo "$JSON_INPUT" | jq -r '
    if (.sources | type) == "array" then
        [.sources[]] | join(",")
    else
        .sources // "default"
    end
')

case "$SOURCE_INPUT" in
    ""|"default"|"edu")
        ;;
    *)
        echo "Error: tiangong-kb-edu-search searches only the edu source; use the course or textbook skill for other sources" >&2
        exit 2
        ;;
esac

REQUEST_FILE=$(echo "$JSON_INPUT" | jq -r '.request_file // .input_file // empty')
QUERY=$(echo "$JSON_INPUT" | jq -r '.query // .input // empty')
TMP_REQUEST_FILE=""
ENV_FILE=$(echo "$JSON_INPUT" | jq -r '.env_file // empty')

if [ -n "$ENV_FILE" ]; then
    load_env_file "$ENV_FILE"
elif [ -n "$REQUEST_FILE" ]; then
    load_env_near_file "$REQUEST_FILE"
fi

cleanup() {
    if [ -n "$TMP_REQUEST_FILE" ]; then
        rm -f "$TMP_REQUEST_FILE"
    fi
}
trap cleanup EXIT

if [ -z "$REQUEST_FILE" ]; then
    if echo "$JSON_INPUT" | jq -e 'has("datefilter") or has("getMeta") or has("get_meta")' >/dev/null; then
        echo "Error: inline datefilter/getMeta fields are not supported for tiangong-kb-edu-search" >&2
        exit 2
    fi
    if echo "$JSON_INPUT" | jq -e 'has("filter") or has("action") or has("fields") or has("topK") or has("extK")' >/dev/null; then
        if ! echo "$JSON_INPUT" | jq -e 'has("action")' >/dev/null && [ -z "$QUERY" ]; then
            echo "Error: 'query' or 'input' field is required when using inline search payload fields" >&2
            exit 1
        fi
        TMP_REQUEST_FILE=$(mktemp "${TMPDIR:-/tmp}/tiangong-kb-edu.XXXXXX.json")
        echo "$JSON_INPUT" | jq '
        (if has("action") then {action: .action} else {query: (.query // .input)} end)
        + (if has("filter") then {filter: .filter} else {} end)
        + (if has("fields") then {fields: .fields} else {} end)
        + (if has("topK") then {topK: .topK} elif has("top_k") then {topK: .top_k} else {} end)
        + (if has("extK") then {extK: .extK} elif has("ext_k") then {extK: .ext_k} else {} end)' > "$TMP_REQUEST_FILE"
        REQUEST_FILE="$TMP_REQUEST_FILE"
    fi
fi

ARGS=(education search --sources edu --json)

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
value_arg "edu_api_key" "--edu-api-key"
value_arg "edu_url" "--edu-url"
value_arg "region" "--region"
value_arg "timeout" "--timeout"

if [ -z "$REQUEST_FILE" ]; then
    value_arg "top_k" "--top-k"
    value_arg "ext_k" "--ext-k"
fi

if jq_bool "dry_run"; then
    ARGS+=(--dry-run)
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
        echo "Results saved to: $OUTPUT_FILE"
    else
        STATUS=$?
        cat "$TMP_OUTPUT" >&2 || true
        rm -f "$TMP_OUTPUT"
        exit "$STATUS"
    fi
else
    run_cli
fi
