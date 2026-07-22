#!/bin/bash
# ESG-source search wrapper over Tiangong AI CLI.
# Usage: ./esg_search.sh '{"query": "topic", ...}' [output_file]

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
    echo "Usage: ./esg_search.sh '<json>' [output_file]" >&2
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
    ""|"default"|"esg")
        ;;
    *)
        echo "Error: tiangong-kb-esg-search searches only the ESG source" >&2
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
    if ! echo "$JSON_INPUT" | jq -e '
        def string_array:
            type == "array" and all(.[]; type == "string");
        def esg_filter:
            type == "object"
            and all(to_entries[]; .value | string_array);
        def date_range:
            type == "object"
            and ((keys_unsorted - ["gte", "lte"]) | length == 0)
            and all(to_entries[]; .value | type == "number");
        def esg_datefilter:
            type == "object"
            and all(to_entries[]; .value | date_range);
        def positive_integer:
            type == "number" and floor == . and . > 0;
        def nonnegative_integer:
            type == "number" and floor == . and . >= 0;

        ((has("filter") | not) or (.filter | esg_filter))
        and ((has("datefilter") and has("dateFilter")) | not)
        and (if has("datefilter") then .datefilter | esg_datefilter
             elif has("dateFilter") then .dateFilter | esg_datefilter
             else true end)
        and ((has("meta_contains") and has("metaContains")) | not)
        and (if has("meta_contains") then (.meta_contains | type == "string")
             elif has("metaContains") then (.metaContains | type == "string")
             else true end)
        and ((has("topK") | not) or (.topK | positive_integer))
        and ((has("top_k") | not) or (.top_k | positive_integer))
        and ((has("extK") | not) or (.extK | nonnegative_integer))
        and ((has("ext_k") | not) or (.ext_k | nonnegative_integer))
    ' >/dev/null; then
        echo "Error: ESG filters must match edge-function shapes: filter.<field> as string arrays, datefilter.<field>.gte/lte as numbers, and meta_contains as a string" >&2
        exit 2
    fi
    if echo "$JSON_INPUT" | jq -e 'has("action") or has("fields") or has("getMeta") or has("get_meta")' >/dev/null; then
        echo "Error: inline action/fields/getMeta fields are not supported for tiangong-kb-esg-search" >&2
        exit 2
    fi
    if echo "$JSON_INPUT" | jq -e 'has("filter") or has("datefilter") or has("dateFilter") or has("meta_contains") or has("metaContains") or has("topK") or has("extK")' >/dev/null; then
        if [ -z "$QUERY" ]; then
            echo "Error: 'query' or 'input' field is required when using inline search payload fields" >&2
            exit 1
        fi
        TMP_REQUEST_FILE=$(mktemp "${TMPDIR:-/tmp}/tiangong-kb-esg.XXXXXX")
        echo "$JSON_INPUT" | jq '
        {query: (.query // .input)}
        + (if has("filter") then {filter: .filter} else {} end)
        + (if has("datefilter") then {datefilter: .datefilter} elif has("dateFilter") then {datefilter: .dateFilter} else {} end)
        + (if has("meta_contains") then {meta_contains: .meta_contains} elif has("metaContains") then {meta_contains: .metaContains} else {} end)
        + (if has("topK") then {topK: .topK} elif has("top_k") then {topK: .top_k} else {} end)
        + (if has("extK") then {extK: .extK} elif has("ext_k") then {extK: .ext_k} else {} end)' > "$TMP_REQUEST_FILE"
        REQUEST_FILE="$TMP_REQUEST_FILE"
    fi
fi

ARGS=(research search --sources esg --json)

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

ESG_API_KEY=$(jq_value "esg_api_key")
if [ -n "$ESG_API_KEY" ]; then
    ARGS+=(--esg-api-key "$ESG_API_KEY")
else
    API_KEY=$(jq_value "api_key")
    if [ -n "$API_KEY" ]; then
        ARGS+=(--api-key "$API_KEY")
    fi
fi

ESG_URL=$(jq_value "esg_url")
if [ -n "$ESG_URL" ]; then
    ARGS+=(--esg-url "$ESG_URL")
fi

API_BASE_URL=$(jq_value "api_base_url")
API_BASE_URL="${API_BASE_URL:-${TIANGONG_ESG_API_BASE_URL:-}}"
if [ -n "$API_BASE_URL" ]; then
    ARGS+=(--api-base-url "$API_BASE_URL")
fi

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
