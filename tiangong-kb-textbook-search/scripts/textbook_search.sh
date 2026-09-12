#!/bin/bash
# Textbook-source search wrapper over Tiangong AI CLI.
# Usage: ./textbook_search.sh '{"query": "topic", ...}' [output_file]

set -euo pipefail
umask 077

JSON_INPUT="${1:-}"
OUTPUT_FILE="${2:-}"
STANDALONE_TESTED_CLI_VERSION="0.0.62"
CLI_COMMAND=()
if [ -n "${TIANGONG_AI_CLI:-}" ]; then
    read -r -a CLI_COMMAND <<< "$TIANGONG_AI_CLI"
elif [ -n "${TIANGONG_AI_CLI_BIN:-}" ]; then
    CLI_COMMAND=("$TIANGONG_AI_CLI_BIN")
else
    CLI_COMMAND=(npx --yes --package "@tiangong-ai/cli@$STANDALONE_TESTED_CLI_VERSION" -- tiangong-ai)
fi

if [ -z "$JSON_INPUT" ]; then
    echo "Usage: ./textbook_search.sh '<json>' [output_file]" >&2
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

contains_sensitive_json() {
    jq -e '[.. | objects | keys[]] | any(.[]; test("(^|[_-])(access[_-]?token|api[_-]?key|apikey|auth|authorization|cookie|credential|password|secret|session|token)([_-]|$)"; "i"))' >/dev/null
}

if echo "$JSON_INPUT" | contains_sensitive_json; then
    echo "Error: credentials are not accepted in wrapper JSON; use owner environment variables" >&2
    exit 2
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
    local required="${2:-false}"
    if [ ! -e "$env_file" ]; then
        if [ "$required" = "true" ]; then
            echo "Error: explicit env_file does not exist" >&2
            exit 2
        fi
        return 0
    fi
    if [ -L "$env_file" ] || [ ! -f "$env_file" ]; then
        echo "Error: env_file must be a regular non-symlink file" >&2
        exit 2
    fi
    local mode=""
    if stat -f '%Lp' "$env_file" >/dev/null 2>&1; then mode="$(stat -f '%Lp' "$env_file")";
    elif stat -c '%a' "$env_file" >/dev/null 2>&1; then mode="$(stat -c '%a' "$env_file")"; fi
    if [ -n "$mode" ] && [ $((10#$mode % 100)) -ne 0 ]; then
        echo "Error: env_file must be owner-only (chmod 600)" >&2
        exit 2
    fi
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
        case "$key" in
            TIANGONG_AI_APIKEY|TIANGONG_TEXTBOOK_APIKEY|TIANGONG_EDUCATION_API_BASE_URL|TIANGONG_AI_SEARCH_API_BASE_URL|TIANGONG_AI_API_BASE_URL|TIANGONG_TEXTBOOK_SEARCH_URL|TIANGONG_REGION|TIANGONG_EDUCATION_TIMEOUT)
                ;;
            *) continue ;;
        esac
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
    ""|"default"|"textbook")
        ;;
    *)
        echo "Error: tiangong-kb-textbook-search searches only the textbook source; use the course or edu skill for other sources" >&2
        exit 2
        ;;
esac

REQUEST_FILE=$(echo "$JSON_INPUT" | jq -r '.request_file // .input_file // empty')
QUERY=$(echo "$JSON_INPUT" | jq -r '.query // .input // empty')
TMP_REQUEST_FILE=""
ENV_FILE=$(echo "$JSON_INPUT" | jq -r '.env_file // empty')

if [ -n "$REQUEST_FILE" ]; then
    if [ -L "$REQUEST_FILE" ] || [ ! -f "$REQUEST_FILE" ]; then
        echo "Error: request_file must be a regular non-symlink JSON file" >&2
        exit 2
    fi
    if ! jq empty "$REQUEST_FILE" 2>/dev/null; then
        echo "Error: request_file is not valid JSON" >&2
        exit 2
    fi
    if contains_sensitive_json < "$REQUEST_FILE"; then
        echo "Error: request_file contains credential-like fields; use owner environment variables" >&2
        exit 2
    fi
fi

if [ -n "$ENV_FILE" ]; then
    load_env_file "$ENV_FILE" true
elif [ -n "$REQUEST_FILE" ]; then
    load_env_near_file "$REQUEST_FILE"
fi

cleanup() {
    if [ -n "$TMP_REQUEST_FILE" ]; then
        rm -f "$TMP_REQUEST_FILE"
    fi
    if [ -n "${TMP_OUTPUT:-}" ]; then rm -f "$TMP_OUTPUT"; fi
}
trap cleanup EXIT

if [ -z "$REQUEST_FILE" ]; then
    if echo "$JSON_INPUT" | jq -e 'has("getMeta") or has("get_meta")' >/dev/null; then
        echo "Error: inline getMeta/get_meta is not supported for tiangong-kb-textbook-search" >&2
        exit 2
    fi
    if echo "$JSON_INPUT" | jq -e 'has("filter") or has("datefilter") or has("topK") or has("extK")' >/dev/null; then
        if [ -z "$QUERY" ]; then
            echo "Error: 'query' or 'input' field is required when using inline raw payload fields" >&2
            exit 1
        fi
        TMP_REQUEST_FILE=$(mktemp "${TMPDIR:-/tmp}/tiangong-kb-textbook.XXXXXX.json")
        echo "$JSON_INPUT" | jq '{
            query: (.query // .input)
        }
        + (if has("filter") then {filter: .filter} else {} end)
        + (if has("datefilter") then {datefilter: .datefilter} else {} end)
        + (if has("topK") then {topK: .topK} elif has("top_k") then {topK: .top_k} else {} end)
        + (if has("extK") then {extK: .extK} elif has("ext_k") then {extK: .ext_k} else {} end)' > "$TMP_REQUEST_FILE"
        REQUEST_FILE="$TMP_REQUEST_FILE"
    fi
fi

ARGS=(education search --sources textbook --json)

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

safe_url_arg() {
    local json_key="$1"
    local cli_flag="$2"
    local value
    value=$(jq_value "$json_key")
    if [ -z "$value" ]; then return 0; fi
    case "$value" in https://*) ;; *) echo "Error: $json_key must be an HTTPS URL" >&2; exit 2 ;; esac
    case "$value" in *\?*|*\#*|*@*) echo "Error: $json_key must not contain userinfo, query parameters, or fragments" >&2; exit 2 ;; esac
    ARGS+=("$cli_flag" "$value")
}

safe_url_arg "api_base_url" "--api-base-url"
safe_url_arg "textbook_url" "--textbook-url"
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
        DO_NOT_TRACK=1 node "${CLI_COMMAND[@]}" "${ARGS[@]}"
    else
        DO_NOT_TRACK=1 "${CLI_COMMAND[@]}" "${ARGS[@]}"
    fi
}

if [ -n "$OUTPUT_FILE" ]; then
    if [ -L "$OUTPUT_FILE" ]; then echo "Error: output_file must not be a symbolic link" >&2; exit 2; fi
    TMP_OUTPUT=$(mktemp "${OUTPUT_FILE}.tmp.XXXXXX")
    if run_cli > "$TMP_OUTPUT"; then
        mv "$TMP_OUTPUT" "$OUTPUT_FILE"
        TMP_OUTPUT=""
        echo "Results saved to: $OUTPUT_FILE"
    else
        status=$?
        echo "Error: Tiangong CLI search failed; see its sanitized stderr diagnostic" >&2
        exit "$status"
    fi
else
    run_cli
fi
