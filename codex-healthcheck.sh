#!/usr/bin/env bash

set -uo pipefail
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="${LOG_FILE:-$SCRIPT_DIR/codex-healthcheck.log}"
PROMPTS_FILE="${PROMPTS_FILE:-$SCRIPT_DIR/prompts.txt}"
MIN_INTERVAL_SEC="${MIN_INTERVAL_SEC:-2700}"
MAX_INTERVAL_SEC="${MAX_INTERVAL_SEC:-3300}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-180}"

RUN_ONCE=false
DRY_RUN=false
CURRENT_RUN_DIR=""
CURRENT_RESPONSE_FILE=""
CURRENT_RAW_FILE=""
CURRENT_COMMAND_PID=""
CURRENT_WATCHDOG_PID=""

usage() {
    cat <<'EOF'
Usage: codex-healthcheck.sh [--once] [--dry-run]

Options:
  --once     Send one request, then exit.
  --dry-run  Create and verify the temporary workspace without calling Codex.
  -h, --help Show this help.

Environment variables:
  MIN_INTERVAL_SEC     Minimum delay between requests (default: 2700).
  MAX_INTERVAL_SEC     Maximum delay between requests (default: 3300).
  REQUEST_TIMEOUT_SEC  Per-request timeout (default: 180).
  LOG_FILE             Status log path.
  PROMPTS_FILE         Prompt list path (one prompt per line).
  CODEX_BIN            Codex executable path.
EOF
}

log() {
    local message="$1"
    printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$message" | tee -a "$LOG_FILE"
}

is_positive_integer() {
    case "$1" in
        ''|*[!0-9]*|0) return 1 ;;
        *) return 0 ;;
    esac
}

cleanup_current_run() {
    if [ -n "$CURRENT_RUN_DIR" ] && [ "${PWD:-}" = "$CURRENT_RUN_DIR" ]; then
        cd /tmp 2>/dev/null || true
    fi

    if [ -n "$CURRENT_RESPONSE_FILE" ]; then
        rm -f -- "$CURRENT_RESPONSE_FILE"
        CURRENT_RESPONSE_FILE=""
    fi

    if [ -n "$CURRENT_RAW_FILE" ]; then
        rm -f -- "$CURRENT_RAW_FILE"
        CURRENT_RAW_FILE=""
    fi

    if [ -n "$CURRENT_RUN_DIR" ] && [ -d "$CURRENT_RUN_DIR" ]; then
        if ! rmdir -- "$CURRENT_RUN_DIR" 2>/dev/null; then
            log "WARNING: temporary workspace was not empty; left in place: $CURRENT_RUN_DIR"
        fi
        CURRENT_RUN_DIR=""
    fi
}

handle_signal() {
    if [ -n "$CURRENT_COMMAND_PID" ]; then
        kill -TERM "$CURRENT_COMMAND_PID" 2>/dev/null || true
    fi
    if [ -n "$CURRENT_WATCHDOG_PID" ]; then
        kill "$CURRENT_WATCHDOG_PID" 2>/dev/null || true
    fi
    log "Stopped by signal."
    cleanup_current_run
    exit 130
}

trap handle_signal INT TERM
trap cleanup_current_run EXIT

run_with_timeout() {
    local timeout_sec="$1"
    shift

    "$@" &
    CURRENT_COMMAND_PID=$!

    (
        sleep "$timeout_sec"
        kill -TERM "$CURRENT_COMMAND_PID" 2>/dev/null || exit 0
        sleep 5
        kill -KILL "$CURRENT_COMMAND_PID" 2>/dev/null || true
    ) &
    CURRENT_WATCHDOG_PID=$!
    local exit_code=0

    wait "$CURRENT_COMMAND_PID" || exit_code=$?
    kill "$CURRENT_WATCHDOG_PID" 2>/dev/null || true
    wait "$CURRENT_WATCHDOG_PID" 2>/dev/null || true
    CURRENT_COMMAND_PID=""
    CURRENT_WATCHDOG_PID=""

    case "$exit_code" in
        137|143) return 124 ;;
        *) return "$exit_code" ;;
    esac
}

random_interval() {
    local span=$((MAX_INTERVAL_SEC - MIN_INTERVAL_SEC + 1))
    printf '%s\n' "$((MIN_INTERVAL_SEC + RANDOM % span))"
}

pick_prompt() {
    local line check
    local prompts=()

    while IFS= read -r line || [ -n "$line" ]; do
        line="${line%$'\r'}"
        check="${line#"${line%%[![:space:]]*}"}"

        [ -z "$check" ] && continue
        case "$check" in
            \#*) continue ;;
        esac

        prompts[${#prompts[@]}]="$line"
    done < "$PROMPTS_FILE"

    if [ "${#prompts[@]}" -eq 0 ]; then
        return 1
    fi

    printf '%s\n' "${prompts[$((RANDOM % ${#prompts[@]}))]}"
}

run_request() {
    local request_id selected_prompt prompt exit_code response_bytes previous_dir

    CURRENT_RUN_DIR=$(mktemp -d /tmp/cxrun.XXXXXX) || return 1
    CURRENT_RESPONSE_FILE=$(mktemp /tmp/cxresponse.XXXXXX) || return 1
    CURRENT_RAW_FILE=$(mktemp /tmp/cxoutput.XXXXXX) || return 1

    if [ -n "$(ls -A "$CURRENT_RUN_DIR" 2>/dev/null)" ]; then
        log "ERROR: temporary workspace is not empty: $CURRENT_RUN_DIR"
        cleanup_current_run
        return 1
    fi

    if ! selected_prompt=$(pick_prompt); then
        log "ERROR: no usable prompts found in $PROMPTS_FILE"
        cleanup_current_run
        return 1
    fi

    request_id="$(date +%s)-$RANDOM"
    prompt="$selected_prompt

Answer concisely in at most 80 words. Do not inspect local files, run commands, browse, or use tools. ID=$request_id"
    log "Prompt: ${selected_prompt:0:100}"

    if [ "$DRY_RUN" = true ]; then
        log "DRY RUN: verified empty workspace $CURRENT_RUN_DIR"
        cleanup_current_run
        return 0
    fi

    local codex_args=(
        exec
        --ephemeral
        --skip-git-repo-check
        --ignore-rules
        --sandbox read-only
        --color never
        -C "$CURRENT_RUN_DIR"
        -c 'model_reasoning_effort="low"'
        -c 'project_doc_max_bytes=0'
        -c 'project_doc_fallback_filenames=[]'
        -c 'history.persistence="none"'
        -c 'features.memories=false'
        -c 'features.skills=false'
        -c 'features.multi_agent=false'
        -c 'features.personality=false'
        -c 'features.shell_snapshot=false'
        -c 'notify=[]'
        -c 'disable_response_storage=true'
        --output-last-message "$CURRENT_RESPONSE_FILE"
        "$prompt"
    )

    previous_dir="$PWD"
    exit_code=0
    if cd "$CURRENT_RUN_DIR"; then
        run_with_timeout "$REQUEST_TIMEOUT_SEC" "$CODEX_BIN" "${codex_args[@]}" \
            >"$CURRENT_RAW_FILE" 2>&1 || exit_code=$?
        cd "$previous_dir" 2>/dev/null || cd "$SCRIPT_DIR" || exit 1
    else
        exit_code=1
    fi

    if [ "$exit_code" -eq 0 ] && [ -s "$CURRENT_RESPONSE_FILE" ]; then
        response_bytes=$(wc -c < "$CURRENT_RESPONSE_FILE" | tr -d '[:space:]')
        log "OK (response=${response_bytes}B, workspace=$CURRENT_RUN_DIR)"
    else
        log "FAILED (exit=$exit_code, workspace=$CURRENT_RUN_DIR)"
        {
            printf '%s\n' '--- last command output ---'
            tail -n 30 "$CURRENT_RAW_FILE" 2>/dev/null || true
            printf '%s\n' '--- end command output ---'
        } >> "$LOG_FILE"
    fi

    cleanup_current_run
    return "$exit_code"
}

while [ "$#" -gt 0 ]; do
    case "$1" in
        --once) RUN_ONCE=true ;;
        --dry-run) DRY_RUN=true; RUN_ONCE=true ;;
        -h|--help) usage; exit 0 ;;
        *) printf 'Unknown option: %s\n' "$1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

if ! is_positive_integer "$MIN_INTERVAL_SEC" ||
   ! is_positive_integer "$MAX_INTERVAL_SEC" ||
   ! is_positive_integer "$REQUEST_TIMEOUT_SEC"; then
    printf 'Intervals and timeout must be positive integers.\n' >&2
    exit 2
fi

if [ "$MIN_INTERVAL_SEC" -gt "$MAX_INTERVAL_SEC" ]; then
    printf 'MIN_INTERVAL_SEC must be <= MAX_INTERVAL_SEC.\n' >&2
    exit 2
fi

CODEX_BIN="${CODEX_BIN:-$(command -v codex 2>/dev/null || true)}"
if [ -z "$CODEX_BIN" ] || [ ! -x "$CODEX_BIN" ]; then
    printf 'Codex executable not found. Set CODEX_BIN or update PATH.\n' >&2
    exit 127
fi

if [ ! -r "$PROMPTS_FILE" ]; then
    printf 'Prompt file is not readable: %s\n' "$PROMPTS_FILE" >&2
    exit 2
fi

if ! pick_prompt >/dev/null; then
    printf 'Prompt file has no non-empty, non-comment lines: %s\n' "$PROMPTS_FILE" >&2
    exit 2
fi

mkdir -p "$(dirname "$LOG_FILE")"
log "Started (interval=${MIN_INTERVAL_SEC}-${MAX_INTERVAL_SEC}s, timeout=${REQUEST_TIMEOUT_SEC}s)."

while true; do
    run_request || true

    if [ "$RUN_ONCE" = true ]; then
        break
    fi

    sleep_sec=$(random_interval)
    log "Next request in ${sleep_sec}s."
    sleep "$sleep_sec"
done

log "Finished."
