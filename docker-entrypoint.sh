#!/usr/bin/env bash
set -euo pipefail

run_ui() {
    local public_port="${DUCKDB_UI_PORT:-4213}"
    local bind_address="${DUCKDB_UI_BIND_ADDRESS:-0.0.0.0}"

    mkdir -p /workspace

    python /usr/local/bin/start_duckdb_ui.py &
    local ui_pid=$!

    socat "TCP-LISTEN:${public_port},fork,reuseaddr,bind=${bind_address}" "TCP6:[::1]:${public_port}" &
    local proxy_pid=$!

    shutdown() {
        kill "${ui_pid}" "${proxy_pid}" 2>/dev/null || true
        wait "${ui_pid}" 2>/dev/null || true
        wait "${proxy_pid}" 2>/dev/null || true
    }

    trap shutdown INT TERM

    wait -n "${ui_pid}" "${proxy_pid}"
    local status=$?

    shutdown
    return "${status}"
}

command="${1:-ui}"

case "${command}" in
    ui)
        shift || true
        run_ui "$@"
        ;;
    cli)
        shift || true
        if [[ "$#" -eq 0 ]]; then
            exec duckdb "${DUCKDB_DATABASE:-/workspace/workspace.duckdb}"
        fi
        exec duckdb "$@"
        ;;
    shell)
        shift || true
        exec bash "$@"
        ;;
    bash|sh|duckdb|python)
        exec "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
