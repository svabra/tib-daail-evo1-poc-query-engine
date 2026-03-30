#!/usr/bin/env bash
set -euo pipefail

run_ui() {
    local public_port="${DUCKDB_UI_PORT:-4213}"
    local bind_address="${DUCKDB_UI_BIND_ADDRESS:-0.0.0.0}"
    local proxy_target=""
    local database_path="${DUCKDB_DATABASE:-/tmp/workspace/workspace.duckdb}"

    export HOME="${HOME:-/tmp}"

    mkdir -p "$(dirname "${database_path}")"
    mkdir -p "${HOME}/.duckdb/extension_data"

    python /usr/local/bin/start_duckdb_ui.py &
    local ui_pid=$!

    for _ in $(seq 1 60); do
        if curl --silent --show-error --output /dev/null --max-time 1 "http://[::1]:${public_port}/info"; then
            proxy_target="TCP6:[::1]:${public_port}"
            break
        fi

        if curl --silent --show-error --output /dev/null --max-time 1 "http://127.0.0.1:${public_port}/info"; then
            proxy_target="TCP4:127.0.0.1:${public_port}"
            break
        fi

        if ! kill -0 "${ui_pid}" 2>/dev/null; then
            echo "DuckDB UI process exited before the proxy target became reachable." >&2
            wait "${ui_pid}" 2>/dev/null || true
            return 1
        fi

        sleep 1
    done

    if [[ -z "${proxy_target}" ]]; then
        echo "DuckDB UI loopback endpoint did not become reachable on ::1 or 127.0.0.1." >&2
        kill "${ui_pid}" 2>/dev/null || true
        wait "${ui_pid}" 2>/dev/null || true
        return 1
    fi

    echo "Publishing DuckDB UI via proxy target ${proxy_target} on ${bind_address}:${public_port}." >&2
    socat "TCP-LISTEN:${public_port},fork,reuseaddr,bind=${bind_address}" "${proxy_target}" &
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
            exec duckdb "${DUCKDB_DATABASE:-/tmp/workspace/workspace.duckdb}"
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
