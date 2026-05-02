#!/usr/bin/env bash
set -euo pipefail

log() {
    echo "[$(date -Is)] $*"
}

TYPEDB_BIN="${TYPEDB_BIN:-typedb}"
TYPEDB_ADDRESS="${TYPEDB_ADDRESS:-0.0.0.0:1729}"
TYPEDB_HTTP_ADDRESS="${TYPEDB_HTTP_ADDRESS:-0.0.0.0:8000}"
TYPEDB_CLIENT_ADDRESS="${TYPEDB_CLIENT_ADDRESS:-127.0.0.1:1729}"
TYPEDB_USERNAME="${TYPEDB_USERNAME:-admin}"
TYPEDB_PASSWORD="${TYPEDB_PASSWORD:-password}"
TYPEDB_TLS_DISABLED="${TYPEDB_TLS_DISABLED:-true}"
TYPEDB_DATA_DIR="${TYPEDB_DATA_DIR:-/var/lib/typedb/data}"
STARTUP_TIMEOUT="${TYPEDB_STARTUP_TIMEOUT:-60}"

DB_NAME="${DB_NAME:-tsarstvie-investigation}"
DUMPS_DIR="${DUMPS_DIR:-/dumps}"
SCHEMA_DUMP="${SCHEMA_DUMP:-${DUMPS_DIR}/schema}"
DATA_DUMP="${DATA_DUMP:-${DUMPS_DIR}/data}"
RESERVE_COPY_TIME="${RESERVE_COPY_TIME:-12:00}"
RESERVE_DIR="${RESERVE_DIR:-${DUMPS_DIR}/reserve}"

TYPEDB_BIN_RESOLVED="$(command -v "${TYPEDB_BIN}")"
TYPEDB_BIN="${TYPEDB_BIN_RESOLVED}"

typedb_connect=(
    --address "${TYPEDB_CLIENT_ADDRESS}"
    --username "${TYPEDB_USERNAME}"
    --password "${TYPEDB_PASSWORD}"
)

if [ "${TYPEDB_TLS_DISABLED}" = "true" ]; then
    typedb_connect+=(--tls-disabled)
fi

run_console() {
    "${TYPEDB_BIN}" console "${typedb_connect[@]}" "$@"
}

health_check() {
    run_console --command "database list" >/dev/null 2>&1
}

export_database() {
    local schema_tmp="${SCHEMA_DUMP}.tmp.$$"
    local data_tmp="${DATA_DUMP}.tmp.$$"

    mkdir -p "$(dirname "${SCHEMA_DUMP}")" "$(dirname "${DATA_DUMP}")"
    rm -f "${schema_tmp}" "${data_tmp}"

    log "Exporting database ${DB_NAME} to ${SCHEMA_DUMP} and ${DATA_DUMP}..."

    if ! run_console --command "database export ${DB_NAME} ${schema_tmp} ${data_tmp}"; then
        rm -f "${schema_tmp}" "${data_tmp}"
        log "Export failed, possibly missing DB; continuing."
        return 1
    fi

    mv -f "${schema_tmp}" "${SCHEMA_DUMP}"
    mv -f "${data_tmp}" "${DATA_DUMP}"

    log "Export completed."
}

seconds_until_reserve_time() {
    local hour
    local minute
    local now
    local target

    if ! [[ "${RESERVE_COPY_TIME}" =~ ^([0-9]{1,2}):([0-9]{2})$ ]]; then
        log "[reserve] Invalid RESERVE_COPY_TIME=${RESERVE_COPY_TIME}; fallback to 3600 seconds."
        echo 3600
        return
    fi

    hour="${BASH_REMATCH[1]}"
    minute="${BASH_REMATCH[2]}"

    now="$(date +%s)"
    target="$(date -d "today ${hour}:${minute}:00" +%s)"

    if [ "${target}" -le "${now}" ]; then
        target="$(date -d "tomorrow ${hour}:${minute}:00" +%s)"
    fi

    echo $((target - now))
}

rotate_and_dump() {
    local ts
    local dest
    local moved=false

    ts="$(date +%Y%m%d%H%M%S)"
    dest="$(dirname "${RESERVE_DIR}")/$(basename "${RESERVE_DIR}")_${ts}"

    log "[reserve] Removing previous reserve directory at ${RESERVE_DIR}"
    rm -rf "${RESERVE_DIR}"

    mkdir -p "${dest}"

    for src in "${SCHEMA_DUMP}" "${DATA_DUMP}"; do
        if [ -e "${src}" ]; then
            mv "${src}" "${dest}/"
            moved=true
        fi
    done

    if [ "${moved}" = true ]; then
        log "[reserve] Existing dumps moved to ${dest}"
    else
        rmdir "${dest}" 2>/dev/null || true
        log "[reserve] No existing dumps to move into ${dest}"
    fi

    export_database || log "[reserve] Export failed; moved dumps, if any, remain in ${dest}"
}

reserve_worker() {
    while true; do
        sleep "$(seconds_until_reserve_time)"

        if health_check; then
            log "[reserve] TypeDB healthy at ${TYPEDB_CLIENT_ADDRESS}"
        else
            log "[reserve] TypeDB health check FAILED at ${TYPEDB_CLIENT_ADDRESS}"
        fi

        rotate_and_dump || true
    done
}

cleanup() {
    if [ "${CLEANUP_STARTED:-false}" = true ]; then
        return
    fi
    CLEANUP_STARTED=true

    log "Shutdown requested."

    if [ -n "${RESERVE_PID:-}" ]; then
        kill "${RESERVE_PID}" 2>/dev/null || true
        wait "${RESERVE_PID}" 2>/dev/null || true
    fi

    export_database || true

    if [ -n "${SERVER_PID:-}" ]; then
        kill "${SERVER_PID}" 2>/dev/null || true
        wait "${SERVER_PID}" 2>/dev/null || true
    fi
}

trap cleanup EXIT
trap 'cleanup; exit 0' TERM INT

mkdir -p "${TYPEDB_DATA_DIR}" "${DUMPS_DIR}" "${RESERVE_DIR}" /var/log/typedb

log "Starting TypeDB server on ${TYPEDB_ADDRESS}; HTTP on ${TYPEDB_HTTP_ADDRESS}; data dir ${TYPEDB_DATA_DIR}..."

"${TYPEDB_BIN}" server \
    --server.address="${TYPEDB_ADDRESS}" \
    --server.http.address="${TYPEDB_HTTP_ADDRESS}" \
    --storage.data-directory="${TYPEDB_DATA_DIR}" \
    --logging.directory="/var/log/typedb" &

SERVER_PID=$!

log "Waiting for TypeDB to become ready (${STARTUP_TIMEOUT}s timeout)..."

for i in $(seq 1 "${STARTUP_TIMEOUT}"); do
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        log "TypeDB server exited during startup."
        wait "${SERVER_PID}" 2>/dev/null || true
        exit 1
    fi

    if health_check; then
        log "TypeDB is ready."
        break
    fi

    if [ "${i}" -eq "${STARTUP_TIMEOUT}" ]; then
        log "TypeDB did not start in time."
        exit 1
    fi

    sleep 1
done

if [ -e "${SCHEMA_DUMP}" ] && [ -e "${DATA_DUMP}" ]; then
    log "Importing database ${DB_NAME} from ${SCHEMA_DUMP} and ${DATA_DUMP}..."

    if ! run_console --command "database import ${DB_NAME} ${SCHEMA_DUMP} ${DATA_DUMP}"; then
        log "Import failed. If database already exists, this is expected; continuing."
    fi
else
    log "Skipping import: dumps missing at ${SCHEMA_DUMP} and/or ${DATA_DUMP}."
fi

reserve_worker &
RESERVE_PID=$!

wait "${SERVER_PID}"