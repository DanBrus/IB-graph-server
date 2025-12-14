#!/usr/bin/env bash
set -euo pipefail

TYPEDB_BIN="${TYPEDB_BIN:-${TYPEDB_HOME}/typedb}"
TYPEDB_ADDR="${TYPEDB_ADDRESS:-0.0.0.0:1729}"
DB_NAME="${DB_NAME:-tsarstvie-investigation}"
DUMPS_DIR="${DUMPS_DIR:-/dumps}"
SCHEMA_DUMP="${SCHEMA_DUMP:-${DUMPS_DIR}/schema}"
DATA_DUMP="${DATA_DUMP:-${DUMPS_DIR}/data}"
STARTUP_TIMEOUT="${TYPEDB_STARTUP_TIMEOUT:-60}"

mkdir -p "${SCHEMA_DUMP}" "${DATA_DUMP}"

cleanup() {
    echo "Exporting database ${DB_NAME} to ${SCHEMA_DUMP} and ${DATA_DUMP}..."
    if ! "${TYPEDB_BIN}" console --command "database export ${DB_NAME} ${SCHEMA_DUMP} ${DATA_DUMP}" >/proc/1/fd/1 2>/proc/1/fd/2; then
        echo "Export failed (possibly missing DB); continuing shutdown."
    fi
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
}
trap cleanup EXIT TERM INT

echo "Starting TypeDB server on ${TYPEDB_ADDR}..."
"${TYPEDB_BIN}" server --address="${TYPEDB_ADDR}" >/proc/1/fd/1 2>/proc/1/fd/2 &
SERVER_PID=$!

HOST="${TYPEDB_ADDR%:*}"
PORT="${TYPEDB_ADDR##*:}"
if [ "${HOST}" = "0.0.0.0" ]; then
    HOST="127.0.0.1"
fi

echo "Waiting for TypeDB to become ready (${STARTUP_TIMEOUT}s timeout)..."
for i in $(seq 1 "${STARTUP_TIMEOUT}"); do
    if nc -z "${HOST}" "${PORT}" >/dev/null 2>&1; then
        break
    fi
    if [ "${i}" -eq "${STARTUP_TIMEOUT}" ]; then
        echo "TypeDB did not start in time."
        exit 1
    fi
    sleep 1
done
echo "TypeDB is ready."

echo "Importing database ${DB_NAME} from ${SCHEMA_DUMP} and ${DATA_DUMP} (if present)..."
if ! "${TYPEDB_BIN}" console --command "database import ${DB_NAME} ${SCHEMA_DUMP} ${DATA_DUMP}" >/proc/1/fd/1 2>/proc/1/fd/2; then
    echo "Import failed (possibly empty dumps); continuing."
fi

wait "${SERVER_PID}"
