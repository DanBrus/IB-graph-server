#!/usr/bin/env bash
set -euo pipefail

TYPEDB_BIN="${TYPEDB_BIN:-${TYPEDB_HOME}/typedb}"
TYPEDB_ADDR="${TYPEDB_ADDRESS:-0.0.0.0:1729}"
DB_NAME="${DB_NAME:-tsarstvie-investigation}"
DUMPS_DIR="${DUMPS_DIR:-/dumps}"
SCHEMA_DUMP="${SCHEMA_DUMP:-${DUMPS_DIR}/schema}"
DATA_DUMP="${DATA_DUMP:-${DUMPS_DIR}/data}"
STARTUP_TIMEOUT="${TYPEDB_STARTUP_TIMEOUT:-60}"
RESERVE_COPY_TIME="${RESERVE_COPY_TIME:-12:00}"
RESERVE_DIR="${RESERVE_DIR:-${DUMPS_DIR}/reserve}"

typedb_connect="--address ${TYPEDB_ADDR} --username admin --password password --tls-disabled"

cleanup() {
    echo "Exporting database ${DB_NAME} to ${SCHEMA_DUMP} and ${DATA_DUMP}..."
    if ! "${TYPEDB_BIN}" console $typedb_connect --command "database export ${DB_NAME} ${SCHEMA_DUMP} ${DATA_DUMP}" >/proc/1/fd/1 2>/proc/1/fd/2; then
        echo "Export failed (possibly missing DB); continuing shutdown."
    fi
    kill "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
}
trap cleanup EXIT TERM INT

echo "Starting TypeDB server on ${TYPEDB_ADDR}..."
"${TYPEDB_BIN}" server --server.address "${TYPEDB_ADDR}" >/proc/1/fd/1 2>/proc/1/fd/2 &
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

if [ -e "${SCHEMA_DUMP}" ] && [ -e "${DATA_DUMP}" ]; then
    echo "Importing database ${DB_NAME} from ${SCHEMA_DUMP} and ${DATA_DUMP}..."
    if ! "${TYPEDB_BIN}" console $typedb_connect --command "database import ${DB_NAME} ${SCHEMA_DUMP} ${DATA_DUMP}" >/proc/1/fd/1 2>/proc/1/fd/2; then
        echo "Import failed (dumps found but could not be imported); continuing."
    fi
else
    echo "Skipping import: dumps missing at ${SCHEMA_DUMP} and/or ${DATA_DUMP}."
fi

# --------- резервные копии в /reserve --------- #
cat >/tmp/typedb_reserve.sh <<EOF
#!/usr/bin/env bash
set -euo pipefail

TYPEDB_BIN="${TYPEDB_BIN}"
TYPEDB_ADDR="${HOST}:${PORT}"
DB_NAME="${DB_NAME}"
SCHEMA_DUMP="${SCHEMA_DUMP}"
DATA_DUMP="${DATA_DUMP}"
RESERVE_DIR="${RESERVE_DIR}"

typedb_connect="--address \${TYPEDB_ADDR} --username admin --password password --tls-disabled"
RESERVE_BASE_DIR="\$(dirname "\${RESERVE_DIR}")"
RESERVE_PREFIX="\$(basename "\${RESERVE_DIR}")"
mkdir -p "\${RESERVE_BASE_DIR}"

health_check() {
    if "\${TYPEDB_BIN}" console  \${typedb_connect} --command "database list" >/dev/null 2>&1; then
        echo "[reserve] TypeDB healthy at \${TYPEDB_ADDR}"
        return 0
    else
        echo "[reserve] TypeDB health check FAILED at \${TYPEDB_ADDR}"
        return 1
    fi
}

rotate_and_dump() {
    TS=\$(date +%Y%m%d%H%M%S)
    DEST="\${RESERVE_BASE_DIR}/\${RESERVE_PREFIX}_\${TS}"

    echo "[reserve] Removing previous reserve directory at \${RESERVE_DIR}"
    rm -rf "\${RESERVE_DIR}"

    mkdir -p "\${DEST}"
    MOVED=false
    for SRC in "\${SCHEMA_DUMP}" "\${DATA_DUMP}"; do
        if [ -e "\${SRC}" ]; then
            mv "\${SRC}" "\${DEST}/"
            MOVED=true
        fi
    done
    if [ "\${MOVED}" = true ]; then
        echo "[reserve] Existing dumps moved to \${DEST}"
    else
        rmdir "\${DEST}" 2>/dev/null || true
        echo "[reserve] No existing dumps to move into \${DEST}"
    fi

    mkdir -p "\$(dirname "\${SCHEMA_DUMP}")" "\$(dirname "\${DATA_DUMP}")"
    echo "[reserve] Exporting database \${DB_NAME} to \${SCHEMA_DUMP} and \${DATA_DUMP}..."
    if ! "\${TYPEDB_BIN}" console \${typedb_connect} --command "database export \${DB_NAME} \${SCHEMA_DUMP} \${DATA_DUMP}" >/proc/1/fd/1 2>/proc/1/fd/2; then
        echo "[reserve] Export failed; keeping moved dumps in \${DEST}"
    else
        echo "[reserve] Export completed."
    fi
}

while true; do
    health_check || true
    rotate_and_dump
    sleep 3600
done
EOF
chmod +x /tmp/typedb_reserve.sh
/tmp/typedb_reserve.sh >/proc/1/fd/1 2>/proc/1/fd/2 &

wait "${SERVER_PID}"
