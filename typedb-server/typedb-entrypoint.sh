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

# --------- резервные копии в /reserve --------- #
cat >/tmp/typedb_reserve.sh <<EOF
#!/usr/bin/env bash
set -euo pipefail

TYPEDB_BIN="${TYPEDB_BIN}"
TYPEDB_ADDR="${TYPEDB_ADDR}"
DB_NAME="${DB_NAME}"
SCHEMA_DUMP="${SCHEMA_DUMP}"
DATA_DUMP="${DATA_DUMP}"
RESERVE_COPY_TIME="${RESERVE_COPY_TIME}"
RESERVE_DIR="${RESERVE_DIR}"

TARGET_HOUR="\${RESERVE_COPY_TIME%%:*}"
LAST_BACKUP_DAY=""

mkdir -p "\${RESERVE_DIR}"

health_check() {
    if "\${TYPEDB_BIN}" console --command "database list" >/dev/null 2>&1; then
        echo "[reserve] TypeDB healthy at \${TYPEDB_ADDR}"
        return 0
    else
        echo "[reserve] TypeDB health check FAILED at \${TYPEDB_ADDR}"
        return 1
    fi
}

do_backup() {
    TS=\$(date +%Y%m%d%H%M%S)
    DEST="\${RESERVE_DIR}/\${TS}"
    mkdir -p "\${DEST}"
    if [ -d "\${SCHEMA_DUMP}" ]; then
        cp -a "\${SCHEMA_DUMP}" "\${DEST}/schema" || true
    fi
    if [ -d "\${DATA_DUMP}" ]; then
        cp -a "\${DATA_DUMP}" "\${DEST}/data" || true
    fi
    echo "[reserve] Backup created at \${DEST}"
}

while true; do
    health_check || true
    CURRENT_DAY=\$(date +%F)
    CURRENT_HOUR=\$(date +%H)
    if [ "\${CURRENT_HOUR}" = "\${TARGET_HOUR}" ] && [ "\${CURRENT_DAY}" != "\${LAST_BACKUP_DAY}" ]; then
        do_backup
        LAST_BACKUP_DAY="\${CURRENT_DAY}"
    fi
    sleep 3600
done
EOF
chmod +x /tmp/typedb_reserve.sh
/tmp/typedb_reserve.sh >/proc/1/fd/1 2>/proc/1/fd/2 &

wait "${SERVER_PID}"
