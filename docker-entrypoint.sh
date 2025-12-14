#!/usr/bin/env sh
set -e

TYPEDB_BIN="${TYPEDB_BIN:-${TYPEDB_HOME}/typedb}"
TYPEDB_ADDR="${TYPEDB_ADDRESS:-127.0.0.1:1729}"
STARTUP_TIMEOUT="${TYPEDB_STARTUP_TIMEOUT:-60}"

host_from_addr() {
    echo "$1" | cut -d: -f1
}

port_from_addr() {
    echo "$1" | cut -d: -f2
}

echo "Starting TypeDB server from ${TYPEDB_BIN} ..."
"${TYPEDB_BIN}" server > /proc/1/fd/1 2>/proc/1/fd/2 &
TYPEDB_PID=$!

echo "Waiting for TypeDB on ${TYPEDB_ADDR} (timeout: ${STARTUP_TIMEOUT}s)..."
i=0
HOST="$(host_from_addr "${TYPEDB_ADDR}")"
PORT="$(port_from_addr "${TYPEDB_ADDR}")"
while ! nc -z "${HOST}" "${PORT}" >/dev/null 2>&1; do
    i=$((i + 1))
    if [ "${i}" -ge "${STARTUP_TIMEOUT}" ]; then
        echo "TypeDB did not become ready in ${STARTUP_TIMEOUT}s, exiting."
        exit 1
    fi
    sleep 1
done
echo "TypeDB is up."

echo "Starting app: $*"
exec "$@"
