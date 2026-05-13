ARG TYPEDB_VERSION=3.7.0

FROM typedb/typedb:${TYPEDB_VERSION} AS typedb-dist

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND="noninteractive"

ENV TYPEDB_BIN="typedb" \
    TYPEDB_ADDRESS="0.0.0.0:1729" \
    TYPEDB_HTTP_ADDRESS="0.0.0.0:8000" \
    TYPEDB_CLIENT_ADDRESS="127.0.0.1:1729" \
    TYPEDB_USERNAME="admin" \
    TYPEDB_PASSWORD="password" \
    TYPEDB_TLS_DISABLED="true" \
    TYPEDB_DATA_DIR="/var/lib/typedb/data" \
    DB_NAME="tsarstvie-investigation" \
    DUMPS_DIR="/dumps" \
    RESERVE_COPY_TIME="12:00" \
    RESERVE_DIR="/dumps/reserve"

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        bash \
        ca-certificates; \
    rm -rf /var/lib/apt/lists/*

COPY --from=typedb-dist /opt/typedb-server-linux-* /opt/

RUN set -eux; \
    ln -s /opt/typedb-server-linux-*/typedb /usr/local/bin/typedb; \
    command -v typedb; \
    typedb server --help >/dev/null; \
    mkdir -p "${TYPEDB_DATA_DIR}" "${DUMPS_DIR}" "${RESERVE_DIR}"

COPY typedb-entrypoint.sh /usr/local/bin/typedb-entrypoint.sh
RUN chmod +x /usr/local/bin/typedb-entrypoint.sh

VOLUME ["/var/lib/typedb/data", "/dumps"]

EXPOSE 1729 8000

ENTRYPOINT ["/usr/local/bin/typedb-entrypoint.sh"]
