ARG TYPEDB_VERSION=3.7.0

FROM typedb/typedb:${TYPEDB_VERSION} AS typedb-dist

FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND="noninteractive"

ENV TYPEDB_SERVER_BIN="/usr/local/bin/typedb-server-pinned" \
    TYPEDB_CONSOLE_BIN="typedb" \
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
        apt-transport-https \
        bash \
        ca-certificates \
        curl \
        gpg; \
    rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    mkdir -p /usr/share/keyrings; \
    curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x17507562824cfdcc" \
        | gpg --dearmor -o /usr/share/keyrings/typedb.gpg; \
    echo "deb [signed-by=/usr/share/keyrings/typedb.gpg] https://repo.typedb.com/public/public-release/deb/ubuntu trusty main" \
        > /etc/apt/sources.list.d/typedb.list

RUN set -eux; \
    printf '%s\n' \
      'Acquire::Retries "5";' \
      'Acquire::http::Timeout "120";' \
      'Acquire::https::Timeout "120";' \
      > /etc/apt/apt.conf.d/80-retries

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends typedb; \
    command -v typedb; \
    typedb console --help >/dev/null; \
    rm -rf /var/lib/apt/lists/*

COPY --from=typedb-dist /opt/ /opt/

RUN set -eux; \
    TYPEDB_DIST_DIR="$(find /opt -maxdepth 1 -type d -name 'typedb-server-linux-*' | head -n 1)"; \
    test -n "${TYPEDB_DIST_DIR}"; \
    test -x "${TYPEDB_DIST_DIR}/typedb"; \
    ln -s "${TYPEDB_DIST_DIR}/typedb" /usr/local/bin/typedb-server-pinned; \
    /usr/local/bin/typedb-server-pinned server --help >/dev/null; \
    mkdir -p "${TYPEDB_DATA_DIR}" "${DUMPS_DIR}" "${RESERVE_DIR}"

COPY typedb-entrypoint.sh /usr/local/bin/typedb-entrypoint.sh
RUN chmod +x /usr/local/bin/typedb-entrypoint.sh

VOLUME ["/var/lib/typedb/data", "/dumps"]

EXPOSE 1729 8000

ENTRYPOINT ["/usr/local/bin/typedb-entrypoint.sh"]
