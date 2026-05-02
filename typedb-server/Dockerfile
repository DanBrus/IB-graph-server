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
        ca-certificates \
        curl \
        apt-transport-https \
        gpg \
        bash; \
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
    typedb server --help >/dev/null; \
    mkdir -p "${TYPEDB_DATA_DIR}" "${DUMPS_DIR}" "${RESERVE_DIR}"; \
    rm -rf /var/lib/apt/lists/*

COPY typedb-entrypoint.sh /usr/local/bin/typedb-entrypoint.sh
RUN chmod +x /usr/local/bin/typedb-entrypoint.sh

VOLUME ["/var/lib/typedb/data", "/dumps"]

EXPOSE 1729 8000

ENTRYPOINT ["/usr/local/bin/typedb-entrypoint.sh"]
