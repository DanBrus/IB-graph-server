FROM debian:bookworm-slim

ENV TYPEDB_HOME="/root/.typedb" \
    PATH="/root/.typedb:${PATH}" \
    TYPEDB_ADDRESS="0.0.0.0:1729" \
    DB_NAME="tsarstvie-investigation" \
    DUMPS_DIR="/dumps"

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates netcat-openbsd bash \
    && rm -rf /var/lib/apt/lists/*

# Install TypeDB server + console
RUN curl -sSL https://typedb.com/install.sh | sh

COPY typedb-entrypoint.sh /usr/local/bin/typedb-entrypoint.sh
RUN chmod +x /usr/local/bin/typedb-entrypoint.sh

VOLUME ["/dumps"]

EXPOSE 1729

ENTRYPOINT ["typedb-entrypoint.sh"]
