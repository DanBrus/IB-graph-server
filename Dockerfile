FROM python:3.12-slim

ARG TYPEDB_VERSION=3.5.5
ARG TYPEDB_DOWNLOAD_URL="https://cloudsmith.io/~vaticle/repos/typedb/packages/download/typedb-all-linux/${TYPEDB_VERSION}/typedb-all-linux-${TYPEDB_VERSION}.tar.gz"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    TYPEDB_HOME="/opt/typedb" \
    TYPEDB_DATA="/var/lib/typedb" \
    TYPEDB_ADDRESS="127.0.0.1:1729"

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем TypeDB server
RUN mkdir -p "${TYPEDB_HOME}" "${TYPEDB_DATA}" \
    && curl -fSL "${TYPEDB_DOWNLOAD_URL}" \
        | tar -xz -C "${TYPEDB_HOME}" --strip-components=1 \
    && rm -rf "${TYPEDB_HOME}/server/data" \
    && ln -s "${TYPEDB_DATA}" "${TYPEDB_HOME}/server/data"

ENV PATH="${TYPEDB_HOME}:${PATH}"

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY db ./db
COPY src ./src

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 8001 1729

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["uvicorn", "graph_api:app", "--host", "0.0.0.0", "--port", "8001"]
