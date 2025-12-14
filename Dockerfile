FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH="/app/src" \
    TYPEDB_ADDRESS="typedb:1729" \
    TYPEDB_USERNAME="admin" \
    TYPEDB_PASSWORD="password" \
    TYPEDB_TLS_ENABLED="false" \
    TYPEDB_TLS_CA="" \
    TYPEDB_DB_NAME="investigation_board" \
    BOARD_SCHEMA_VERSION="v0.1" \
    INVESTIGATION_NAME="tsarstvie"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY db ./db
COPY src ./src

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 8001

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["uvicorn", "graph_api:app", "--host", "0.0.0.0", "--port", "8001"]
