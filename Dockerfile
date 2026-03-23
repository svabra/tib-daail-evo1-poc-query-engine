FROM python:3.12-slim

ARG DUCKDB_VERSION=1.5.0

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DUCKDB_DATABASE=/workspace/workspace.duckdb \
    DUCKDB_UI_PORT=4213 \
    DUCKDB_EXTENSION_DIRECTORY=/workspace/.duckdb/extensions

WORKDIR /workspace

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        bash \
        ca-certificates \
        curl \
        socat \
        tini \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
        "duckdb==${DUCKDB_VERSION}" \
        "duckdb-cli==${DUCKDB_VERSION}"

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY docker/start_duckdb_ui.py /usr/local/bin/start_duckdb_ui.py

RUN chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/start_duckdb_ui.py \
    && mkdir -p /workspace/.duckdb/extensions /root/.duckdb/extension_data

EXPOSE 4213

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["ui"]
