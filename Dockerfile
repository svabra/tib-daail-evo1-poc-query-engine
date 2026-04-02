FROM python:3.12-slim

ARG DUCKDB_VERSION=1.5.0
ARG IMAGE_VERSION=0.3.17

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/tmp \
    IMAGE_VERSION=${IMAGE_VERSION} \
    DUCKDB_DATABASE=/tmp/workspace/workspace.duckdb \
    DUCKDB_UI_PORT=4213 \
    DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions

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
        "duckdb-cli==${DUCKDB_VERSION}" \
        "pytz==2025.2"

RUN rm -rf /tmp/.duckdb \
    && mkdir -p /opt/duckdb/extensions

RUN python - <<'PY'
from pathlib import Path
import duckdb
import pytz

extension_dir = Path("/opt/duckdb/extensions")
extension_dir.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect(":memory:")
conn.execute(f"SET extension_directory = '{extension_dir.as_posix()}'")
for extension in ("ui", "httpfs", "postgres"):
    conn.execute(f"INSTALL {extension}")
conn.close()
PY

COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY docker/start_duckdb_ui.py /usr/local/bin/start_duckdb_ui.py

RUN chmod +x /usr/local/bin/docker-entrypoint.sh /usr/local/bin/start_duckdb_ui.py

EXPOSE 4213

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["ui"]
