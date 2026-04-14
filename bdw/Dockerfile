FROM python:3.12-slim

ARG DUCKDB_VERSION=1.4.4
ARG IMAGE_VERSION=0.4.7

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/tmp \
    IMAGE_VERSION=${IMAGE_VERSION} \
    PORT=8000 \
    DUCKDB_DATABASE=/tmp/workspace/workspace.duckdb \
    DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions

WORKDIR /app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        ca-certificates \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY bdw/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm -f /tmp/requirements.txt

RUN mkdir -p /opt/duckdb/extensions /app/logs/bdw /app/logs/playwright /app/logs/schema-panel /app/logs/misc

RUN python - <<'PY'
from pathlib import Path
import duckdb
import pytz

extension_dir = Path("/opt/duckdb/extensions")
extension_dir.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect(":memory:")
conn.execute(f"SET extension_directory = '{extension_dir.as_posix()}'")
for extension in ("httpfs", "postgres"):
    conn.execute(f"INSTALL {extension}")
conn.close()
PY

COPY bdw /app

RUN chgrp -R 0 /app /opt/duckdb \
    && chmod -R g=u /app /opt/duckdb

EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "-m", "bit_data_workbench.launcher"]
