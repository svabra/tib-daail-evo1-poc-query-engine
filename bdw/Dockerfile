FROM python:3.12-slim

ARG DUCKDB_VERSION=1.5.0
ARG IMAGE_VERSION=0.3.0

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HOME=/tmp \
    IMAGE_VERSION=${IMAGE_VERSION} \
    PORT=8000 \
    DUCKDB_DATABASE=/workspace/workspace.duckdb \
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

RUN mkdir -p /opt/duckdb/extensions

RUN python - <<'PY'
from pathlib import Path
import duckdb

extension_dir = Path("/opt/duckdb/extensions")
extension_dir.mkdir(parents=True, exist_ok=True)

conn = duckdb.connect(":memory:")
conn.execute(f"SET extension_directory = '{extension_dir.as_posix()}'")
for extension in ("httpfs", "postgres"):
    conn.execute(f"INSTALL {extension}")
conn.close()
PY

COPY bdw /app

EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "bit_data_workbench.main:app", "--host", "0.0.0.0", "--port", "8000"]
