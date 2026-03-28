from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip() or default


def env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def env_bool(name: str, default: bool) -> bool:
    raw = env_optional(name)
    if raw is None:
        return default

    normalized = raw.lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False

    raise ValueError(f"Unsupported boolean value for {name}: {raw}")


@dataclass(slots=True)
class Settings:
    service_name: str
    ui_title: str
    image_version: str
    port: int
    duckdb_database: Path
    duckdb_extension_directory: Path
    max_result_rows: int
    s3_endpoint: str | None
    s3_region: str | None
    s3_bucket: str | None
    s3_access_key_id: str | None
    s3_secret_access_key: str | None
    s3_url_style: str | None
    s3_use_ssl: bool
    s3_session_token: str | None
    s3_startup_view_schema: str
    s3_startup_views: str | None
    pg_host: str | None
    pg_port: str | None
    pg_user: str | None
    pg_password: str | None
    pg_oltp_database: str | None
    pg_olap_database: str | None
    pod_name: str | None
    pod_namespace: str | None
    pod_ip: str | None
    node_name: str | None

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            service_name="bit-data-workbench",
            ui_title="DAAIFL Query Workbench",
            image_version=env("IMAGE_VERSION", "unknown"),
            port=int(env("PORT", "8000")),
            duckdb_database=Path(env("DUCKDB_DATABASE", "/workspace/workspace.duckdb")),
            duckdb_extension_directory=Path(
                env("DUCKDB_EXTENSION_DIRECTORY", "/opt/duckdb/extensions")
            ),
            max_result_rows=int(env("MAX_RESULT_ROWS", "200")),
            s3_endpoint=env_optional("S3_ENDPOINT"),
            s3_region=env_optional("S3_REGION"),
            s3_bucket=env_optional("S3_BUCKET"),
            s3_access_key_id=env_optional("S3_ACCESS_KEY_ID"),
            s3_secret_access_key=env_optional("S3_SECRET_ACCESS_KEY"),
            s3_url_style=env_optional("S3_URL_STYLE"),
            s3_use_ssl=env_bool("S3_USE_SSL", True),
            s3_session_token=env_optional("S3_SESSION_TOKEN"),
            s3_startup_view_schema=env("S3_STARTUP_VIEW_SCHEMA", "s3"),
            s3_startup_views=env_optional("S3_STARTUP_VIEWS"),
            pg_host=env_optional("PG_HOST"),
            pg_port=env_optional("PG_PORT"),
            pg_user=env_optional("PG_USER") or env_optional("PGUSER"),
            pg_password=env_optional("PG_PASSWORD") or env_optional("PGPASSWORD"),
            pg_oltp_database=env_optional("PG_OLTP_DATABASE") or env_optional("PGDATABASE"),
            pg_olap_database=env_optional("PG_OLAP_DATABASE"),
            pod_name=env_optional("POD_NAME"),
            pod_namespace=env_optional("POD_NAMESPACE"),
            pod_ip=env_optional("POD_IP"),
            node_name=env_optional("NODE_NAME"),
        )

    def runtime_info(self) -> dict[str, str]:
        return {
            "service": self.service_name,
            "image_version": self.image_version,
            "hostname": socket.gethostname(),
            "pod_name": self.pod_name or "unknown",
            "pod_namespace": self.pod_namespace or "unknown",
            "pod_ip": self.pod_ip or "unknown",
            "node_name": self.node_name or "unknown",
            "duckdb_database": self.duckdb_database.as_posix(),
            "timestamp_utc": datetime.now(UTC).isoformat(),
        }
