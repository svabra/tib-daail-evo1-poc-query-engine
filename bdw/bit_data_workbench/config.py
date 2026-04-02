from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


WORKBENCH_ENVIRONMENT_VARIABLES = (
    "IMAGE_VERSION",
    "PORT",
    "DUCKDB_DATABASE",
    "DUCKDB_EXTENSION_DIRECTORY",
    "MAX_RESULT_ROWS",
    "S3_ENDPOINT",
    "S3_REGION",
    "S3_BUCKET",
    "S3_ACCESS_KEY_ID",
    "S3_ACCESS_KEY_ID_FILE",
    "S3_SECRET_ACCESS_KEY",
    "S3_SECRET_ACCESS_KEY_FILE",
    "S3_URL_STYLE",
    "S3_USE_SSL",
    "S3_VERIFY_SSL",
    "S3_CA_CERT_FILE",
    "S3_SESSION_TOKEN",
    "S3_SESSION_TOKEN_FILE",
    "S3_STARTUP_VIEW_SCHEMA",
    "S3_STARTUP_VIEWS",
    "PG_HOST",
    "PG_PORT",
    "PG_USER",
    "PGUSER",
    "PG_PASSWORD",
    "PGPASSWORD",
    "PG_OLTP_DATABASE",
    "PGDATABASE",
    "PG_OLAP_DATABASE",
    "POD_NAME",
    "POD_NAMESPACE",
    "POD_IP",
    "NODE_NAME",
)

REDACTED_ENVIRONMENT_VARIABLES = {
    "PG_PASSWORD",
    "PGPASSWORD",
    "S3_SECRET_ACCESS_KEY",
    "S3_SESSION_TOKEN",
}


def env(name: str, default: str = "") -> str:
    value = os.getenv(name, default)
    return value.strip() or default


def env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def env_path_optional(name: str) -> Path | None:
    value = env_optional(name)
    return Path(value) if value is not None else None


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


def read_secret_file(path: Path, *, variable_name: str) -> str:
    try:
        raw_value = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"{variable_name} file '{path}' could not be read: {exc}") from exc

    value = raw_value.strip()
    if not value:
        raise ValueError(f"{variable_name} file '{path}' is empty.")
    return value


def resolve_secret_value(
    *,
    value: str | None,
    file_path: Path | None,
    variable_name: str,
) -> str | None:
    if value is not None:
        return value
    if file_path is None:
        return None
    return read_secret_file(file_path, variable_name=f"{variable_name}_FILE")


def format_environment_value(name: str) -> str:
    value = os.getenv(name)
    if value is None:
        return "null"
    if name in REDACTED_ENVIRONMENT_VARIABLES:
        return "[REDACTED]"
    if value == "":
        return '""'
    return value


def default_image_version() -> str:
    value = env_optional("IMAGE_VERSION")
    if value is not None:
        return value

    version_file = Path(__file__).resolve().parents[2] / "VERSION"
    try:
        raw_value = version_file.read_text(encoding="utf-8").strip()
    except OSError:
        raw_value = ""

    return raw_value or "dev"


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
    s3_access_key_id_file: Path | None
    s3_secret_access_key: str | None
    s3_secret_access_key_file: Path | None
    s3_url_style: str | None
    s3_use_ssl: bool
    s3_verify_ssl: bool
    s3_ca_cert_file: Path | None
    s3_session_token: str | None
    s3_session_token_file: Path | None
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
        # Runtime selection is intentionally environment-driven instead of using
        # an explicit APP_ENV flag. Local launchers inject localhost endpoints
        # and direct credentials, while the RHOS deployment injects pod metadata,
        # Vault-mounted file paths, TLS settings, and cluster endpoints.
        return cls(
            service_name="bit-data-workbench",
            ui_title="DAAIFL Workbench",
            image_version=default_image_version(),
            port=int(env("PORT", "8000")),
            duckdb_database=Path(env("DUCKDB_DATABASE", "/tmp/workspace/workspace.duckdb")),
            duckdb_extension_directory=Path(
                env("DUCKDB_EXTENSION_DIRECTORY", "/opt/duckdb/extensions")
            ),
            max_result_rows=int(env("MAX_RESULT_ROWS", "200")),
            s3_endpoint=env_optional("S3_ENDPOINT"),
            s3_region=env_optional("S3_REGION"),
            s3_bucket=env_optional("S3_BUCKET"),
            s3_access_key_id=env_optional("S3_ACCESS_KEY_ID"),
            s3_access_key_id_file=env_path_optional("S3_ACCESS_KEY_ID_FILE"),
            s3_secret_access_key=env_optional("S3_SECRET_ACCESS_KEY"),
            s3_secret_access_key_file=env_path_optional("S3_SECRET_ACCESS_KEY_FILE"),
            s3_url_style=env_optional("S3_URL_STYLE"),
            s3_use_ssl=env_bool("S3_USE_SSL", True),
            s3_verify_ssl=env_bool("S3_VERIFY_SSL", True),
            s3_ca_cert_file=env_path_optional("S3_CA_CERT_FILE"),
            s3_session_token=env_optional("S3_SESSION_TOKEN"),
            s3_session_token_file=env_path_optional("S3_SESSION_TOKEN_FILE"),
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

    def current_s3_access_key_id(self) -> str | None:
        # Prefer a direct env value for local/dev flows, then fall back to the
        # file path used by RHOS Vault bindings.
        return resolve_secret_value(
            value=self.s3_access_key_id,
            file_path=self.s3_access_key_id_file,
            variable_name="S3_ACCESS_KEY_ID",
        )

    def current_s3_secret_access_key(self) -> str | None:
        # Prefer a direct env value for local/dev flows, then fall back to the
        # file path used by RHOS Vault bindings.
        return resolve_secret_value(
            value=self.s3_secret_access_key,
            file_path=self.s3_secret_access_key_file,
            variable_name="S3_SECRET_ACCESS_KEY",
        )

    def current_s3_session_token(self) -> str | None:
        # Session tokens follow the same env-first, file-second pattern.
        return resolve_secret_value(
            value=self.s3_session_token,
            file_path=self.s3_session_token_file,
            variable_name="S3_SESSION_TOKEN",
        )

    def apply_runtime_environment(self) -> None:
        if not self.s3_ca_cert_file:
            return

        ca_bundle_path = self.s3_ca_cert_file.as_posix()
        for env_name in ("AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"):
            os.environ[env_name] = ca_bundle_path

    @staticmethod
    def startup_environment_lines() -> list[str]:
        return [
            f"{name}={format_environment_value(name)}"
            for name in WORKBENCH_ENVIRONMENT_VARIABLES
        ]
