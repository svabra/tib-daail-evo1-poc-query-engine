#!/usr/bin/env python3
from __future__ import annotations

import os
import re
import signal
import socket
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import duckdb


VALID_EXTENSION = re.compile(r"^[A-Za-z0-9_]+$")
SUPPORTED_S3_VIEW_FORMATS = {"parquet", "csv", "json"}
STOP = False
DEFAULT_VERSION = "dev"
VERSION_FILE_NAME = "VERSION"


def env(name: str, default: str) -> str:
    value = os.getenv(name, default).strip()
    return value or default


def read_text_file(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _normalize_anchor(anchor: Path) -> Path:
    expanded = anchor.expanduser()
    try:
        resolved = expanded.resolve()
    except OSError:
        resolved = expanded.absolute()
    return resolved if resolved.is_dir() else resolved.parent


def discover_version_file(*anchors: Path) -> Path | None:
    effective_anchors = anchors or (Path(__file__).resolve(), Path.cwd())
    seen: set[str] = set()
    for anchor in effective_anchors:
        directory = _normalize_anchor(anchor)
        for candidate_dir in (directory, *directory.parents):
            candidate = candidate_dir / VERSION_FILE_NAME
            key = candidate.as_posix().lower()
            if key in seen:
                continue
            seen.add(key)
            try:
                if candidate.is_file():
                    return candidate
            except OSError:
                continue
    return None


def runtime_image_version() -> str:
    raw_value = os.getenv("IMAGE_VERSION")
    if raw_value is not None:
        normalized = raw_value.strip()
        if normalized:
            return normalized
    embedded_version = read_text_file(discover_version_file(Path(__file__).resolve(), Path.cwd()))
    return embedded_version or DEFAULT_VERSION


def format_bytes(value: int | None) -> str:
    if value is None:
        return "unlimited/unknown"

    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024

    return f"{value} B"


def detect_memory_limit_bytes() -> int | None:
    cgroup_v2 = read_text_file(Path("/sys/fs/cgroup/memory.max"))
    if cgroup_v2 and cgroup_v2 != "max":
        return int(cgroup_v2)

    cgroup_v1 = read_text_file(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"))
    if cgroup_v1:
        limit = int(cgroup_v1)
        if limit < 1 << 60:
            return limit

    return None


def detect_cpu_limit() -> str:
    cgroup_v2 = read_text_file(Path("/sys/fs/cgroup/cpu.max"))
    if cgroup_v2:
        quota, period = cgroup_v2.split()
        if quota == "max":
            return "unlimited"
        return f"{float(quota) / float(period):.2f} CPUs"

    quota = read_text_file(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"))
    period = read_text_file(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
    if quota and period:
        quota_value = int(quota)
        period_value = int(period)
        if quota_value < 0:
            return "unlimited"
        return f"{float(quota_value) / float(period_value):.2f} CPUs"

    return "unknown"


def print_runtime_diagnostics(
    *,
    database_path: Path,
    extension_dir: Path,
    ui_port: str,
) -> None:
    home = Path(env("HOME", "/tmp"))
    extension_data_dir = home / ".duckdb" / "extension_data"
    pod_name = env_optional("POD_NAME")
    pod_namespace = env_optional("POD_NAMESPACE")
    pod_ip = env_optional("POD_IP")
    node_name = env_optional("NODE_NAME")

    print(f"Image version: {runtime_image_version()}", flush=True)
    print(f"DuckDB version: {duckdb.__version__}", flush=True)
    print(f"Hostname: {socket.gethostname()}", flush=True)
    if pod_name is not None:
        print(f"Pod name: {pod_name}", flush=True)
    if pod_namespace is not None:
        print(f"Pod namespace: {pod_namespace}", flush=True)
    if pod_ip is not None:
        print(f"Pod IP: {pod_ip}", flush=True)
    if node_name is not None:
        print(f"Node name: {node_name}", flush=True)
    print(f"UID:GID: {os.getuid()}:{os.getgid()}", flush=True)
    print(f"HOME: {home}", flush=True)
    print(f"DuckDB database path: {database_path}", flush=True)
    print(f"DuckDB extension directory: {extension_dir}", flush=True)
    print(f"DuckDB extension data directory: {extension_data_dir}", flush=True)
    print(f"DuckDB UI port: {ui_port}", flush=True)
    print(f"Memory limit: {format_bytes(detect_memory_limit_bytes())}", flush=True)
    print(f"CPU limit: {detect_cpu_limit()}", flush=True)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_qualified_identifier(*parts: str) -> str:
    return ".".join(sql_identifier(part) for part in parts)


def parse_extensions(raw_value: str) -> list[str]:
    if not raw_value:
        return []

    extensions: list[str] = []
    for item in raw_value.split(","):
        extension = item.strip()
        if not extension:
            continue
        if not VALID_EXTENSION.fullmatch(extension):
            raise ValueError(f"Unsupported extension name: {extension}")
        extensions.append(extension)

    return extensions


def handle_signal(signum: int, _frame: object) -> None:
    global STOP
    STOP = True
    print(f"Received signal {signum}, stopping DuckDB UI.", flush=True)


def ensure_extension(conn: duckdb.DuckDBPyConnection, extension: str) -> None:
    try:
        conn.execute(f"LOAD {extension}")
        print(f"Loaded DuckDB extension '{extension}'.", flush=True)
    except duckdb.Error:
        conn.execute(f"INSTALL {extension}")
        conn.execute(f"LOAD {extension}")
        print(f"Installed and loaded DuckDB extension '{extension}'.", flush=True)


def env_optional(name: str) -> str | None:
    value = os.getenv(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def first_env(*names: str) -> str | None:
    for name in names:
        value = env_optional(name)
        if value is not None:
            return value
    return None


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


def configure_s3_tls(conn: duckdb.DuckDBPyConnection) -> None:
    verify_ssl = env_bool("S3_VERIFY_SSL", True)
    ca_cert_file = env_optional("S3_CA_CERT_FILE")

    conn.execute(f"SET enable_server_cert_verification = {'true' if verify_ssl else 'false'}")
    if ca_cert_file is not None:
        conn.execute(f"SET ca_cert_file = {sql_string(ca_cert_file)}")


def normalize_port(name: str, value: str) -> str:
    if not value.isdigit():
        raise ValueError(f"{name} must be numeric, got: {value}")
    return value


def normalize_s3_endpoint(raw_value: str, use_ssl: bool) -> tuple[str, bool]:
    if "://" in raw_value:
        parsed = urlparse(raw_value)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported S3 endpoint scheme: {parsed.scheme}")
        if not parsed.netloc:
            raise ValueError(f"Invalid S3 endpoint: {raw_value}")
        return parsed.netloc, parsed.scheme == "https"

    return raw_value, use_ssl


def parse_s3_startup_views(raw_value: str) -> list[tuple[str, str, str]]:
    entries: list[tuple[str, str, str]] = []
    for item in re.split(r"[;\r\n]+", raw_value):
        entry = item.strip()
        if not entry:
            continue

        if "=" not in entry:
            raise ValueError(
                "Each S3 startup view entry must use the format "
                "'view_name=s3://bucket/path' or 'view_name=format:s3://bucket/path'."
            )

        view_name, spec = entry.split("=", 1)
        view_name = view_name.strip()
        spec = spec.strip()
        if not view_name or not spec:
            raise ValueError(f"Invalid S3 startup view entry: {entry}")

        prefix = spec.split(":", 1)[0].lower()
        if prefix in SUPPORTED_S3_VIEW_FORMATS and ":" in spec:
            data_format, path = spec.split(":", 1)
            data_format = data_format.lower().strip()
            path = path.strip()
        else:
            data_format = infer_s3_view_format(spec)
            path = spec

        entries.append((view_name, data_format, path))

    return entries


def infer_s3_view_format(path: str) -> str:
    lowered = path.lower()
    if ".parquet" in lowered:
        return "parquet"
    if ".csv" in lowered or ".tsv" in lowered:
        return "csv"
    if ".json" in lowered or ".jsonl" in lowered or ".ndjson" in lowered:
        return "json"

    raise ValueError(
        "Could not infer S3 startup view format from path. "
        "Use 'view_name=format:s3://bucket/path'."
    )


def create_s3_startup_views(conn: duckdb.DuckDBPyConnection) -> None:
    raw_views = env_optional("S3_STARTUP_VIEWS")
    if raw_views is None:
        print("No S3 startup views configured.", flush=True)
        return

    schema = env("S3_STARTUP_VIEW_SCHEMA", "s3")
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {sql_identifier(schema)}")

    for view_name, data_format, path in parse_s3_startup_views(raw_views):
        try:
            if data_format == "parquet":
                query = f"SELECT * FROM read_parquet({sql_string(path)})"
            elif data_format == "csv":
                query = f"SELECT * FROM read_csv_auto({sql_string(path)})"
            elif data_format == "json":
                query = f"SELECT * FROM read_json_auto({sql_string(path)})"
            else:
                raise ValueError(f"Unsupported S3 startup view format: {data_format}")

            conn.execute(
                f"CREATE OR REPLACE VIEW {sql_qualified_identifier(schema, view_name)} AS {query}"
            )
            print(
                f"Created startup view '{schema}.{view_name}' for S3 path '{path}'.",
                flush=True,
            )
        except Exception as exc:
            print(
                f"Failed to create S3 startup view '{schema}.{view_name}' from '{path}': {exc}",
                flush=True,
            )


def create_s3_secret(conn: duckdb.DuckDBPyConnection) -> None:
    endpoint = env_optional("S3_ENDPOINT")
    bucket = env_optional("S3_BUCKET")
    key_id = env_optional("S3_ACCESS_KEY_ID")
    secret = env_optional("S3_SECRET_ACCESS_KEY")
    url_style = env_optional("S3_URL_STYLE")
    session_token = env_optional("S3_SESSION_TOKEN")
    use_ssl = env_bool("S3_USE_SSL", True)

    required = [endpoint, bucket, key_id, secret]
    if all(value is None for value in required):
        print("Skipping S3 bootstrap because no S3 environment variables were provided.", flush=True)
        return

    missing = [
        name
        for name, value in (
            ("S3_ENDPOINT", endpoint),
            ("S3_BUCKET", bucket),
            ("S3_ACCESS_KEY_ID", key_id),
            ("S3_SECRET_ACCESS_KEY", secret),
        )
        if value is None
    ]
    if missing:
        raise ValueError(f"Missing required S3 bootstrap variables: {', '.join(missing)}")

    endpoint, use_ssl = normalize_s3_endpoint(endpoint, use_ssl)

    options = [
        "TYPE s3",
        "PROVIDER config",
        f"KEY_ID {sql_string(key_id)}",
        f"SECRET {sql_string(secret)}",
        f"ENDPOINT {sql_string(endpoint)}",
        f"USE_SSL {'true' if use_ssl else 'false'}",
        f"SCOPE {sql_string(f's3://{bucket}')}",
    ]
    if url_style is not None:
        options.append(f"URL_STYLE {sql_string(url_style)}")
    if session_token is not None:
        options.append(f"SESSION_TOKEN {sql_string(session_token)}")

    conn.execute(f"CREATE OR REPLACE SECRET s3_ui ({', '.join(options)})")
    print(f"Configured DuckDB S3 secret for bucket '{bucket}'.", flush=True)


def create_postgres_secret(
    conn: duckdb.DuckDBPyConnection,
    *,
    prefix: str,
    secret_name: str,
) -> bool:
    host = first_env(f"{prefix}_HOST", "PG_HOST")
    port = first_env(f"{prefix}_PORT", "PG_PORT")
    database = first_env(f"{prefix}_DATABASE", "PGDATABASE")
    user = first_env(f"{prefix}_USER", "PG_USER", "PGUSER")
    password = first_env(f"{prefix}_PASSWORD", "PG_PASSWORD", "PGPASSWORD")

    required = [host, port, database, user, password]
    if all(value is None for value in required):
        print(f"Skipping {secret_name} bootstrap because no {prefix}_* environment variables were provided.", flush=True)
        return False

    missing = [
        name
        for name, value in (
            (f"{prefix}_HOST", host),
            (f"{prefix}_PORT", port),
            (f"{prefix}_DATABASE", database),
            (f"{prefix}_USER", user),
            (f"{prefix}_PASSWORD", password),
        )
        if value is None
    ]
    if missing:
        raise ValueError(f"Missing required PostgreSQL bootstrap variables: {', '.join(missing)}")

    port = normalize_port(f"{prefix}_PORT", port)

    conn.execute(
        "CREATE OR REPLACE SECRET "
        f"{sql_identifier(secret_name)} "
        "("
        "TYPE postgres, "
        f"HOST {sql_string(host)}, "
        f"PORT {port}, "
        f"DATABASE {sql_string(database)}, "
        f"USER {sql_string(user)}, "
        f"PASSWORD {sql_string(password)}"
        ")"
    )
    return True


def attach_postgres_database(
    conn: duckdb.DuckDBPyConnection,
    *,
    alias: str,
    secret_name: str,
    read_only: bool,
) -> None:
    options = [
        "TYPE postgres",
        f"SECRET {sql_identifier(secret_name)}",
    ]
    if read_only:
        options.append("READ_ONLY")

    conn.execute(f"ATTACH '' AS {sql_identifier(alias)} ({', '.join(options)})")
    mode = "read-only" if read_only else "read-write"
    print(f"Attached PostgreSQL database '{alias}' in {mode} mode.", flush=True)


def bootstrap_integrations(conn: duckdb.DuckDBPyConnection) -> None:
    create_s3_secret(conn)
    create_s3_startup_views(conn)

    if create_postgres_secret(conn, prefix="PG_OLTP", secret_name="pg_oltp_secret"):
        attach_postgres_database(conn, alias="pg_oltp", secret_name="pg_oltp_secret", read_only=True)

    if create_postgres_secret(conn, prefix="PG_OLAP", secret_name="pg_olap_secret"):
        attach_postgres_database(conn, alias="pg_olap", secret_name="pg_olap_secret", read_only=False)


def main() -> int:
    database_path = Path(env("DUCKDB_DATABASE", "/tmp/workspace/workspace.duckdb"))
    extension_dir = Path(env("DUCKDB_EXTENSION_DIRECTORY", "/workspace/.duckdb/extensions"))
    ui_port = env("DUCKDB_UI_PORT", "4213")
    extra_extensions = parse_extensions(os.getenv("DUCKDB_EXTRA_EXTENSIONS", ""))

    database_path.parent.mkdir(parents=True, exist_ok=True)
    extension_dir.mkdir(parents=True, exist_ok=True)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print_runtime_diagnostics(
        database_path=database_path,
        extension_dir=extension_dir,
        ui_port=ui_port,
    )
    print(f"Opening DuckDB database at {database_path}.", flush=True)
    conn = duckdb.connect(str(database_path))

    try:
        conn.execute(f"SET extension_directory = {sql_string(extension_dir.as_posix())}")
        configure_s3_tls(conn)

        ensure_extension(conn, "ui")
        for extension in extra_extensions:
            ensure_extension(conn, extension)

        bootstrap_integrations(conn)

        conn.execute(f"SET ui_local_port = {ui_port}")
        conn.execute("CALL start_ui_server()")

        print(
            (
                "DuckDB UI server started. "
                f"Local DuckDB UI port: localhost:{ui_port}. "
                f"Published container port: {ui_port}."
            ),
            flush=True,
        )

        while not STOP:
            time.sleep(1)
    finally:
        conn.close()
        print("DuckDB connection closed.", flush=True)

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(f"Failed to start DuckDB UI: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
