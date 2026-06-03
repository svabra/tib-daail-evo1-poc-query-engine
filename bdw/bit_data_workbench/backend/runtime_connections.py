from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb

from ..config import Settings
from .s3_storage import effective_s3_url_style, normalize_s3_endpoint
from .sql_utils import sql_identifier, sql_literal


logger = logging.getLogger(__name__)
DUCKDB_LOCK_RETRY_ATTEMPTS = 20
DUCKDB_LOCK_RETRY_DELAY_SECONDS = 0.5


def duckdb_s3_bootstrap_configured(settings: Settings) -> bool:
    return any(
        (
            settings.s3_endpoint,
            settings.current_s3_access_key_id(),
            settings.current_s3_secret_access_key(),
        )
    )


def duckdb_postgres_bootstrap_configured(settings: Settings) -> bool:
    return all(
        (
            settings.pg_host,
            settings.pg_port,
            settings.pg_user,
            settings.pg_password,
        )
    ) and any((settings.pg_oltp_database, settings.pg_olap_database))


def normalize_port(value: str, variable_name: str) -> str:
    if not value.isdigit():
        raise ValueError(f"{variable_name} must be numeric, got: {value}")
    return value


def normalize_postgres_host(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if normalized in {"localhost", "::1"}:
        return "127.0.0.1"
    return value


def apply_duckdb_runtime_settings(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> dict[str, object]:
    applied: dict[str, object] = {}
    memory_limit = str(getattr(settings, "duckdb_memory_limit", "") or "").strip()
    if memory_limit:
        connection.execute(f"SET memory_limit = {sql_literal(memory_limit)}")
        applied["memoryLimit"] = memory_limit

    threads = getattr(settings, "duckdb_threads", None)
    if threads is not None:
        normalized_threads = max(1, int(threads))
        connection.execute(f"SET threads = {normalized_threads}")
        applied["threads"] = normalized_threads

    preserve_order = getattr(settings, "duckdb_preserve_insertion_order", None)
    if preserve_order is not None:
        connection.execute(
            f"SET preserve_insertion_order = {'true' if bool(preserve_order) else 'false'}"
        )
        applied["preserveInsertionOrder"] = bool(preserve_order)

    temp_directory = getattr(settings, "duckdb_temp_directory", None)
    if temp_directory is not None:
        temp_path = Path(temp_directory)
        temp_path.mkdir(parents=True, exist_ok=True)
        connection.execute(f"SET temp_directory = {sql_literal(temp_path.as_posix())}")
        applied["tempDirectory"] = temp_path.as_posix()

    max_temp_directory_size = str(
        getattr(settings, "duckdb_max_temp_directory_size", "") or ""
    ).strip()
    if max_temp_directory_size:
        connection.execute(
            f"SET max_temp_directory_size = {sql_literal(max_temp_directory_size)}"
        )
        applied["maxTempDirectorySize"] = max_temp_directory_size

    return applied


def _is_duckdb_lock_conflict(exc: duckdb.IOException) -> bool:
    message = str(exc).lower()
    return "being used by another process" in message or "file is already open" in message


def _connect_duckdb_with_lock_retry(
    connection_target: str,
    *,
    read_only: bool,
) -> duckdb.DuckDBPyConnection:
    for attempt in range(1, DUCKDB_LOCK_RETRY_ATTEMPTS + 1):
        try:
            return duckdb.connect(connection_target, read_only=read_only)
        except duckdb.IOException as exc:
            if not _is_duckdb_lock_conflict(exc) or attempt >= DUCKDB_LOCK_RETRY_ATTEMPTS:
                raise
            logger.warning(
                "DuckDB worker connection is waiting for the database file lock to clear "
                "(attempt %d/%d, path=%s).",
                attempt,
                DUCKDB_LOCK_RETRY_ATTEMPTS,
                connection_target,
            )
            time.sleep(DUCKDB_LOCK_RETRY_DELAY_SECONDS)

    raise RuntimeError("DuckDB worker connection retry loop exited unexpectedly.")


def create_duckdb_worker_connection(
    settings: Settings,
    *,
    database_path: Path | str | None = None,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    target_database = database_path or settings.duckdb_database
    if isinstance(target_database, Path):
        target_path = target_database
        target_path.parent.mkdir(parents=True, exist_ok=True)
        connection_target = str(target_path)
    else:
        normalized_target = str(target_database).strip()
        if not normalized_target:
            target_path = settings.duckdb_database
            target_path.parent.mkdir(parents=True, exist_ok=True)
            connection_target = str(target_path)
        else:
            if normalized_target != ":memory:":
                Path(normalized_target).parent.mkdir(parents=True, exist_ok=True)
            connection_target = normalized_target
    settings.duckdb_extension_directory.mkdir(parents=True, exist_ok=True)

    connection = _connect_duckdb_with_lock_retry(connection_target, read_only=read_only)
    apply_duckdb_runtime_settings(connection, settings)
    _configure_s3_tls(connection, settings)
    connection.execute(
        f"SET extension_directory = {sql_literal(settings.duckdb_extension_directory.as_posix())}"
    )
    if duckdb_s3_bootstrap_configured(settings):
        _ensure_extension(connection, "httpfs")
        _bootstrap_s3(connection, settings)
    if duckdb_postgres_bootstrap_configured(settings):
        _ensure_extension(connection, "postgres")
        _bootstrap_postgres(connection, settings, read_only=read_only)
    return connection


def open_postgres_native_connection(settings: Settings, target: str):
    normalized_target = str(target).strip().lower()
    if normalized_target == "oltp":
        database = settings.pg_oltp_database
    elif normalized_target == "olap":
        database = settings.pg_olap_database
    else:
        raise ValueError(f"Unsupported PostgreSQL native target: {target}")

    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "psycopg is required for native PostgreSQL query execution."
        ) from exc

    if not all(
        (
            settings.pg_host,
            settings.pg_port,
            settings.pg_user,
            settings.pg_password,
            database,
        )
    ):
        raise RuntimeError(
            "PostgreSQL native execution requires PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, "
            "and the target database to be configured."
        )

    return psycopg.connect(
        host=normalize_postgres_host(settings.pg_host),
        port=int(settings.pg_port),
        user=settings.pg_user,
        password=settings.pg_password,
        dbname=database,
        autocommit=True,
        connect_timeout=10,
    )


def _configure_s3_tls(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> None:
    connection.execute(
        f"SET enable_server_cert_verification = {'true' if settings.s3_verify_ssl else 'false'}"
    )
    effective_ca_bundle = settings.effective_s3_ca_cert_file()
    if effective_ca_bundle is not None:
        connection.execute(
            f"SET ca_cert_file = {sql_literal(effective_ca_bundle.as_posix())}"
        )


def _ensure_extension(connection: duckdb.DuckDBPyConnection, extension: str) -> None:
    try:
        connection.execute(f"LOAD {extension}")
    except duckdb.Error:
        connection.execute(f"INSTALL {extension}")
        connection.execute(f"LOAD {extension}")


def _bootstrap_s3(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
) -> None:
    access_key_id = settings.current_s3_access_key_id()
    secret_access_key = settings.current_s3_secret_access_key()
    required_values = (
        settings.s3_endpoint,
        access_key_id,
        secret_access_key,
    )
    if not any(required_values):
        return

    missing = [
        name
        for name, value in (
            ("S3_ENDPOINT", settings.s3_endpoint),
            ("S3_ACCESS_KEY_ID", access_key_id),
            ("S3_SECRET_ACCESS_KEY", secret_access_key),
        )
        if value is None
    ]
    if missing:
        raise ValueError(
            "S3 worker connection bootstrap skipped because required variables are missing: "
            f"{', '.join(missing)}"
        )

    endpoint, use_ssl, _transport_reason = normalize_s3_endpoint(
        settings.s3_endpoint,
        use_ssl=settings.s3_use_ssl,
        verify_ssl=settings.s3_verify_ssl,
    )
    connection.execute(
        f"CREATE OR REPLACE SECRET bdw_s3 ({', '.join(_s3_secret_options(settings, endpoint=endpoint, use_ssl=use_ssl))})"
    )


def _s3_secret_options(
    settings: Settings,
    *,
    endpoint: str,
    use_ssl: bool,
    url_style: str | None = None,
) -> list[str]:
    effective_url_style = effective_s3_url_style(
        settings,
        endpoint=endpoint,
        use_ssl=use_ssl,
        explicit_url_style=url_style,
    )
    access_key_id = settings.current_s3_access_key_id()
    secret_access_key = settings.current_s3_secret_access_key()
    session_token = settings.current_s3_session_token()
    if access_key_id is None or secret_access_key is None:
        raise ValueError(
            "S3 credentials are incomplete. Configure access key and secret key values or file paths."
        )
    options = [
        "TYPE s3",
        "PROVIDER config",
        f"KEY_ID {sql_literal(access_key_id)}",
        f"SECRET {sql_literal(secret_access_key)}",
        f"ENDPOINT {sql_literal(endpoint)}",
        f"USE_SSL {'true' if use_ssl else 'false'}",
    ]
    if effective_url_style:
        options.append(f"URL_STYLE {sql_literal(effective_url_style)}")
    if session_token:
        options.append(f"SESSION_TOKEN {sql_literal(session_token)}")
    return options


def _bootstrap_postgres(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    read_only: bool = False,
) -> None:
    if not all(
        (
            settings.pg_host,
            settings.pg_port,
            settings.pg_user,
            settings.pg_password,
        )
    ):
        return

    port = normalize_port(settings.pg_port, "PG_PORT")
    host = normalize_postgres_host(settings.pg_host)

    if settings.pg_oltp_database:
        _create_postgres_secret(
            connection,
            settings,
            secret_name="bdw_pg_oltp",
            database=settings.pg_oltp_database,
            host=host,
            port=port,
        )
        _attach_postgres(
            connection,
            alias="pg_oltp",
            secret_name="bdw_pg_oltp",
            read_only=read_only,
        )

    if settings.pg_olap_database:
        _create_postgres_secret(
            connection,
            settings,
            secret_name="bdw_pg_olap",
            database=settings.pg_olap_database,
            host=host,
            port=port,
        )
        _attach_postgres(
            connection,
            alias="pg_olap",
            secret_name="bdw_pg_olap",
            read_only=read_only,
        )


def _create_postgres_secret(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    *,
    secret_name: str,
    database: str,
    host: str | None,
    port: str,
) -> None:
    connection.execute(
        "CREATE OR REPLACE SECRET "
        f"{sql_identifier(secret_name)} "
        "("
        "TYPE postgres, "
        f"HOST {sql_literal(host or settings.pg_host or '')}, "
        f"PORT {port}, "
        f"DATABASE {sql_literal(database)}, "
        f"USER {sql_literal(settings.pg_user)}, "
        f"PASSWORD {sql_literal(settings.pg_password)}"
        ")"
    )


def _attach_postgres(
    connection: duckdb.DuckDBPyConnection,
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

    connection.execute(
        f"ATTACH OR REPLACE '' AS {sql_identifier(alias)} ({', '.join(options)})"
    )
