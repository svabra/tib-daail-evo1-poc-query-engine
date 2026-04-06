from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path


WORKBENCH_ENVIRONMENT_VARIABLES = (
    "BDW_ENABLE_FILE_LOGGING",
    "IMAGE_VERSION",
    "PORT",
    "DUCKDB_DATABASE",
    "DUCKDB_EXTENSION_DIRECTORY",
    "MAX_RESULT_ROWS",
    "S3_ENDPOINT",
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
    "S3_ACCESS_KEY_ID",
    "PG_PASSWORD",
    "PGPASSWORD",
    "S3_SECRET_ACCESS_KEY",
    "S3_SESSION_TOKEN",
}

SENSITIVE_ENVIRONMENT_VARIABLE_HINTS = (
    "PASSWORD",
    "SECRET",
    "TOKEN",
    "ACCESS_KEY",
)

STARTUP_REDACTED_FILE_HINTS = (
    "secret",
    "token",
    "password",
    "access-key",
)

STARTUP_CONFIG_SCAN_ROOTS = (
    Path("/myconfigmap"),
    Path("/myconfigmap/daai-brs-d/bit-ros-trusted-certs"),
    Path("/myconfigmap/bit-ros-trusted-certs"),
    Path("/etc/bit/trusted-certs"),
    Path("/vault/secrets"),
    Path("/var/run/secrets"),
    Path("/var/run/configmaps"),
)

PREFERRED_S3_CA_CERT_FILES = (
    Path("/myconfigmap/daai-brs-d/bit-ros-trusted-certs/ca-bundle.crt"),
    Path("/myconfigmap/bit-ros-trusted-certs/ca-bundle.crt"),
    Path("/etc/bit/trusted-certs/ca-bundle.crt"),
)

AUTO_S3_CA_CERT_SEARCH_ROOTS = (
    Path("/myconfigmap/daai-brs-d/bit-ros-trusted-certs"),
    Path("/myconfigmap/bit-ros-trusted-certs"),
    Path("/etc/bit/trusted-certs"),
)

AUTO_S3_CA_CERT_FILE_SUFFIXES = {".crt", ".pem", ".cer"}
AUTO_S3_CA_BUNDLE_PATH = Path("/tmp/bit-data-workbench-s3-ca-bundle.pem")


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
    return format_environment_pair(name, value)


def should_redact_environment_variable(name: str) -> bool:
    normalized_name = name.strip().upper()
    if normalized_name in REDACTED_ENVIRONMENT_VARIABLES:
        return True
    return any(hint in normalized_name for hint in SENSITIVE_ENVIRONMENT_VARIABLE_HINTS)


def format_environment_pair(name: str, value: str | None) -> str:
    if value is None:
        return "null"
    if should_redact_environment_variable(name):
        return "[MASKED]"
    if value == "":
        return '""'
    return value


def _decode_mount_path(raw_value: str) -> str:
    return raw_value.replace("\\040", " ")


def _mountinfo_paths() -> list[Path]:
    mountinfo_path = Path("/proc/self/mountinfo")
    if not mountinfo_path.exists():
        return []

    paths: list[Path] = []
    try:
        lines = mountinfo_path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []

    for line in lines:
        left_side, _separator, _right_side = line.partition(" - ")
        parts = left_side.split()
        if len(parts) < 5:
            continue
        mount_path = Path(_decode_mount_path(parts[4]))
        paths.append(mount_path)
    return paths


def _looks_like_config_mount(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False

    normalized_path = path.as_posix().lower()
    if any(normalized_path.startswith(root.as_posix()) for root in STARTUP_CONFIG_SCAN_ROOTS):
        return True
    if "trusted-certs" in normalized_path:
        return True

    try:
        child_names = {child.name for child in path.iterdir()}
    except OSError:
        return False
    return "..data" in child_names


def _startup_redacted_paths() -> set[str]:
    paths: set[str] = set()
    for env_name in ("S3_ACCESS_KEY_ID_FILE", "S3_SECRET_ACCESS_KEY_FILE", "S3_SESSION_TOKEN_FILE"):
        raw_value = env_optional(env_name)
        if raw_value is not None:
            paths.add(Path(raw_value).as_posix().lower())
    return paths


def _should_redact_startup_file(path: Path) -> bool:
    normalized_path = path.as_posix().lower()
    if normalized_path in _startup_redacted_paths():
        return True
    return any(hint in normalized_path for hint in STARTUP_REDACTED_FILE_HINTS)


def _read_startup_file_lines(path: Path, *, max_bytes: int = 262_144) -> list[str]:
    try:
        raw_value = path.read_bytes()
    except OSError as exc:
        return [f"[unreadable: {exc}]"]

    if len(raw_value) > max_bytes:
        return [f"[content skipped: file is larger than {max_bytes} bytes]"]
    if b"\x00" in raw_value:
        return ["[content skipped: binary file]"]

    try:
        text = raw_value.decode("utf-8")
    except UnicodeDecodeError:
        return ["[content skipped: not valid UTF-8 text]"]

    stripped = text.rstrip("\n")
    if not stripped:
        return ['[empty file]']
    return stripped.splitlines()


def _describe_startup_path(
    path: Path,
    *,
    depth: int = 0,
    max_depth: int = 2,
    include_file_contents: bool = True,
) -> list[str]:
    prefix = "  " * depth
    try:
        is_symlink = path.is_symlink()
    except OSError as exc:
        return [f"{prefix}[unavailable] {path}: {exc}"]

    if is_symlink:
        try:
            target = os.readlink(path)
        except OSError as exc:
            return [f"{prefix}[symlink] {path} -> [unreadable target: {exc}]"]
        lines = [f"{prefix}[symlink] {path} -> {target}"]
        try:
            resolved = path.resolve(strict=True)
        except OSError:
            return lines
        if resolved != path:
            lines.extend(
                _describe_startup_path(
                    resolved,
                    depth=depth + 1,
                    max_depth=max_depth,
                    include_file_contents=include_file_contents,
                )
            )
        return lines

    if path.is_dir():
        lines = [f"{prefix}[dir] {path}"]
        if depth >= max_depth:
            return lines
        try:
            children = sorted(path.iterdir(), key=lambda child: child.name.lower())
        except OSError as exc:
            lines.append(f"{prefix}  [unreadable directory: {exc}]")
            return lines
        for child in children:
            lines.extend(
                _describe_startup_path(
                    child,
                    depth=depth + 1,
                    max_depth=max_depth,
                    include_file_contents=include_file_contents,
                )
            )
        return lines

    if path.is_file():
        try:
            size = path.stat().st_size
        except OSError:
            size = -1
        lines = [f"{prefix}[file] {path} ({size} bytes)"]
        if not include_file_contents:
            return lines
        if _should_redact_startup_file(path):
            lines.append(f"{prefix}  [content redacted]")
            return lines
        for line in _read_startup_file_lines(path):
            lines.append(f"{prefix}  {line}")
        return lines

    return [f"{prefix}[other] {path}"]


def _path_status_summary(path: Path) -> str:
    if not path.exists():
        return "missing"
    if path.is_dir():
        return "directory"
    if path.is_file():
        return "file"
    return "other"


def _visible_directory_entries(path: Path, *, limit: int = 50) -> list[Path]:
    if not path.exists() or not path.is_dir():
        return []
    try:
        entries = sorted(path.iterdir(), key=lambda item: item.name.lower())
    except OSError:
        return []
    return entries[:limit]


def discover_startup_config_lines() -> list[str]:
    lines = [
        "[bdw-startup] Accessible config mounts and files visible inside the container follow.",
        "[bdw-startup] Kubernetes ConfigMaps that are not mounted or injected into this pod will not appear here.",
    ]

    candidate_paths: dict[str, Path] = {}
    for raw_root in STARTUP_CONFIG_SCAN_ROOTS:
        if raw_root.exists():
            candidate_paths[raw_root.as_posix()] = raw_root

    for env_name in ("S3_ACCESS_KEY_ID_FILE", "S3_SECRET_ACCESS_KEY_FILE", "S3_SESSION_TOKEN_FILE", "S3_CA_CERT_FILE"):
        raw_value = env_optional(env_name)
        if raw_value is None:
            continue
        path = Path(raw_value)
        candidate_paths[path.as_posix()] = path
        candidate_paths[path.parent.as_posix()] = path.parent

    for mount_path in _mountinfo_paths():
        if _looks_like_config_mount(mount_path):
            candidate_paths[mount_path.as_posix()] = mount_path

    if not candidate_paths:
        lines.append("[bdw-startup] No configmap-like mount paths are visible from this process.")
        return lines

    ordered_paths = sorted(
        candidate_paths.values(),
        key=lambda item: (len(item.as_posix()), item.as_posix().lower()),
    )
    selected_paths: list[Path] = []
    for path in ordered_paths:
        if any(path != selected and path.is_relative_to(selected) for selected in selected_paths):
            continue
        selected_paths.append(path)

    for path in selected_paths:
        for detail_line in _describe_startup_path(path):
            lines.append(f"[bdw-startup] {detail_line}")
    return lines


def discover_s3_ca_cert_source_files() -> list[Path]:
    discovered: dict[str, Path] = {}
    for root in AUTO_S3_CA_CERT_SEARCH_ROOTS:
        if not root.exists():
            continue
        try:
            candidates = sorted(root.rglob("*"), key=lambda item: item.as_posix().lower())
        except OSError:
            continue
        for candidate in candidates:
            try:
                if not candidate.is_file():
                    continue
            except OSError:
                continue
            if candidate.suffix.lower() not in AUTO_S3_CA_CERT_FILE_SUFFIXES:
                continue
            discovered[candidate.as_posix()] = candidate
    return list(discovered.values())


def discover_preferred_s3_ca_cert_file() -> Path | None:
    for candidate in PREFERRED_S3_CA_CERT_FILES:
        try:
            if candidate.is_file():
                return candidate
        except OSError:
            continue
    return None


def build_s3_ca_bundle(
    source_files: list[Path],
    *,
    destination: Path | None = None,
) -> Path | None:
    bundle_parts: list[str] = []
    for source_file in source_files:
        try:
            content = source_file.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError):
            continue
        if not content:
            continue
        bundle_parts.append(content)

    if not bundle_parts:
        return None

    destination = destination or AUTO_S3_CA_BUNDLE_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text("\n\n".join(bundle_parts) + "\n", encoding="utf-8")
    return destination


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
    _generated_s3_ca_cert_file: Path | None = field(init=False, default=None, repr=False)

    @classmethod
    def from_env(cls) -> "Settings":
        # Runtime selection is intentionally environment-driven instead of using
        # an explicit APP_ENV flag. Local launchers inject localhost endpoints
        # and direct credentials, while the RHOS deployment injects pod metadata,
        # Secret-backed credentials, TLS settings, and cluster endpoints.
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
        # Prefer a direct env value for local/dev and RHOS Secret-backed flows,
        # then fall back to an optional file path if one is configured.
        return resolve_secret_value(
            value=self.s3_access_key_id,
            file_path=self.s3_access_key_id_file,
            variable_name="S3_ACCESS_KEY_ID",
        )

    def current_s3_secret_access_key(self) -> str | None:
        # Prefer a direct env value for local/dev and RHOS Secret-backed flows,
        # then fall back to an optional file path if one is configured.
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

    def effective_s3_ca_cert_file(self) -> Path | None:
        if self.s3_ca_cert_file is not None:
            try:
                if self.s3_ca_cert_file.is_file():
                    return self.s3_ca_cert_file
            except OSError:
                pass

        preferred_file = discover_preferred_s3_ca_cert_file()
        if preferred_file is not None:
            return preferred_file

        if self._generated_s3_ca_cert_file is not None:
            return self._generated_s3_ca_cert_file

        source_files = discover_s3_ca_cert_source_files()
        if len(source_files) == 1:
            return source_files[0]
        generated_bundle = build_s3_ca_bundle(source_files)
        self._generated_s3_ca_cert_file = generated_bundle
        return generated_bundle

    def startup_s3_certificate_lines(self) -> list[str]:
        lines: list[str] = []
        if self.s3_ca_cert_file is not None:
            lines.append(
                "[bdw-startup] Configured S3_CA_CERT_FILE="
                f"{self.s3_ca_cert_file.as_posix()} ({_path_status_summary(self.s3_ca_cert_file)})"
            )

        for root in AUTO_S3_CA_CERT_SEARCH_ROOTS:
            lines.append(
                f"[bdw-startup] Configured S3 configmap mount path {root.as_posix()} ({_path_status_summary(root)})"
            )
            for entry in _visible_directory_entries(root):
                lines.append(
                    f"[bdw-startup] S3 CA search root entry: {entry.as_posix()} ({_path_status_summary(entry)})"
                )
            if root.exists():
                lines.append(
                    f"[bdw-startup] Recursive S3 configmap tree for {root.as_posix()} follows."
                )
                for detail_line in _describe_startup_path(
                    root,
                    max_depth=8,
                    include_file_contents=False,
                ):
                    lines.append(f"[bdw-startup] {detail_line}")

        for candidate in PREFERRED_S3_CA_CERT_FILES:
            lines.append(
                "[bdw-startup] Preferred S3 CA candidate "
                f"{candidate.as_posix()} ({_path_status_summary(candidate)})"
            )

        source_files = discover_s3_ca_cert_source_files()
        if not source_files:
            lines.append("[bdw-startup] No mounted S3 CA certificate files were auto-discovered.")
        else:
            lines.extend(
                f"[bdw-startup] S3 CA source file: {source_file.as_posix()}"
                for source_file in source_files
            )

        effective_bundle = self.effective_s3_ca_cert_file()
        if effective_bundle is None:
            lines.append("[bdw-startup] No effective S3 CA certificate path could be resolved.")
            return lines

        if self.s3_ca_cert_file is not None and effective_bundle == self.s3_ca_cert_file:
            lines.append(
                f"[bdw-startup] Using configured S3 CA certificate file: {effective_bundle.as_posix()}"
            )
            return lines

        if effective_bundle in PREFERRED_S3_CA_CERT_FILES:
            lines.append(
                f"[bdw-startup] Using preferred S3 CA certificate file: {effective_bundle.as_posix()}"
            )
            return lines

        if effective_bundle in source_files:
            lines.append(
                f"[bdw-startup] Using discovered S3 CA certificate file: {effective_bundle.as_posix()}"
            )
            return lines

        lines.append(f"[bdw-startup] Auto-generated S3 CA bundle: {effective_bundle.as_posix()}")
        return lines

    def apply_runtime_environment(self) -> None:
        effective_ca_bundle = self.effective_s3_ca_cert_file()
        if not effective_ca_bundle:
            return

        ca_bundle_path = effective_ca_bundle.as_posix()
        for env_name in ("AWS_CA_BUNDLE", "REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"):
            os.environ[env_name] = ca_bundle_path

    @staticmethod
    def startup_environment_lines() -> list[str]:
        return [
            f"{name}={format_environment_value(name)}"
            for name in WORKBENCH_ENVIRONMENT_VARIABLES
        ]

    @staticmethod
    def startup_all_environment_lines() -> list[str]:
        lines = [
            "[bdw-startup] Full os.environ dump follows. Sensitive values are masked.",
        ]
        lines.extend(
            f"{name}={format_environment_pair(name, value)}"
            for name, value in sorted(os.environ.items(), key=lambda item: item[0].lower())
        )
        return lines

    @staticmethod
    def startup_config_lines() -> list[str]:
        return discover_startup_config_lines()
