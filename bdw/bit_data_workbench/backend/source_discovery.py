from __future__ import annotations

import fnmatch
import hashlib
import logging
import re
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any, Callable
from urllib.parse import urlparse

import duckdb

from ..config import Settings
from ..models import DataSourceDiscoveryEventDefinition, SourceConnectionStatus
from .s3_storage import iter_s3_keys, list_s3_buckets, s3_bucket_schema_name, s3_client


logger = logging.getLogger(__name__)
SUPPORTED_DISCOVERED_S3_FORMATS = {"parquet", "csv", "json"}
MAX_SOURCE_EVENT_HISTORY = 40


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def qualified_name(*parts: str) -> str:
    return ".".join(sql_identifier(part) for part in parts)


def infer_s3_view_format(path: str) -> str:
    lowered = path.lower()
    if ".parquet" in lowered:
        return "parquet"
    if ".csv" in lowered or ".tsv" in lowered:
        return "csv"
    if ".json" in lowered or ".jsonl" in lowered or ".ndjson" in lowered:
        return "json"
    raise ValueError(
        "Could not infer the S3 startup view format from the configured path. "
        "Use 'view_name=format:s3://bucket/path'."
    )


def parse_s3_startup_views(raw_value: str | None) -> list[tuple[str, str, str]]:
    if not raw_value:
        return []

    entries: list[tuple[str, str, str]] = []
    for item in re.split(r"[;\r\n]+", raw_value):
        entry = item.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise ValueError(
                "Each S3 startup view entry must use 'view_name=s3://bucket/path' "
                "or 'view_name=format:s3://bucket/path'."
            )

        view_name, spec = entry.split("=", 1)
        view_name = view_name.strip()
        spec = spec.strip()
        if not view_name or not spec:
            raise ValueError(f"Invalid S3 startup view entry: {entry}")

        prefix = spec.split(":", 1)[0].lower()
        if prefix in SUPPORTED_DISCOVERED_S3_FORMATS and ":" in spec:
            data_format, path = spec.split(":", 1)
            entries.append((view_name, data_format.strip().lower(), path.strip()))
        else:
            entries.append((view_name, infer_s3_view_format(spec), spec))

    return entries


def build_s3_query(data_format: str, path: str) -> str:
    if data_format == "parquet":
        return f"SELECT * FROM read_parquet({sql_literal(path)})"
    if data_format == "csv":
        return f"SELECT * FROM read_csv_auto({sql_literal(path)})"
    if data_format == "json":
        return f"SELECT * FROM read_json_auto({sql_literal(path)})"
    raise ValueError(f"Unsupported S3 discovery format: {data_format}")


def parse_s3_path(path: str) -> tuple[str, str]:
    parsed = urlparse(path)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Unsupported S3 path: {path}")
    return parsed.netloc, parsed.path.lstrip("/")


def key_exists(key_pattern: str, keys: set[str]) -> bool:
    if not key_pattern:
        return False
    if any(token in key_pattern for token in "*?["):
        return any(fnmatch.fnmatchcase(key, key_pattern) for key in keys)
    return key_pattern in keys


def infer_key_format(key: str) -> str | None:
    lowered = key.lower()
    if lowered.endswith(".parquet"):
        return "parquet"
    if lowered.endswith(".csv") or lowered.endswith(".tsv"):
        return "csv"
    if lowered.endswith(".json") or lowered.endswith(".jsonl") or lowered.endswith(".ndjson"):
        return "json"
    return None


def sanitize_relation_name(raw_value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_]+", "_", raw_value).strip("_").lower()
    if not normalized:
        normalized = "s3_object"
    if normalized[0].isdigit():
        normalized = f"s3_{normalized}"
    if len(normalized) <= 56:
        return normalized
    suffix = hashlib.sha1(raw_value.encode("utf-8")).hexdigest()[:8]
    return f"{normalized[:47].rstrip('_')}_{suffix}"


def choose_unique_relation_name(
    preferred_name: str,
    *,
    source_hint: str,
    used_names: set[str],
) -> str:
    normalized_preferred = sanitize_relation_name(preferred_name)
    if normalized_preferred not in used_names:
        used_names.add(normalized_preferred)
        return normalized_preferred

    expanded_name = sanitize_relation_name(source_hint)
    if expanded_name not in used_names:
        used_names.add(expanded_name)
        return expanded_name

    suffix = hashlib.sha1(source_hint.encode("utf-8")).hexdigest()[:8]
    fallback = sanitize_relation_name(f"{normalized_preferred}_{suffix}")
    used_names.add(fallback)
    return fallback


@dataclass(frozen=True, slots=True)
class DiscoveredRelationSpec:
    schema_name: str
    relation_name: str
    query_sql: str
    object_path: str
    object_format: str


@dataclass(slots=True)
class DataSourceDiscoveryResult:
    source_type: str
    source_id: str
    source_label: str
    added_relations: list[str]
    removed_relations: list[str]
    updated_relations: list[str]
    message: str
    metadata_changed: bool = False
    connection_status: SourceConnectionStatus | None = None

    @property
    def has_relation_changes(self) -> bool:
        return bool(
            self.metadata_changed
            or self.added_relations
            or self.removed_relations
            or self.updated_relations
        )

    @property
    def has_changes(self) -> bool:
        return self.has_relation_changes or self.connection_status is not None


class DataSourceDiscoverer(ABC):
    source_type = "unknown"
    source_id = "unknown"
    source_label = "Unknown"
    poll_interval_seconds = 5.0
    manual_connection_control = False

    @abstractmethod
    def sync(self, connection: duckdb.DuckDBPyConnection) -> DataSourceDiscoveryResult | None:
        raise NotImplementedError

    def connect(self) -> DataSourceDiscoveryResult | None:
        raise RuntimeError(f"{self.source_label} does not support manual connection control.")

    def disconnect(self) -> DataSourceDiscoveryResult | None:
        raise RuntimeError(f"{self.source_label} does not support manual connection control.")


@dataclass(frozen=True, slots=True)
class SqlDiscoveredRelation:
    schema_name: str
    relation_name: str
    relation_kind: str

    @property
    def relation_id(self) -> str:
        return f"{self.schema_name}.{self.relation_name}"


class SqlSourceDiscoverer(DataSourceDiscoverer):
    source_type = "sql"
    poll_interval_seconds = 5.0
    manual_connection_control = True

    def __init__(
        self,
        *,
        source_id: str,
        source_label: str,
        snapshot_provider: Callable[[], tuple[SourceConnectionStatus, list[SqlDiscoveredRelation]]],
        disconnect_handler: Callable[[], None] | None = None,
    ) -> None:
        self.source_id = source_id
        self.source_label = source_label
        self._snapshot_provider = snapshot_provider
        self._disconnect_handler = disconnect_handler
        self._current_relations: dict[str, SqlDiscoveredRelation] = {}
        self._lock = threading.RLock()
        self._enabled = True

    def connect(self) -> DataSourceDiscoveryResult | None:
        with self._lock:
            self._enabled = True
        return self.sync(None)

    def disconnect(self) -> DataSourceDiscoveryResult | None:
        with self._lock:
            self._enabled = False
        if self._disconnect_handler is not None:
            self._disconnect_handler()
        return DataSourceDiscoveryResult(
            source_type=self.source_type,
            source_id=self.source_id,
            source_label=self.source_label,
            added_relations=[],
            removed_relations=[],
            updated_relations=[],
            message=f"{self.source_label} disconnected.",
            connection_status=SourceConnectionStatus(
                source_id=self.source_id,
                state="disconnected",
                label="Disconnected",
                detail=f"{self.source_label} live discovery is disconnected.",
                checked_at=utc_now_iso(),
            ),
        )

    def sync(self, connection: duckdb.DuckDBPyConnection) -> DataSourceDiscoveryResult | None:
        del connection
        with self._lock:
            enabled = self._enabled
        if not enabled:
            return DataSourceDiscoveryResult(
                source_type=self.source_type,
                source_id=self.source_id,
                source_label=self.source_label,
                added_relations=[],
                removed_relations=[],
                updated_relations=[],
                message=f"{self.source_label} discovery is disconnected.",
                connection_status=SourceConnectionStatus(
                    source_id=self.source_id,
                    state="disconnected",
                    label="Disconnected",
                    detail=f"{self.source_label} live discovery is disconnected.",
                    checked_at=utc_now_iso(),
                ),
            )

        connection_status, relations = self._snapshot_provider()

        if connection_status.state != "connected":
            return DataSourceDiscoveryResult(
                source_type=self.source_type,
                source_id=self.source_id,
                source_label=self.source_label,
                added_relations=[],
                removed_relations=[],
                updated_relations=[],
                message=f"{self.source_label} connection checked.",
                connection_status=connection_status,
            )

        next_relations = {relation.relation_id: relation for relation in relations}
        with self._lock:
            removed_relation_ids = [
                relation_id
                for relation_id in self._current_relations
                if relation_id not in next_relations
            ]
            added_relation_ids = [
                relation_id
                for relation_id in next_relations
                if relation_id not in self._current_relations
            ]
            updated_relation_ids = [
                relation_id
                for relation_id, relation in next_relations.items()
                if relation_id in self._current_relations and self._current_relations[relation_id] != relation
            ]

            if not added_relation_ids and not removed_relation_ids and not updated_relation_ids:
                return DataSourceDiscoveryResult(
                    source_type=self.source_type,
                    source_id=self.source_id,
                    source_label=self.source_label,
                    added_relations=[],
                    removed_relations=[],
                    updated_relations=[],
                    message=f"{self.source_label} connection checked.",
                    connection_status=connection_status,
                )

            self._current_relations = next_relations
        return DataSourceDiscoveryResult(
            source_type=self.source_type,
            source_id=self.source_id,
            source_label=self.source_label,
            added_relations=[f"{self.source_id}.{relation_id}" for relation_id in added_relation_ids],
            removed_relations=[f"{self.source_id}.{relation_id}" for relation_id in removed_relation_ids],
            updated_relations=[f"{self.source_id}.{relation_id}" for relation_id in updated_relation_ids],
            message=self._build_message(
                added_count=len(added_relation_ids),
                removed_count=len(removed_relation_ids),
                updated_count=len(updated_relation_ids),
            ),
            connection_status=connection_status,
        )

    def _build_message(
        self,
        *,
        added_count: int,
        removed_count: int,
        updated_count: int,
    ) -> str:
        fragments: list[str] = []
        if added_count:
            fragments.append(f"added {added_count}")
        if updated_count:
            fragments.append(f"updated {updated_count}")
        if removed_count:
            fragments.append(f"removed {removed_count}")
        summary = ", ".join(fragments) if fragments else "no visible changes"
        return f"{self.source_label} discovery {summary} relation(s)."


class S3DataSourceDiscoverer(DataSourceDiscoverer):
    source_type = "s3"
    source_id = "workspace.s3"
    source_label = "MinIO / S3"
    poll_interval_seconds = 2.0
    manual_connection_control = True

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._startup_views = parse_s3_startup_views(settings.s3_startup_views)
        self._current_specs: dict[str, DiscoveredRelationSpec] = {}
        self._current_buckets: set[str] = set()
        self._lock = threading.RLock()
        self._enabled = True

    def connect(self) -> DataSourceDiscoveryResult | None:
        with self._lock:
            self._enabled = True
        return DataSourceDiscoveryResult(
            source_type=self.source_type,
            source_id=self.source_id,
            source_label=self.source_label,
            added_relations=[],
            removed_relations=[],
            updated_relations=[],
            message=f"{self.source_label} connected.",
            connection_status=self._connected_status("MinIO / S3 live discovery is connected."),
        )

    def disconnect(self) -> DataSourceDiscoveryResult | None:
        with self._lock:
            self._enabled = False
        return DataSourceDiscoveryResult(
            source_type=self.source_type,
            source_id=self.source_id,
            source_label=self.source_label,
            added_relations=[],
            removed_relations=[],
            updated_relations=[],
            message=f"{self.source_label} disconnected.",
            connection_status=self._disconnected_status("MinIO / S3 live discovery is disconnected."),
        )

    def sync(self, connection: duckdb.DuckDBPyConnection) -> DataSourceDiscoveryResult | None:
        with self._lock:
            enabled = self._enabled
        if not enabled:
            return DataSourceDiscoveryResult(
                source_type=self.source_type,
                source_id=self.source_id,
                source_label=self.source_label,
                added_relations=[],
                removed_relations=[],
                updated_relations=[],
                message=f"{self.source_label} discovery is disconnected.",
                connection_status=self._disconnected_status("MinIO / S3 live discovery is disconnected."),
            )

        if not self._is_configured():
            return DataSourceDiscoveryResult(
                source_type=self.source_type,
                source_id=self.source_id,
                source_label=self.source_label,
                added_relations=[],
                removed_relations=[],
                updated_relations=[],
                message=f"{self.source_label} is not configured.",
                connection_status=self._disconnected_status("MinIO / S3 is not configured."),
            )

        try:
            client = s3_client(self._settings)
            current_buckets = set(list_s3_buckets(self._settings))
            configured_bucket = (self._settings.s3_bucket or "").strip()

            if configured_bucket and configured_bucket not in current_buckets:
                fallback_probe = None
                try:
                    client.head_bucket(Bucket=configured_bucket)
                    fallback_probe = "head_bucket"
                except Exception:
                    try:
                        client.list_objects_v2(Bucket=configured_bucket, MaxKeys=1)
                        fallback_probe = "list_objects_v2"
                    except Exception:
                        fallback_probe = None

                if fallback_probe is not None:
                    current_buckets.add(configured_bucket)
                    logger.info(
                        "S3 discovery added configured bucket %r via %s fallback after bucket enumeration returned %d bucket(s).",
                        configured_bucket,
                        fallback_probe,
                        len(current_buckets) - 1,
                    )
        except Exception as exc:
            configured_bucket = (self._settings.s3_bucket or "").strip()
            if configured_bucket:
                try:
                    client = s3_client(self._settings)
                    client.head_bucket(Bucket=configured_bucket)
                    current_buckets = {configured_bucket}
                    logger.info(
                        "S3 discovery fell back to configured bucket %r because list_buckets failed: %s",
                        configured_bucket,
                        exc,
                    )
                except Exception as head_exc:
                    try:
                        client.list_objects_v2(Bucket=configured_bucket, MaxKeys=1)
                        current_buckets = {configured_bucket}
                        logger.info(
                            "S3 discovery fell back to configured bucket %r via list_objects_v2 because list_buckets failed: %s",
                            configured_bucket,
                            exc,
                        )
                    except Exception as list_exc:
                        return DataSourceDiscoveryResult(
                            source_type=self.source_type,
                            source_id=self.source_id,
                            source_label=self.source_label,
                            added_relations=[],
                            removed_relations=[],
                            updated_relations=[],
                            message=f"{self.source_label} connection failed.",
                            connection_status=self._disconnected_status(
                                f"{self.source_label} connection failed: list_buckets={exc}; "
                                f"head_bucket={head_exc}; list_objects_v2={list_exc}"
                            ),
                        )
            else:
                return DataSourceDiscoveryResult(
                    source_type=self.source_type,
                    source_id=self.source_id,
                    source_label=self.source_label,
                    added_relations=[],
                    removed_relations=[],
                    updated_relations=[],
                    message=f"{self.source_label} connection failed.",
                    connection_status=self._disconnected_status(
                        f"{self.source_label} connection failed: {exc}"
                    ),
                )

        if not self._current_specs:
            self._current_specs = self._load_existing_specs(connection)
        if not self._current_buckets:
            self._current_buckets = set(current_buckets)

        desired_specs = self._build_desired_specs(client, current_buckets)
        removed_names = [name for name in self._current_specs if name not in desired_specs]
        added_names = [name for name in desired_specs if name not in self._current_specs]
        updated_names = [
            name
            for name, spec in desired_specs.items()
            if name in self._current_specs and self._current_specs[name] != spec
        ]
        added_buckets = sorted(current_buckets - self._current_buckets)
        removed_buckets = sorted(self._current_buckets - current_buckets)
        if not removed_names and not added_names and not updated_names and not added_buckets and not removed_buckets:
            return DataSourceDiscoveryResult(
                source_type=self.source_type,
                source_id=self.source_id,
                source_label=self.source_label,
                added_relations=[],
                removed_relations=[],
                updated_relations=[],
                message=f"{self.source_label} connection checked.",
                connection_status=self._connected_status(f"{self.source_label} is connected."),
            )

        for schema_name in sorted({spec.schema_name for spec in desired_specs.values()}):
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(schema_name)}")

        next_specs = dict(self._current_specs)
        successful_added: list[str] = []
        successful_removed: list[str] = []
        successful_updated: list[str] = []

        for spec_key in removed_names:
            current_spec = self._current_specs.get(spec_key)
            if current_spec is None:
                continue
            try:
                connection.execute(
                    f"DROP VIEW IF EXISTS {qualified_name(current_spec.schema_name, current_spec.relation_name)}"
                )
            except duckdb.Error as exc:
                logger.warning(
                    "Failed to drop stale discovered S3 view '%s.%s': %s",
                    current_spec.schema_name,
                    current_spec.relation_name,
                    exc,
                )
                continue
            next_specs.pop(spec_key, None)
            successful_removed.append(f"{current_spec.schema_name}.{current_spec.relation_name}")

        for spec_key in added_names + updated_names:
            spec = desired_specs[spec_key]
            try:
                connection.execute(
                    "CREATE OR REPLACE VIEW "
                    f"{qualified_name(spec.schema_name, spec.relation_name)} "
                    f"AS {spec.query_sql}"
                )
            except duckdb.Error as exc:
                logger.warning(
                    "Skipping discovered S3 object '%s' for relation '%s.%s': %s",
                    spec.object_path,
                    spec.schema_name,
                    spec.relation_name,
                    exc,
                )
                continue

            next_specs[spec_key] = spec
            qualified_relation = f"{spec.schema_name}.{spec.relation_name}"
            if spec_key in self._current_specs:
                successful_updated.append(qualified_relation)
            else:
                successful_added.append(qualified_relation)

        if (
            not successful_removed
            and not successful_added
            and not successful_updated
            and not added_buckets
            and not removed_buckets
        ):
            return DataSourceDiscoveryResult(
                source_type=self.source_type,
                source_id=self.source_id,
                source_label=self.source_label,
                added_relations=[],
                removed_relations=[],
                updated_relations=[],
                message=f"{self.source_label} connection checked.",
                connection_status=self._connected_status(f"{self.source_label} is connected."),
            )

        self._current_specs = next_specs
        self._current_buckets = set(current_buckets)
        return DataSourceDiscoveryResult(
            source_type=self.source_type,
            source_id=self.source_id,
            source_label=self.source_label,
            added_relations=successful_added,
            removed_relations=successful_removed,
            updated_relations=successful_updated,
            message=self._build_message(
                added_buckets=added_buckets,
                removed_buckets=removed_buckets,
                added_relations=successful_added,
                removed_relations=successful_removed,
                updated_relations=successful_updated,
            ),
            metadata_changed=bool(added_buckets or removed_buckets),
            connection_status=self._connected_status(f"{self.source_label} is connected."),
        )

    def _is_configured(self) -> bool:
        return all(
            (
                self._settings.s3_endpoint,
                self._settings.s3_bucket,
                self._settings.current_s3_access_key_id(),
                self._settings.current_s3_secret_access_key(),
            )
        )

    def _connected_status(self, detail: str) -> SourceConnectionStatus:
        return SourceConnectionStatus(
            source_id=self.source_id,
            state="connected",
            label="Connected",
            detail=detail,
            checked_at=utc_now_iso(),
        )

    def _disconnected_status(self, detail: str) -> SourceConnectionStatus:
        return SourceConnectionStatus(
            source_id=self.source_id,
            state="disconnected",
            label="Disconnected",
            detail=detail,
            checked_at=utc_now_iso(),
        )

    def _load_existing_specs(
        self,
        connection: duckdb.DuckDBPyConnection,
    ) -> dict[str, DiscoveredRelationSpec]:
        rows = connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_catalog NOT IN ('pg_oltp', 'pg_olap')
            ORDER BY table_schema, table_name
            """
        ).fetchall()
        return {
            f"{str(table_schema)}.{str(table_name)}": DiscoveredRelationSpec(
                schema_name=str(table_schema),
                relation_name=str(table_name),
                query_sql="",
                object_path="",
                object_format="",
            )
            for table_schema, table_name in rows
        }

    def _build_desired_specs(
        self,
        client,
        buckets: set[str],
    ) -> dict[str, DiscoveredRelationSpec]:
        desired_specs: dict[str, DiscoveredRelationSpec] = {}
        for bucket in sorted(buckets):
            schema_name = s3_bucket_schema_name(bucket)
            used_names: set[str] = set()
            keys = sorted(set(iter_s3_keys(client, bucket)))
            key_set = set(keys)

            for view_name, data_format, path in self._startup_views:
                try:
                    view_bucket, key_pattern = parse_s3_path(path)
                except ValueError:
                    logger.warning("Skipping unsupported S3 startup path '%s'.", path)
                    continue
                if view_bucket != bucket:
                    continue
                if not key_exists(key_pattern, key_set):
                    continue

                relation_name = choose_unique_relation_name(
                    view_name,
                    source_hint=view_name,
                    used_names=used_names,
                )
                desired_specs[f"{schema_name}.{relation_name}"] = DiscoveredRelationSpec(
                    schema_name=schema_name,
                    relation_name=relation_name,
                    query_sql=build_s3_query(data_format, path),
                    object_path=path,
                    object_format=data_format,
                )

            generated_datasets = sorted(
                {
                    key.split("/", 2)[1]
                    for key in key_set
                    if key.startswith("generated/")
                    and key.lower().endswith(".parquet")
                    and len(key.split("/", 2)) >= 3
                }
            )
            for dataset_name in generated_datasets:
                relation_name = choose_unique_relation_name(
                    dataset_name,
                    source_hint=f"generated_{dataset_name}",
                    used_names=used_names,
                )
                object_path = f"s3://{bucket}/generated/{dataset_name}/*.parquet"
                desired_specs[f"{schema_name}.{relation_name}"] = DiscoveredRelationSpec(
                    schema_name=schema_name,
                    relation_name=relation_name,
                    query_sql=build_s3_query("parquet", object_path),
                    object_path=object_path,
                    object_format="parquet",
                )

            startup_paths = {
                parse_s3_path(path)[1]
                for _view_name, _data_format, path in self._startup_views
                if path.startswith(f"s3://{bucket}/")
            }
            for key in keys:
                if key in startup_paths:
                    continue
                if key.startswith("generated/"):
                    continue

                data_format = infer_key_format(key)
                if data_format is None:
                    continue

                relation_name = choose_unique_relation_name(
                    PurePosixPath(key).stem,
                    source_hint=PurePosixPath(key).with_suffix("").as_posix(),
                    used_names=used_names,
                )
                object_path = f"s3://{bucket}/{key}"
                desired_specs[f"{schema_name}.{relation_name}"] = DiscoveredRelationSpec(
                    schema_name=schema_name,
                    relation_name=relation_name,
                    query_sql=build_s3_query(data_format, object_path),
                    object_path=object_path,
                    object_format=data_format,
                )

        return desired_specs

    def _build_message(
        self,
        *,
        added_buckets: list[str],
        removed_buckets: list[str],
        added_relations: list[str],
        removed_relations: list[str],
        updated_relations: list[str],
    ) -> str:
        fragments: list[str] = []
        if added_buckets:
            fragments.append(f"buckets added {len(added_buckets)}")
        if removed_buckets:
            fragments.append(f"buckets removed {len(removed_buckets)}")
        if added_relations:
            fragments.append(f"added {len(added_relations)}")
        if updated_relations:
            fragments.append(f"updated {len(updated_relations)}")
        if removed_relations:
            fragments.append(f"removed {len(removed_relations)}")
        summary = ", ".join(fragments) if fragments else "no visible changes"
        return f"S3 discovery {summary} relation(s)."


class DataSourceDiscoveryManager:
    def __init__(
        self,
        *,
        connection_factory: Callable[[], duckdb.DuckDBPyConnection],
        metadata_refresher: Callable[[], None],
        discoverers: list[DataSourceDiscoverer],
    ) -> None:
        self._connection_factory = connection_factory
        self._metadata_refresher = metadata_refresher
        self._discoverers = list(discoverers)
        self._lock = threading.RLock()
        self._condition = threading.Condition(self._lock)
        self._events: list[DataSourceDiscoveryEventDefinition] = []
        self._source_statuses: dict[str, SourceConnectionStatus] = {}
        self._discoverers_by_source_id = {
            discoverer.source_id: discoverer
            for discoverer in self._discoverers
            if getattr(discoverer, "source_id", "")
        }
        self._state_version = 0
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []

    def start(self) -> None:
        with self._condition:
            if self._threads:
                return
            self._stop_event.clear()
        logger.info(
            "Starting data source discovery manager with %d discoverer(s); initial sync will run in background threads.",
            len(self._discoverers),
        )

        threads: list[threading.Thread] = []
        for discoverer in self._discoverers:
            worker = threading.Thread(
                target=self._run_discoverer,
                args=(discoverer,),
                daemon=True,
                name=f"bdw-source-discovery-{discoverer.source_type}",
            )
            worker.start()
            threads.append(worker)
            logger.info(
                "Started data source discovery thread %s for source_id=%s",
                worker.name,
                getattr(discoverer, "source_id", discoverer.source_type),
            )

        with self._condition:
            self._threads = threads

    def stop(self) -> None:
        with self._condition:
            threads = list(self._threads)
            self._threads = []
            self._stop_event.set()
            self._condition.notify_all()

        for thread in threads:
            thread.join(timeout=3.0)

    def state_payload(self) -> dict[str, Any]:
        with self._condition:
            return self._state_payload_locked()

    def source_statuses(self) -> dict[str, SourceConnectionStatus]:
        with self._condition:
            return dict(self._source_statuses)

    def supports_manual_connection_control(self, source_id: str) -> bool:
        discoverer = self._discoverers_by_source_id.get(source_id)
        return bool(discoverer and getattr(discoverer, "manual_connection_control", False))

    def connect_source(self, source_id: str) -> dict[str, Any]:
        discoverer = self._discoverers_by_source_id.get(source_id)
        if discoverer is None or not getattr(discoverer, "manual_connection_control", False):
            raise KeyError(f"Unknown data source: {source_id}")
        result = discoverer.connect()
        self._apply_result(result, emit_event=False)
        return self.state_payload()

    def disconnect_source(self, source_id: str) -> dict[str, Any]:
        discoverer = self._discoverers_by_source_id.get(source_id)
        if discoverer is None or not getattr(discoverer, "manual_connection_control", False):
            raise KeyError(f"Unknown data source: {source_id}")
        result = discoverer.disconnect()
        self._apply_result(result, emit_event=False)
        return self.state_payload()

    def wait_for_state(self, last_version: int | None, timeout: float = 15.0) -> dict[str, Any]:
        with self._condition:
            if last_version is None or last_version != self._state_version:
                return self._state_payload_locked()

            self._condition.wait_for(lambda: self._state_version != last_version, timeout=timeout)
            return self._state_payload_locked()

    def _run_discoverer(self, discoverer: DataSourceDiscoverer) -> None:
        while not self._stop_event.is_set():
            self._sync_discoverer(discoverer, emit_event=True)
            self._stop_event.wait(discoverer.poll_interval_seconds)

    def _sync_discoverer(
        self,
        discoverer: DataSourceDiscoverer,
        *,
        emit_event: bool,
    ) -> None:
        connection: duckdb.DuckDBPyConnection | None = None
        try:
            connection = self._connection_factory()
            result = discoverer.sync(connection)
        except Exception as exc:
            logger.warning(
                "Data source discovery failed for '%s': %s",
                discoverer.source_type,
                exc,
            )
            return
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

        if result is None or not result.has_changes:
            return

        self._apply_result(result, emit_event=emit_event)

    def _apply_result(
        self,
        result: DataSourceDiscoveryResult | None,
        *,
        emit_event: bool,
    ) -> None:
        if result is None or not result.has_changes:
            return

        status_changed = False
        if result.connection_status is not None:
            status_changed = self._update_source_status(result.connection_status)

        try:
            if result.has_relation_changes:
                self._metadata_refresher()
        except Exception as exc:
            logger.warning(
                "Failed to refresh workbench metadata after '%s' discovery: %s",
                result.source_type,
                exc,
            )

        if emit_event and result.has_relation_changes:
            self._record_event(result)
            return

        if status_changed:
            with self._condition:
                self._state_version += 1
                self._condition.notify_all()

    def _update_source_status(self, status: SourceConnectionStatus) -> bool:
        with self._condition:
            previous = self._source_statuses.get(status.source_id)
            if (
                previous is not None
                and previous.source_id == status.source_id
                and previous.state == status.state
                and previous.label == status.label
                and previous.detail == status.detail
            ):
                return False
            self._source_statuses[status.source_id] = status
            return True

    def _record_event(self, result: DataSourceDiscoveryResult) -> None:
        event = DataSourceDiscoveryEventDefinition(
            event_id=f"source-{uuid.uuid4().hex}",
            source_type=result.source_type,
            source_id=result.source_id,
            source_label=result.source_label,
            detected_at=utc_now_iso(),
            message=result.message,
            added_relations=list(result.added_relations),
            removed_relations=list(result.removed_relations),
            updated_relations=list(result.updated_relations),
        )
        with self._condition:
            self._events.insert(0, event)
            del self._events[MAX_SOURCE_EVENT_HISTORY:]
            self._state_version += 1
            self._condition.notify_all()

    def _state_payload_locked(self) -> dict[str, Any]:
        last_detected_at = self._events[0].detected_at if self._events else None
        return {
            "version": self._state_version,
            "summary": {
                "eventCount": len(self._events),
                "lastDetectedAt": last_detected_at,
            },
            "events": [event.payload for event in self._events],
            "statuses": [status.payload for status in self._source_statuses.values()],
        }
