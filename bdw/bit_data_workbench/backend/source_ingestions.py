from __future__ import annotations

from contextlib import suppress
from datetime import UTC, datetime
import json
import logging
import re
from threading import Event, RLock, Thread
import time
from typing import Any, Callable
from urllib.parse import quote
import uuid
from zoneinfo import ZoneInfo

from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from .runtime_connections import create_duckdb_worker_connection
from .s3_hidden import INTERNAL_S3_PREFIX, is_hidden_s3_bucket_name, is_internal_s3_key
from .s3_storage import ensure_s3_bucket, iter_s3_keys, list_s3_buckets, s3_client
from .source_sourcing import SourceSourcingCoordinator, SourceSourcingError, validated_demo_actor


logger = logging.getLogger(__name__)

SOURCE_INGESTION_SCHEMA_VERSION = 1
SOURCE_INGESTION_PREFIX = f"{INTERNAL_S3_PREFIX}source-ingestions/"
SOURCE_INGESTION_DEFINITIONS_PREFIX = f"{SOURCE_INGESTION_PREFIX}definitions/"
SOURCE_INGESTION_RUNS_PREFIX = f"{SOURCE_INGESTION_PREFIX}runs/"
SOURCE_INGESTION_STAGING_PREFIX = f"{SOURCE_INGESTION_PREFIX}staging/"
SOURCE_INGESTION_TIMEZONE = "Europe/Zurich"
SOURCE_INGESTION_SCHEDULER_INTERVAL_SECONDS = 15.0
SOURCE_INGESTION_TERMINAL_STATUSES = frozenset(
    {"completed", "failed", "blocked", "skipped", "cancelled"}
)
SOURCE_INGESTION_ACTIVE_STATUSES = frozenset({"queued", "running"})
SOURCE_ID_RE = re.compile(r"^ora_[a-z0-9_]+$")
RELATION_PART_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$#]*$")


class SourceIngestionError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_utc(value: datetime | None = None) -> str:
    normalized = (value or utc_now()).astimezone(UTC).replace(microsecond=0)
    return normalized.isoformat().replace("+00:00", "Z")


def parse_iso(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def next_full_hour(value: datetime | None = None) -> datetime:
    current = (value or utc_now()).astimezone(UTC)
    next_epoch = (int(current.timestamp()) // 3600 + 1) * 3600
    return datetime.fromtimestamp(next_epoch, tz=UTC)


def swiss_datetime_label(value: object) -> str:
    parsed = parse_iso(value)
    if parsed is None:
        return ""
    return parsed.astimezone(ZoneInfo(SOURCE_INGESTION_TIMEZONE)).strftime(
        "%d.%m.%Y, %H:%M %Z"
    )


def scheduled_run_id(definition_id: str, scheduled_for: datetime) -> str:
    stamp = scheduled_for.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
    return f"source-ingestion-run-{definition_id.removeprefix('source-ingestion-')}-{stamp}"


def _safe_path_segment(value: object) -> str:
    text = str(value or "").strip()
    return quote(text, safe="-_.")


def _definition_key(definition_id: str) -> str:
    return f"{SOURCE_INGESTION_DEFINITIONS_PREFIX}{_safe_path_segment(definition_id)}.json"


def _run_key(run: dict[str, Any]) -> str:
    observed = parse_iso(run.get("createdAt")) or utc_now()
    return (
        f"{SOURCE_INGESTION_RUNS_PREFIX}{observed:%Y/%m/%d}/"
        f"{_safe_path_segment(run.get('id'))}.json"
    )


def _json_clone(payload: dict[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(payload))


def _validate_source_id(value: object) -> str:
    source_id = str(value or "").strip().lower()
    if not SOURCE_ID_RE.fullmatch(source_id):
        raise SourceIngestionError(422, "Only a granted Oracle PoC source can be ingested.")
    return source_id


def _validate_relation_part(value: object, label: str) -> str:
    normalized = str(value or "").strip().upper()
    if not RELATION_PART_RE.fullmatch(normalized):
        raise SourceIngestionError(422, f"Provide a valid Oracle {label}.")
    return normalized


def normalize_destination(settings: Settings, payload: object) -> tuple[str, str]:
    destination = payload if isinstance(payload, dict) else {}
    bucket = str(destination.get("bucket") or "").strip()
    key = str(destination.get("key") or "").strip().replace("\\", "/").lstrip("/")
    if not bucket:
        raise SourceIngestionError(422, "Choose an S3 destination bucket.")
    if is_hidden_s3_bucket_name(bucket, settings):
        raise SourceIngestionError(422, "Reserved Workbench buckets cannot be ingestion targets.")
    if not key or key.endswith("/"):
        raise SourceIngestionError(422, "The S3 destination must point to a Parquet file.")
    if any(character in key for character in "*?[]"):
        raise SourceIngestionError(422, "The S3 destination must not contain wildcard characters.")
    if is_internal_s3_key(key):
        raise SourceIngestionError(422, "Internal Workbench S3 paths are reserved.")
    if not key.casefold().endswith(".parquet"):
        raise SourceIngestionError(422, "Source ingestions currently write Parquet files only.")
    return bucket, key


class SourceIngestionStore:
    """S3-backed, restart-safe storage for definitions and immutable run records."""

    def __init__(
        self,
        settings: Settings,
        *,
        client_factory: Callable[[], Any] | None = None,
    ) -> None:
        self._settings = settings
        self._bucket = str(settings.s3_bucket or "").strip()
        self._client_factory = client_factory or (lambda: s3_client(settings))
        self._lock = RLock()

    @property
    def bucket(self) -> str:
        return self._bucket

    def _client(self):
        if not self._bucket:
            raise SourceIngestionError(503, "S3 is not configured for source-ingestion state.")
        ensure_s3_bucket(self._settings, self._bucket)
        return self._client_factory()

    @staticmethod
    def _read_json(client, bucket: str, key: str) -> dict[str, Any] | None:
        try:
            response = client.get_object(Bucket=bucket, Key=key)
            payload = json.loads(response["Body"].read().decode("utf-8"))
        except (ClientError, BotoCoreError, ValueError, UnicodeError, KeyError):
            return None
        return dict(payload) if isinstance(payload, dict) else None

    def list_definitions(self) -> list[dict[str, Any]]:
        with self._lock:
            client = self._client()
            definitions = [
                payload
                for key in iter_s3_keys(client, self._bucket, SOURCE_INGESTION_DEFINITIONS_PREFIX)
                if key.endswith(".json")
                for payload in [self._read_json(client, self._bucket, key)]
                if payload is not None
            ]
        return sorted(definitions, key=lambda item: str(item.get("updatedAt") or ""), reverse=True)

    def get_definition(self, definition_id: str) -> dict[str, Any]:
        with self._lock:
            payload = self._read_json(
                self._client(), self._bucket, _definition_key(definition_id)
            )
        if payload is None:
            raise KeyError(f"Unknown source ingestion: {definition_id}")
        return payload

    def put_definition(self, definition: dict[str, Any]) -> dict[str, Any]:
        payload = _json_clone(definition)
        payload["schemaVersion"] = SOURCE_INGESTION_SCHEMA_VERSION
        with self._lock:
            self._client().put_object(
                Bucket=self._bucket,
                Key=_definition_key(str(payload.get("id") or "")),
                Body=json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json",
            )
        return payload

    def list_runs(self, definition_id: str = "") -> list[dict[str, Any]]:
        with self._lock:
            client = self._client()
            runs = [
                payload
                for key in iter_s3_keys(client, self._bucket, SOURCE_INGESTION_RUNS_PREFIX)
                if key.endswith(".json")
                for payload in [self._read_json(client, self._bucket, key)]
                if payload is not None
                and (not definition_id or payload.get("definitionId") == definition_id)
            ]
        return sorted(runs, key=lambda item: str(item.get("createdAt") or ""), reverse=True)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        return next(
            (run for run in self.list_runs() if str(run.get("id") or "") == run_id),
            None,
        )

    def put_run(self, run: dict[str, Any]) -> dict[str, Any]:
        payload = _json_clone(run)
        payload["schemaVersion"] = SOURCE_INGESTION_SCHEMA_VERSION
        key = str(payload.get("storeKey") or _run_key(payload))
        payload["storeKey"] = key
        with self._lock:
            self._client().put_object(
                Bucket=self._bucket,
                Key=key,
                Body=json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json",
            )
        return payload


class SourceIngestionManager:
    """Actor-scoped Oracle-to-S3 full-refresh definitions and executions."""

    def __init__(
        self,
        settings: Settings,
        *,
        source_sourcing: SourceSourcingCoordinator,
        query_runner: Callable[..., dict[str, Any]],
        store: SourceIngestionStore | None = None,
        client_factory: Callable[[], Any] | None = None,
        validation_connection_factory: Callable[[], Any] | None = None,
        metadata_refresher: Callable[[str], None] | None = None,
        state_change_callback: Callable[[dict[str, Any]], None] | None = None,
        clock: Callable[[], datetime] = utc_now,
        scheduler_interval_seconds: float = SOURCE_INGESTION_SCHEDULER_INTERVAL_SECONDS,
    ) -> None:
        self._settings = settings
        self._source_sourcing = source_sourcing
        self._query_runner = query_runner
        self._store = store or SourceIngestionStore(settings, client_factory=client_factory)
        self._client_factory = client_factory or (lambda: s3_client(settings))
        self._validation_connection_factory = validation_connection_factory or (
            lambda: create_duckdb_worker_connection(
                settings,
                database_path=":memory:",
                bootstrap_postgres=False,
            )
        )
        self._metadata_refresher = metadata_refresher or (lambda _bucket: None)
        self._state_change_callback = state_change_callback or (lambda _payload: None)
        self._clock = clock
        self._scheduler_interval_seconds = max(0.1, float(scheduler_interval_seconds))
        self._lock = RLock()
        self._state_version = 0
        self._stop_event = Event()
        self._scheduler_thread: Thread | None = None
        self._workers: dict[str, Thread] = {}

    def start(self) -> None:
        self._recover_interrupted_runs()
        with self._lock:
            if self._scheduler_thread is not None and self._scheduler_thread.is_alive():
                return
            self._stop_event.clear()
            self._scheduler_thread = Thread(
                target=self._scheduler_loop,
                daemon=True,
                name="bdw-source-ingestion-scheduler",
            )
            self._scheduler_thread.start()

    def _recover_interrupted_runs(self) -> None:
        """Close runs whose worker disappeared during a process restart."""
        observed = iso_utc(self._clock())
        changed = False
        for run in self._store.list_runs():
            if run.get("status") not in SOURCE_INGESTION_ACTIVE_STATUSES:
                continue
            run["status"] = "failed"
            run["completedAt"] = observed
            run["updatedAt"] = observed
            run["message"] = "The Workbench restarted before this ingestion completed."
            run["error"] = "Source ingestion worker interrupted by process restart."
            self._store.put_run(run)
            with suppress(KeyError):
                definition = self._store.get_definition(str(run.get("definitionId") or ""))
                definition["lastRunId"] = run["id"]
                definition["pendingActivation"] = False
                if not definition.get("schedule", {}).get("enabled"):
                    definition["state"] = "draft"
                definition["updatedAt"] = observed
                self._store.put_definition(definition)
            changed = True
        if changed:
            self._notify()

    def stop(self) -> None:
        self._stop_event.set()
        thread = self._scheduler_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)
        with self._lock:
            workers = list(self._workers.values())
        for worker in workers:
            if worker.is_alive():
                worker.join(timeout=0.2)

    def _notify(self) -> None:
        with self._lock:
            self._state_version += 1
        with suppress(Exception):
            self._state_change_callback(self.state_payload())

    def state_payload(self) -> dict[str, Any]:
        try:
            self._store.list_definitions()
            available = True
            message = ""
        except Exception as exc:
            available = False
            message = str(exc)
        return {
            "version": self._state_version,
            "available": available,
            # The shared SSE stream must not expose actor-specific definitions.
            # Clients use this signal to reload the actor-protected API instead.
            "changedAt": iso_utc(self._clock()),
            "message": message,
        }

    def source_context(self, actor: str) -> dict[str, Any]:
        normalized_actor = validated_demo_actor(actor)
        try:
            sources = self._source_sourcing.active_oracle_sources(normalized_actor)
            catalogs = self._source_sourcing.catalogs_for_actor(normalized_actor)
        except SourceSourcingError as exc:
            raise SourceIngestionError(exc.status_code, exc.detail) from exc
        catalog_by_id = {catalog.name: catalog for catalog in catalogs}
        items: list[dict[str, Any]] = []
        for source in sources:
            source_id = str(source.get("id") or "").strip()
            catalog = catalog_by_id.get(source_id)
            if catalog is None:
                continue
            relations = [
                {
                    "schema": schema.name,
                    "name": source_object.name,
                    "kind": source_object.kind,
                    "relation": source_object.relation,
                    "displayName": source_object.display_name or source_object.name,
                }
                for schema in catalog.schemas
                for source_object in schema.objects
            ]
            items.append(
                {
                    "id": source_id,
                    "displayName": catalog.display_name or source_id,
                    "databaseName": catalog.database_name,
                    "platform": catalog.source_platform or "BIT Oracle RDBMS",
                    "site": catalog.site_label,
                    "owner": catalog.owner_label,
                    "relations": relations,
                }
            )
        try:
            visible_buckets = sorted(
                bucket
                for bucket in list_s3_buckets(self._settings)
                if not is_hidden_s3_bucket_name(bucket, self._settings)
            )
        except Exception:
            configured_bucket = str(self._settings.s3_bucket or "").strip()
            visible_buckets = [configured_bucket] if configured_bucket else []
        return {
            "actorId": normalized_actor,
            "sources": items,
            "defaultBucket": str(self._settings.s3_bucket or "").strip(),
            "visibleBuckets": visible_buckets,
        }

    def list_definitions(self, actor: str) -> dict[str, Any]:
        normalized_actor = validated_demo_actor(actor)
        definitions = [
            self._decorate_definition(item)
            for item in self._store.list_definitions()
            if item.get("actorId") == normalized_actor and item.get("state") != "archived"
        ]
        runs = [
            self._decorate_run(item)
            for item in self._store.list_runs()
            if item.get("actorId") == normalized_actor
        ]
        recent_threshold = self._clock().timestamp() - 24 * 3600
        recent_runs = [
            run
            for run in runs
            if (parse_iso(run.get("createdAt")) or datetime.min.replace(tzinfo=UTC)).timestamp()
            >= recent_threshold
        ]
        return {
            "items": definitions,
            "runs": runs[:100],
            "summary": {
                "total": len(definitions),
                "activeSchedules": sum(
                    1 for item in definitions if bool((item.get("schedule") or {}).get("enabled"))
                ),
                "attention": sum(1 for item in definitions if item.get("state") == "attention"),
                "runsLast24Hours": len(recent_runs),
            },
        }

    def get_definition(self, actor: str, definition_id: str) -> dict[str, Any]:
        definition = self._owned_definition(actor, definition_id)
        runs = [self._decorate_run(run) for run in self._store.list_runs(definition_id)]
        return {"definition": self._decorate_definition(definition), "runs": runs}

    def create_definition(self, actor: str, payload: dict[str, Any]) -> dict[str, Any]:
        normalized_actor = validated_demo_actor(actor)
        client_request_id = str(payload.get("clientRequestId") or "").strip()
        if not client_request_id:
            raise SourceIngestionError(422, "Provide a clientRequestId for idempotent submission.")
        for existing in self._store.list_definitions():
            if (
                existing.get("actorId") == normalized_actor
                and existing.get("clientRequestId") == client_request_id
            ):
                runs = self._store.list_runs(str(existing.get("id") or ""))
                return {
                    "definition": self._decorate_definition(existing),
                    "run": self._decorate_run(runs[0]) if runs else None,
                    "created": False,
                }

        source_id = _validate_source_id(payload.get("sourceId"))
        relation = payload.get("relation") if isinstance(payload.get("relation"), dict) else {}
        schema_name = _validate_relation_part(relation.get("schema"), "schema")
        relation_name = _validate_relation_part(relation.get("name"), "relation")
        relation_kind = self._resolve_relation(
            normalized_actor, source_id, schema_name, relation_name, refresh=True
        )["kind"]
        destination_bucket, destination_key = normalize_destination(
            self._settings, payload.get("destination")
        )
        self._assert_bucket_accessible(destination_bucket)

        schedule_payload = payload.get("schedule") if isinstance(payload.get("schedule"), dict) else {}
        requested_schedule = bool(schedule_payload.get("enabled"))
        definition_id = f"source-ingestion-{uuid.uuid4().hex}"
        now = iso_utc(self._clock())
        name = str(payload.get("name") or "").strip()
        if len(name) < 3:
            name = f"{source_id} {schema_name}.{relation_name} full refresh"
        definition = {
            "schemaVersion": SOURCE_INGESTION_SCHEMA_VERSION,
            "id": definition_id,
            "clientRequestId": client_request_id,
            "actorId": normalized_actor,
            "name": name,
            "sourceKind": "oracle-poc",
            "sourceId": source_id,
            "schemaName": schema_name,
            "relationName": relation_name,
            "relationKind": relation_kind,
            "destinationBucket": destination_bucket,
            "destinationKey": destination_key,
            "format": "parquet",
            "writeMode": "replace",
            "schedule": {
                "enabled": False,
                "requestedEnabled": requested_schedule,
                "cadence": "hourly",
                "minute": 0,
                "timeZone": SOURCE_INGESTION_TIMEZONE,
            },
            "state": "draft",
            "pendingActivation": requested_schedule,
            "lastSuccessfulRunAt": "",
            "lastRunId": "",
            "nextRunAt": "",
            "createdAt": now,
            "updatedAt": now,
        }
        self._store.put_definition(definition)
        run = self.start_run(
            normalized_actor,
            definition_id,
            {
                "clientRequestId": f"{client_request_id}:initial",
                "trigger": "activation-test" if requested_schedule else "manual",
                "activateScheduleOnSuccess": requested_schedule,
            },
        )
        return {
            "definition": self._decorate_definition(self._store.get_definition(definition_id)),
            "run": run,
            "created": True,
        }

    def patch_definition(
        self, actor: str, definition_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        definition = self._owned_definition(actor, definition_id)
        self._assert_not_running(definition_id)
        changed_execution = False
        if "name" in payload:
            name = str(payload.get("name") or "").strip()
            if len(name) < 3:
                raise SourceIngestionError(422, "The ingestion name requires at least 3 characters.")
            definition["name"] = name
        if "destination" in payload:
            bucket, key = normalize_destination(self._settings, payload.get("destination"))
            self._assert_bucket_accessible(bucket)
            changed_execution = (
                bucket != definition.get("destinationBucket")
                or key != definition.get("destinationKey")
            )
            definition["destinationBucket"] = bucket
            definition["destinationKey"] = key
        if changed_execution:
            definition["schedule"]["enabled"] = False
            definition["schedule"]["requestedEnabled"] = False
            definition["pendingActivation"] = False
            definition["state"] = "draft"
            definition["nextRunAt"] = ""
            definition["lastSuccessfulRunAt"] = ""
        definition["updatedAt"] = iso_utc(self._clock())
        self._store.put_definition(definition)
        self._notify()
        return self._decorate_definition(definition)

    def update_schedule(
        self, actor: str, definition_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        definition = self._owned_definition(actor, definition_id)
        self._assert_not_running(definition_id)
        enabled = bool(payload.get("enabled"))
        if not enabled:
            definition["schedule"]["enabled"] = False
            definition["schedule"]["requestedEnabled"] = False
            definition["pendingActivation"] = False
            definition["state"] = "paused"
            definition["nextRunAt"] = ""
            definition["updatedAt"] = iso_utc(self._clock())
            self._store.put_definition(definition)
            self._notify()
            return {"definition": self._decorate_definition(definition), "run": None}

        if definition.get("lastSuccessfulRunAt") and definition.get("state") != "attention":
            definition["schedule"]["enabled"] = True
            definition["schedule"]["requestedEnabled"] = True
            definition["pendingActivation"] = False
            definition["state"] = "active"
            definition["nextRunAt"] = iso_utc(next_full_hour(self._clock()))
            definition["updatedAt"] = iso_utc(self._clock())
            self._store.put_definition(definition)
            self._notify()
            return {"definition": self._decorate_definition(definition), "run": None}

        definition["schedule"]["requestedEnabled"] = True
        definition["pendingActivation"] = True
        definition["updatedAt"] = iso_utc(self._clock())
        self._store.put_definition(definition)
        run = self.start_run(
            actor,
            definition_id,
            {
                "clientRequestId": str(payload.get("clientRequestId") or uuid.uuid4().hex),
                "trigger": "activation-test",
                "activateScheduleOnSuccess": True,
            },
        )
        return {
            "definition": self._decorate_definition(self._store.get_definition(definition_id)),
            "run": run,
        }

    def start_run(
        self, actor: str, definition_id: str, payload: dict[str, Any]
    ) -> dict[str, Any]:
        normalized_actor = validated_demo_actor(actor)
        definition = self._owned_definition(normalized_actor, definition_id)
        client_request_id = str(payload.get("clientRequestId") or "").strip()
        if not client_request_id:
            raise SourceIngestionError(422, "Provide a clientRequestId for idempotent execution.")
        for existing in self._store.list_runs(definition_id):
            if existing.get("clientRequestId") == client_request_id:
                return self._decorate_run(existing)
        self._assert_not_running(definition_id)
        trigger = str(payload.get("trigger") or "manual").strip().lower()
        if trigger not in {"manual", "activation-test", "scheduled"}:
            trigger = "manual"
        run_id = f"source-ingestion-run-{uuid.uuid4().hex}"
        scheduled_for = parse_iso(payload.get("scheduledFor"))
        if trigger == "scheduled" and scheduled_for is not None:
            run_id = scheduled_run_id(definition_id, scheduled_for)
            existing_scheduled = self._store.get_run(run_id)
            if existing_scheduled is not None:
                return self._decorate_run(existing_scheduled)
        now = iso_utc(self._clock())
        run = {
            "schemaVersion": SOURCE_INGESTION_SCHEMA_VERSION,
            "id": run_id,
            "definitionId": definition_id,
            "actorId": normalized_actor,
            "clientRequestId": client_request_id,
            "trigger": trigger,
            "scheduledFor": iso_utc(scheduled_for) if scheduled_for else "",
            "activateScheduleOnSuccess": bool(payload.get("activateScheduleOnSuccess")),
            "status": "queued",
            "queryJobId": "",
            "rowCount": 0,
            "sizeBytes": 0,
            "destinationPath": self._destination_path(definition),
            "startedAt": "",
            "completedAt": "",
            "createdAt": now,
            "updatedAt": now,
            "message": "Source ingestion is queued.",
            "error": "",
        }
        self._store.put_run(run)
        worker = Thread(
            target=self._execute_run,
            args=(run_id,),
            daemon=True,
            name=f"bdw-source-ingestion-{run_id[-8:]}",
        )
        with self._lock:
            self._workers[run_id] = worker
        worker.start()
        self._notify()
        return self._decorate_run(run)

    def wait_for_terminal(self, run_id: str, timeout: float = 60.0) -> dict[str, Any]:
        deadline = time.monotonic() + max(0.1, timeout)
        while time.monotonic() < deadline:
            run = self._store.get_run(run_id)
            if run is not None and run.get("status") in SOURCE_INGESTION_TERMINAL_STATUSES:
                return self._decorate_run(run)
            time.sleep(0.05)
        raise TimeoutError(f"Timed out waiting for source ingestion run {run_id}.")

    def run_scheduler_once(self, now: datetime | None = None) -> None:
        observed = (now or self._clock()).astimezone(UTC)
        for definition in self._store.list_definitions():
            schedule = definition.get("schedule") if isinstance(definition.get("schedule"), dict) else {}
            if not schedule.get("enabled") or definition.get("state") != "active":
                continue
            next_run = parse_iso(definition.get("nextRunAt"))
            if next_run is None:
                definition["nextRunAt"] = iso_utc(next_full_hour(observed))
                definition["updatedAt"] = iso_utc(observed)
                self._store.put_definition(definition)
                continue
            if next_run > observed:
                continue
            following = next_full_hour(observed)
            definition["nextRunAt"] = iso_utc(following)
            definition["updatedAt"] = iso_utc(observed)
            self._store.put_definition(definition)
            if next_run < observed.replace(minute=0, second=0, microsecond=0):
                self._record_skipped(definition, next_run, "Missed schedules are not backfilled.")
                continue
            if self._active_run(definition["id"]) is not None:
                self._record_skipped(definition, next_run, "The previous ingestion is still running.")
                continue
            with suppress(SourceIngestionError):
                self.start_run(
                    definition["actorId"],
                    definition["id"],
                    {
                        "clientRequestId": f"scheduled:{iso_utc(next_run)}",
                        "trigger": "scheduled",
                        "scheduledFor": iso_utc(next_run),
                    },
                )
        self._notify()

    def _scheduler_loop(self) -> None:
        while not self._stop_event.wait(self._scheduler_interval_seconds):
            try:
                self.run_scheduler_once()
            except Exception as exc:
                logger.warning("Source ingestion scheduler tick failed: %s", exc)

    def _execute_run(self, run_id: str) -> None:
        run = self._store.get_run(run_id)
        if run is None:
            return
        definition_id = str(run.get("definitionId") or "")
        staging_key = f"{SOURCE_INGESTION_STAGING_PREFIX}{definition_id}/{run_id}.parquet"
        try:
            definition = self._store.get_definition(definition_id)
            run["status"] = "running"
            run["startedAt"] = iso_utc(self._clock())
            run["updatedAt"] = run["startedAt"]
            run["message"] = "Reading the granted Oracle relation into a staged Parquet object."
            self._store.put_run(run)
            self._notify()

            self._resolve_relation(
                run["actorId"],
                definition["sourceId"],
                definition["schemaName"],
                definition["relationName"],
                refresh=True,
            )
            staging_path = f"s3://{definition['destinationBucket']}/{staging_key}"
            display_sql = (
                f"SELECT * FROM {definition['sourceId']}."
                f"{definition['schemaName']}.{definition['relationName']}"
            )
            query = self._query_runner(
                actor=run["actorId"],
                definition=definition,
                run=run,
                display_sql=display_sql,
                staging_path=staging_path,
            )
            run["queryJobId"] = str(query.get("jobId") or "")
            if query.get("status") != "completed":
                raise SourceIngestionError(
                    502,
                    str(query.get("error") or query.get("message") or "Oracle extraction failed."),
                )
            columns, row_count, staged_size = self._validate_staged_parquet(
                definition["destinationBucket"], staging_key, staging_path
            )
            self._promote(
                definition=definition,
                run=run,
                staging_key=staging_key,
                row_count=row_count,
                column_count=len(columns),
            )
            client = self._client_factory()
            final_head = client.head_object(
                Bucket=definition["destinationBucket"], Key=definition["destinationKey"]
            )
            # A completed run must never remain observable alongside its staging object.
            # The finally block repeats this as a best-effort guard for every failure path.
            client.delete_object(
                Bucket=definition["destinationBucket"],
                Key=staging_key,
            )
            run["status"] = "completed"
            run["rowCount"] = row_count
            run["sizeBytes"] = int(final_head.get("ContentLength") or staged_size)
            run["completedAt"] = iso_utc(self._clock())
            run["updatedAt"] = run["completedAt"]
            run["message"] = (
                f"Replaced {self._destination_path(definition)} with {row_count} row(s)."
            )
            self._store.put_run(run)
            definition["lastSuccessfulRunAt"] = run["completedAt"]
            definition["lastRunId"] = run_id
            if run.get("activateScheduleOnSuccess"):
                definition["schedule"]["enabled"] = True
                definition["schedule"]["requestedEnabled"] = True
                definition["pendingActivation"] = False
                definition["state"] = "active"
                definition["nextRunAt"] = iso_utc(next_full_hour(self._clock()))
            elif definition.get("schedule", {}).get("enabled"):
                definition["state"] = "active"
            else:
                definition["state"] = "paused"
            definition["updatedAt"] = run["completedAt"]
            self._store.put_definition(definition)
            with suppress(Exception):
                self._metadata_refresher(definition["destinationBucket"])
        except SourceSourcingError as exc:
            self._fail_run(run, exc.detail, blocked=exc.status_code in {401, 403, 404})
        except SourceIngestionError as exc:
            blocked = exc.status_code in {401, 403, 404}
            self._fail_run(run, exc.detail, blocked=blocked)
        except Exception as exc:
            logger.exception("Source ingestion run %s failed", run_id)
            self._fail_run(run, str(exc), blocked=False)
        finally:
            with suppress(Exception):
                self._client_factory().delete_object(
                    Bucket=(self._store.get_definition(definition_id))["destinationBucket"],
                    Key=staging_key,
                )
            with self._lock:
                self._workers.pop(run_id, None)
            self._notify()

    def _fail_run(self, run: dict[str, Any], error: str, *, blocked: bool) -> None:
        run["status"] = "blocked" if blocked else "failed"
        run["error"] = str(error or "Source ingestion failed.")
        run["message"] = (
            "The ingestion was blocked because its source grant is not active."
            if blocked
            else "The ingestion failed before the published S3 object was replaced."
        )
        run["completedAt"] = iso_utc(self._clock())
        run["updatedAt"] = run["completedAt"]
        self._store.put_run(run)
        with suppress(Exception):
            definition = self._store.get_definition(str(run.get("definitionId") or ""))
            definition["lastRunId"] = run["id"]
            definition["pendingActivation"] = False
            if blocked:
                definition["schedule"]["enabled"] = False
                definition["state"] = "attention"
                definition["nextRunAt"] = ""
            elif not definition.get("schedule", {}).get("enabled"):
                definition["state"] = "draft"
            definition["updatedAt"] = run["completedAt"]
            self._store.put_definition(definition)

    def _validate_staged_parquet(
        self, bucket: str, key: str, path: str
    ) -> tuple[list[tuple[Any, ...]], int, int]:
        client = self._client_factory()
        head = client.head_object(Bucket=bucket, Key=key)
        size = int(head.get("ContentLength") or 0)
        if size <= 0:
            raise SourceIngestionError(502, "The staged Parquet object is empty or missing.")
        connection = self._validation_connection_factory()
        escaped = path.replace("'", "''")
        try:
            columns = connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{escaped}')"
            ).fetchall()
            row_count = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{escaped}')"
                ).fetchone()[0]
            )
        finally:
            connection.close()
        if not columns:
            raise SourceIngestionError(502, "The staged Parquet object has no schema fields.")
        return list(columns), row_count, size

    def _promote(
        self,
        *,
        definition: dict[str, Any],
        run: dict[str, Any],
        staging_key: str,
        row_count: int,
        column_count: int,
    ) -> None:
        client = self._client_factory()
        client.copy_object(
            Bucket=definition["destinationBucket"],
            Key=definition["destinationKey"],
            CopySource={"Bucket": definition["destinationBucket"], "Key": staging_key},
            MetadataDirective="REPLACE",
            ContentType="application/vnd.apache.parquet",
            Metadata={
                "bdw-source-ingestion-id": definition["id"],
                "bdw-source-ingestion-run-id": run["id"],
                "bdw-source-id": definition["sourceId"],
                "bdw-source-relation": f"{definition['schemaName']}.{definition['relationName']}",
                "bdw-row-count": str(row_count),
                "bdw-column-count": str(column_count),
            },
        )

    def _resolve_relation(
        self,
        actor: str,
        source_id: str,
        schema_name: str,
        relation_name: str,
        *,
        refresh: bool,
    ) -> dict[str, str]:
        try:
            active_sources = self._source_sourcing.active_oracle_sources(actor, refresh=refresh)
            if source_id not in {str(item.get("id") or "") for item in active_sources}:
                raise SourceIngestionError(403, "An active DaCa source grant is required.")
            catalogs = self._source_sourcing.catalogs_for_actor(actor)
        except SourceSourcingError as exc:
            raise SourceIngestionError(exc.status_code, exc.detail) from exc
        for catalog in catalogs:
            if catalog.name != source_id:
                continue
            for schema in catalog.schemas:
                if schema.name.casefold() != schema_name.casefold():
                    continue
                for source_object in schema.objects:
                    if source_object.name.casefold() == relation_name.casefold():
                        fields = self._source_sourcing.fields_for_relation(
                            actor, f"{source_id}.{schema.name}.{source_object.name}"
                        )
                        if not fields:
                            raise SourceIngestionError(422, "The Oracle relation has no schema fields.")
                        return {
                            "schema": schema.name,
                            "name": source_object.name,
                            "kind": source_object.kind,
                        }
        raise SourceIngestionError(404, "The Oracle relation is not available to this user.")

    def _assert_bucket_accessible(self, bucket: str) -> None:
        try:
            self._client_factory().head_bucket(Bucket=bucket)
        except Exception as exc:
            raise SourceIngestionError(422, f"The S3 bucket '{bucket}' is not accessible.") from exc

    def _owned_definition(self, actor: str, definition_id: str) -> dict[str, Any]:
        try:
            definition = self._store.get_definition(str(definition_id or "").strip())
        except KeyError as exc:
            raise SourceIngestionError(404, str(exc)) from exc
        if definition.get("actorId") != validated_demo_actor(actor):
            raise SourceIngestionError(404, f"Unknown source ingestion: {definition_id}")
        return definition

    def _active_run(self, definition_id: str) -> dict[str, Any] | None:
        return next(
            (
                run
                for run in self._store.list_runs(definition_id)
                if run.get("status") in SOURCE_INGESTION_ACTIVE_STATUSES
            ),
            None,
        )

    def _assert_not_running(self, definition_id: str) -> None:
        if self._active_run(definition_id) is not None:
            raise SourceIngestionError(409, "This source ingestion already has a running job.")

    def _record_skipped(
        self, definition: dict[str, Any], scheduled_for: datetime, message: str
    ) -> None:
        run_id = scheduled_run_id(definition["id"], scheduled_for)
        if self._store.get_run(run_id) is not None:
            return
        observed = iso_utc(self._clock())
        self._store.put_run(
            {
                "schemaVersion": SOURCE_INGESTION_SCHEMA_VERSION,
                "id": run_id,
                "definitionId": definition["id"],
                "actorId": definition["actorId"],
                "clientRequestId": f"scheduled:{iso_utc(scheduled_for)}",
                "trigger": "scheduled",
                "scheduledFor": iso_utc(scheduled_for),
                "status": "skipped",
                "queryJobId": "",
                "rowCount": 0,
                "sizeBytes": 0,
                "destinationPath": self._destination_path(definition),
                "startedAt": "",
                "completedAt": observed,
                "createdAt": observed,
                "updatedAt": observed,
                "message": message,
                "error": "",
            }
        )

    @staticmethod
    def _destination_path(definition: dict[str, Any]) -> str:
        return f"s3://{definition.get('destinationBucket')}/{definition.get('destinationKey')}"

    def _decorate_definition(self, definition: dict[str, Any]) -> dict[str, Any]:
        payload = _json_clone(definition)
        payload["destinationPath"] = self._destination_path(payload)
        payload["relation"] = (
            f"{payload.get('sourceId')}.{payload.get('schemaName')}.{payload.get('relationName')}"
        )
        payload["nextRunLabel"] = swiss_datetime_label(payload.get("nextRunAt"))
        payload["lastSuccessfulRunLabel"] = swiss_datetime_label(
            payload.get("lastSuccessfulRunAt")
        )
        return payload

    @staticmethod
    def _decorate_run(run: dict[str, Any]) -> dict[str, Any]:
        payload = _json_clone(run)
        payload["scheduledForLabel"] = swiss_datetime_label(payload.get("scheduledFor"))
        payload["completedAtLabel"] = swiss_datetime_label(payload.get("completedAt"))
        return payload
