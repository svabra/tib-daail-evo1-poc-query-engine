from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
import sys
import time

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))

from bit_data_workbench.backend.source_ingestions import (  # noqa: E402
    SOURCE_INGESTION_PREFIX,
    SourceIngestionError,
    SourceIngestionManager,
    SourceIngestionStore,
    iso_utc,
    next_full_hour,
    scheduled_run_id,
)
from bit_data_workbench.models import (  # noqa: E402
    SourceCatalog,
    SourceField,
    SourceObject,
    SourceSchema,
)


class MemoryStore:
    def __init__(self) -> None:
        self.definitions: dict[str, dict] = {}
        self.runs: dict[str, dict] = {}

    def list_definitions(self):
        return sorted(
            (deepcopy(item) for item in self.definitions.values()),
            key=lambda item: item.get("updatedAt", ""),
            reverse=True,
        )

    def get_definition(self, definition_id):
        if definition_id not in self.definitions:
            raise KeyError(f"Unknown source ingestion: {definition_id}")
        return deepcopy(self.definitions[definition_id])

    def put_definition(self, definition):
        self.definitions[definition["id"]] = deepcopy(definition)
        return deepcopy(definition)

    def list_runs(self, definition_id=""):
        values = [
            deepcopy(item)
            for item in self.runs.values()
            if not definition_id or item.get("definitionId") == definition_id
        ]
        return sorted(values, key=lambda item: item.get("createdAt", ""), reverse=True)

    def get_run(self, run_id):
        item = self.runs.get(run_id)
        return deepcopy(item) if item else None

    def put_run(self, run):
        self.runs[run["id"]] = deepcopy(run)
        return deepcopy(run)


class FakeS3Client:
    def __init__(self) -> None:
        self.buckets = {"visible-bucket"}
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_bucket(self, *, Bucket):
        if Bucket not in self.buckets:
            raise RuntimeError("missing bucket")
        return {}

    def put_object(self, *, Bucket, Key, Body, **_kwargs):
        self.objects[(Bucket, Key)] = bytes(Body)
        return {}

    def get_object(self, *, Bucket, Key):
        return {"Body": BytesIO(self.objects[(Bucket, Key)])}

    def list_objects_v2(self, *, Bucket, Prefix, **_kwargs):
        return {
            "Contents": [
                {"Key": key}
                for bucket, key in sorted(self.objects)
                if bucket == Bucket and key.startswith(Prefix)
            ],
            "IsTruncated": False,
        }

    def head_object(self, *, Bucket, Key):
        payload = self.objects[(Bucket, Key)]
        return {"ContentLength": len(payload)}

    def copy_object(self, *, Bucket, Key, CopySource, **_kwargs):
        self.objects[(Bucket, Key)] = self.objects[
            (CopySource["Bucket"], CopySource["Key"])
        ]
        return {}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {}


class ValidationConnection:
    def __init__(self, row_count=4, columns=None) -> None:
        self.row_count = row_count
        self.columns = columns if columns is not None else [("ANMELDUNG_ID", "VARCHAR")]
        self.sql = ""
        self.closed = False

    def execute(self, sql):
        self.sql = sql
        return self

    def fetchall(self):
        return list(self.columns)

    def fetchone(self):
        return (self.row_count,)

    def close(self):
        self.closed = True


class FakeSourceSourcing:
    def __init__(self) -> None:
        self.active = True
        self.refresh_values: list[bool] = []
        self.catalog = SourceCatalog(
            name="ora_bazg_zoll",
            display_name="BAZG Zentrale Zollabwicklung",
            database_name="BZGZOLL1",
            source_platform="BIT Oracle RDBMS",
            site_label="PRIMUS & CAMPUS",
            owner_label="Sandro Wenger",
            schemas=[
                SourceSchema(
                    name="ZOLL",
                    objects=[
                        SourceObject(
                            name="ANMELDUNGEN",
                            kind="table",
                            relation="ora_bazg_zoll.ZOLL.ANMELDUNGEN",
                        )
                    ],
                )
            ],
        )

    def active_oracle_sources(self, actor, refresh=False):
        self.refresh_values.append(refresh)
        if not self.active or actor != "joel.ruod":
            return []
        return [{"id": "ora_bazg_zoll"}]

    def catalogs_for_actor(self, actor):
        return [self.catalog] if self.active and actor == "joel.ruod" else []

    def fields_for_relation(self, actor, relation):
        if self.active and actor == "joel.ruod" and relation.endswith("ANMELDUNGEN"):
            return [SourceField("ANMELDUNG_ID", "VARCHAR")]
        return []


def settings():
    return SimpleNamespace(s3_bucket="visible-bucket", shared_notebooks_bucket="")


def create_payload(*, scheduled=False, request_id="create-1"):
    return {
        "clientRequestId": request_id,
        "name": "BAZG Zollanmeldungen hourly refresh",
        "sourceId": "ora_bazg_zoll",
        "relation": {"schema": "ZOLL", "name": "ANMELDUNGEN"},
        "destination": {
            "bucket": "visible-bucket",
            "key": "ingestions/oracle/ora_bazg_zoll/zoll/anmeldungen.parquet",
        },
        "schedule": {
            "enabled": scheduled,
            "cadence": "hourly",
            "minute": 0,
            "timeZone": "Europe/Zurich",
        },
    }


def make_manager(*, query_status="completed", row_count=4, columns=None, now=None):
    store = MemoryStore()
    client = FakeS3Client()
    sourcing = FakeSourceSourcing()
    calls = []
    refreshes = []
    current = now or datetime(2026, 8, 20, 10, 15, tzinfo=UTC)

    def runner(**kwargs):
        calls.append(kwargs)
        if query_status == "completed":
            path = kwargs["staging_path"].removeprefix("s3://")
            bucket, key = path.split("/", 1)
            client.objects[(bucket, key)] = b"PARQUET-STAGED-CONTENT"
            return {"jobId": "query-oracle-copy", "status": "completed"}
        return {"jobId": "query-oracle-copy", "status": query_status, "error": "forced query failure"}

    manager = SourceIngestionManager(
        settings(),
        source_sourcing=sourcing,
        query_runner=runner,
        store=store,
        client_factory=lambda: client,
        validation_connection_factory=lambda: ValidationConnection(
            row_count=row_count,
            columns=columns,
        ),
        metadata_refresher=refreshes.append,
        clock=lambda: current,
    )
    return manager, store, client, sourcing, calls, refreshes


def wait_for_run(manager, run):
    return manager.wait_for_terminal(run["id"], timeout=3)


def test_scheduled_creation_runs_once_then_activates_hourly_schedule() -> None:
    manager, store, client, sourcing, calls, refreshes = make_manager()
    created = manager.create_definition("joel.ruod", create_payload(scheduled=True))
    run = wait_for_run(manager, created["run"])
    definition = store.get_definition(created["definition"]["id"])

    assert run["status"] == "completed"
    assert run["trigger"] == "activation-test"
    assert run["rowCount"] == 4
    assert definition["state"] == "active"
    assert definition["schedule"]["enabled"] is True
    assert definition["nextRunAt"] == "2026-08-20T11:00:00Z"
    assert calls[0]["actor"] == "joel.ruod"
    assert calls[0]["display_sql"] == "SELECT * FROM ora_bazg_zoll.ZOLL.ANMELDUNGEN"
    assert sourcing.refresh_values[-1] is True
    final_key = ("visible-bucket", definition["destinationKey"])
    assert client.objects[final_key] == b"PARQUET-STAGED-CONTENT"
    assert not any(key.startswith(SOURCE_INGESTION_PREFIX) for _, key in client.objects)
    assert refreshes == ["visible-bucket"]


def test_one_time_and_hourly_modes_reuse_the_same_definition() -> None:
    manager, store, _client, _sourcing, calls, _refreshes = make_manager()
    created = manager.create_definition("joel.ruod", create_payload())
    wait_for_run(manager, created["run"])
    definition_id = created["definition"]["id"]
    one_time = store.get_definition(definition_id)
    assert one_time["state"] == "paused"
    assert one_time["schedule"]["enabled"] is False

    enabled = manager.update_schedule("joel.ruod", definition_id, {"enabled": True})
    assert enabled["run"] is None
    assert enabled["definition"]["state"] == "active"
    assert enabled["definition"]["sourceId"] == one_time["sourceId"]
    assert enabled["definition"]["destinationKey"] == one_time["destinationKey"]
    assert len(calls) == 1

    disabled = manager.update_schedule("joel.ruod", definition_id, {"enabled": False})
    assert disabled["definition"]["state"] == "paused"
    assert disabled["definition"]["lastSuccessfulRunAt"]


def test_creation_and_manual_run_are_idempotent() -> None:
    manager, store, _client, _sourcing, calls, _refreshes = make_manager()
    first = manager.create_definition("joel.ruod", create_payload(request_id="stable"))
    wait_for_run(manager, first["run"])
    second = manager.create_definition("joel.ruod", create_payload(request_id="stable"))
    assert second["created"] is False
    assert second["definition"]["id"] == first["definition"]["id"]
    assert len(calls) == 1

    manual = manager.start_run(
        "joel.ruod",
        first["definition"]["id"],
        {"clientRequestId": "manual-stable", "trigger": "manual"},
    )
    wait_for_run(manager, manual)
    replay = manager.start_run(
        "joel.ruod",
        first["definition"]["id"],
        {"clientRequestId": "manual-stable", "trigger": "manual"},
    )
    assert replay["id"] == manual["id"]
    assert len(store.list_runs(first["definition"]["id"])) == 2


def test_failed_extraction_preserves_previous_target_byte_for_byte() -> None:
    manager, store, client, _sourcing, _calls, _refreshes = make_manager(query_status="failed")
    target = ("visible-bucket", create_payload()["destination"]["key"])
    client.objects[target] = b"PREVIOUS-COMPLETE-VERSION"
    created = manager.create_definition("joel.ruod", create_payload())
    run = wait_for_run(manager, created["run"])
    assert run["status"] == "failed"
    assert client.objects[target] == b"PREVIOUS-COMPLETE-VERSION"
    assert store.get_definition(created["definition"]["id"])["state"] == "draft"


def test_failed_parquet_schema_validation_preserves_previous_target_byte_for_byte() -> None:
    manager, store, client, _sourcing, _calls, _refreshes = make_manager(columns=[])
    target = ("visible-bucket", create_payload()["destination"]["key"])
    client.objects[target] = b"PREVIOUS-COMPLETE-VERSION"
    created = manager.create_definition("joel.ruod", create_payload())
    run = wait_for_run(manager, created["run"])
    assert run["status"] == "failed"
    assert "schema fields" in run["error"]
    assert client.objects[target] == b"PREVIOUS-COMPLETE-VERSION"
    assert store.get_definition(created["definition"]["id"])["state"] == "draft"


def test_revoked_grant_blocks_run_and_pauses_schedule_fail_closed() -> None:
    manager, store, _client, sourcing, _calls, _refreshes = make_manager()
    created = manager.create_definition("joel.ruod", create_payload(scheduled=True))
    wait_for_run(manager, created["run"])
    sourcing.active = False
    run = manager.start_run(
        "joel.ruod",
        created["definition"]["id"],
        {"clientRequestId": "revoked", "trigger": "manual"},
    )
    blocked = wait_for_run(manager, run)
    definition = store.get_definition(created["definition"]["id"])
    assert blocked["status"] == "blocked"
    assert definition["state"] == "attention"
    assert definition["schedule"]["enabled"] is False
    assert definition["nextRunAt"] == ""


@pytest.mark.parametrize(
    "bucket,key",
    [
        ("visible-bucket", "folder/"),
        ("visible-bucket", "folder/data.csv"),
        ("visible-bucket", "folder/*.parquet"),
        ("visible-bucket", "--bdw-internal--/secret.parquet"),
        ("visible-bucket-shared-notebooks", "data.parquet"),
    ],
)
def test_destination_validation_rejects_non_visible_or_non_parquet_targets(bucket, key) -> None:
    manager, *_ = make_manager()
    payload = create_payload()
    payload["destination"] = {"bucket": bucket, "key": key}
    with pytest.raises(SourceIngestionError) as error:
        manager.create_definition("joel.ruod", payload)
    assert error.value.status_code == 422


def test_actor_isolation_hides_foreign_definitions() -> None:
    manager, _store, _client, _sourcing, _calls, _refreshes = make_manager()
    created = manager.create_definition("joel.ruod", create_payload())
    wait_for_run(manager, created["run"])
    with pytest.raises(SourceIngestionError) as error:
        manager.get_definition("noemie.rochat", created["definition"]["id"])
    assert error.value.status_code == 404
    assert manager.list_definitions("noemie.rochat")["items"] == []

    with pytest.raises(SourceIngestionError) as create_error:
        manager.create_definition("noemie.rochat", create_payload(request_id="noemie"))
    assert create_error.value.status_code == 403


def test_patching_metadata_never_changes_the_granted_source_relation() -> None:
    manager, _store, _client, _sourcing, _calls, _refreshes = make_manager()
    created = manager.create_definition("joel.ruod", create_payload())
    wait_for_run(manager, created["run"])
    updated = manager.patch_definition(
        "joel.ruod",
        created["definition"]["id"],
        {
            "name": "Renamed ingestion",
            "sourceId": "ora_hidden_source",
            "relation": {"schema": "OTHER", "name": "SECRET"},
        },
    )
    assert updated["name"] == "Renamed ingestion"
    assert updated["sourceId"] == "ora_bazg_zoll"
    assert updated["schemaName"] == "ZOLL"
    assert updated["relationName"] == "ANMELDUNGEN"


def test_scheduler_skips_missed_slot_without_backfill() -> None:
    observed = datetime(2026, 8, 20, 10, 15, tzinfo=UTC)
    manager, store, _client, _sourcing, _calls, _refreshes = make_manager(now=observed)
    created = manager.create_definition("joel.ruod", create_payload(scheduled=True))
    wait_for_run(manager, created["run"])
    definition = store.get_definition(created["definition"]["id"])
    definition["nextRunAt"] = "2026-08-20T08:00:00Z"
    store.put_definition(definition)

    manager.run_scheduler_once(observed)
    runs = store.list_runs(definition["id"])
    missed = next(run for run in runs if run["status"] == "skipped")
    assert missed["id"] == scheduled_run_id(definition["id"], datetime(2026, 8, 20, 8, tzinfo=UTC))
    assert store.get_definition(definition["id"])["nextRunAt"] == "2026-08-20T11:00:00Z"


def test_scheduler_records_deterministic_skip_when_previous_run_overlaps() -> None:
    observed = datetime(2026, 8, 20, 10, 15, tzinfo=UTC)
    manager, store, _client, _sourcing, _calls, _refreshes = make_manager(now=observed)
    created = manager.create_definition("joel.ruod", create_payload(scheduled=True))
    wait_for_run(manager, created["run"])
    definition = store.get_definition(created["definition"]["id"])
    slot = datetime(2026, 8, 20, 10, 0, tzinfo=UTC)
    definition["nextRunAt"] = iso_utc(slot)
    store.put_definition(definition)
    store.put_run(
        {
            "id": "source-ingestion-run-still-running",
            "definitionId": definition["id"],
            "actorId": "joel.ruod",
            "status": "running",
            "createdAt": "2026-08-20T09:59:00Z",
        }
    )

    manager.run_scheduler_once(observed)
    expected_id = scheduled_run_id(definition["id"], slot)
    skipped = store.get_run(expected_id)
    assert skipped["status"] == "skipped"
    assert "previous ingestion is still running" in skipped["message"]
    manager.run_scheduler_once(observed)
    assert len([run for run in store.list_runs(definition["id"]) if run["id"] == expected_id]) == 1


def test_temporary_run_failure_keeps_an_active_schedule_for_the_next_hour() -> None:
    manager, store, _client, _sourcing, _calls, _refreshes = make_manager()
    created = manager.create_definition("joel.ruod", create_payload(scheduled=True))
    wait_for_run(manager, created["run"])
    manager._query_runner = lambda **_kwargs: {  # noqa: SLF001 - focused failure seam
        "jobId": "query-temporary-failure",
        "status": "failed",
        "error": "temporary S3 outage",
    }
    failed = manager.start_run(
        "joel.ruod",
        created["definition"]["id"],
        {"clientRequestId": "temporary-failure", "trigger": "manual"},
    )
    failed = wait_for_run(manager, failed)
    definition = store.get_definition(created["definition"]["id"])
    assert failed["status"] == "failed"
    assert definition["state"] == "active"
    assert definition["schedule"]["enabled"] is True
    assert definition["nextRunAt"] == "2026-08-20T11:00:00Z"


def test_next_full_hour_handles_swiss_dst_jump() -> None:
    before_jump = datetime(2026, 3, 29, 0, 30, tzinfo=UTC)
    assert iso_utc(next_full_hour(before_jump)) == "2026-03-29T01:00:00Z"


def test_sse_state_is_only_a_change_signal_without_actor_data() -> None:
    manager, *_ = make_manager()
    payload = manager.state_payload()
    assert payload["available"] is True
    assert "definitions" not in payload
    assert "runs" not in payload
    assert payload["version"] == 0
    assert payload["changedAt"] == "2026-08-20T10:15:00Z"


def test_process_restart_closes_orphaned_run_and_preserves_definition() -> None:
    manager, store, _client, _sourcing, _calls, _refreshes = make_manager()
    definition = {
        "id": "source-ingestion-restart",
        "actorId": "joel.ruod",
        "state": "draft",
        "schedule": {"enabled": False, "requestedEnabled": True},
        "pendingActivation": True,
        "updatedAt": "2026-08-20T10:00:00Z",
    }
    store.put_definition(definition)
    store.put_run(
        {
            "id": "source-ingestion-run-restart",
            "definitionId": definition["id"],
            "actorId": "joel.ruod",
            "status": "running",
            "createdAt": "2026-08-20T10:00:00Z",
        }
    )
    manager.start()
    try:
        recovered = store.get_run("source-ingestion-run-restart")
        assert recovered["status"] == "failed"
        assert "restarted" in recovered["message"]
        persisted = store.get_definition(definition["id"])
        assert persisted["pendingActivation"] is False
        assert persisted["state"] == "draft"
    finally:
        manager.stop()


def test_s3_store_restores_definitions_and_runs_after_new_manager_instance(monkeypatch) -> None:
    client = FakeS3Client()
    monkeypatch.setattr(
        "bit_data_workbench.backend.source_ingestions.ensure_s3_bucket",
        lambda *_args, **_kwargs: None,
    )
    first = SourceIngestionStore(settings(), client_factory=lambda: client)
    first.put_definition(
        {
            "id": "source-ingestion-persisted",
            "actorId": "joel.ruod",
            "state": "active",
            "schedule": {"enabled": True},
            "updatedAt": "2026-08-20T10:15:00Z",
        }
    )
    first.put_run(
        {
            "id": "source-ingestion-run-persisted",
            "definitionId": "source-ingestion-persisted",
            "actorId": "joel.ruod",
            "status": "completed",
            "createdAt": "2026-08-20T10:15:00Z",
        }
    )

    restored = SourceIngestionStore(settings(), client_factory=lambda: client)
    assert restored.get_definition("source-ingestion-persisted")["state"] == "active"
    assert restored.list_runs("source-ingestion-persisted")[0]["id"] == (
        "source-ingestion-run-persisted"
    )
    assert all(key.startswith(SOURCE_INGESTION_PREFIX) for _, key in client.objects)
