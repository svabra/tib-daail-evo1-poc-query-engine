from __future__ import annotations

from io import BytesIO
import json
from pathlib import Path
import sys
import threading
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


def import_shared_notebook_components():
    from bit_data_workbench.backend.service import WorkbenchService
    from bit_data_workbench.backend.shared_notebooks import (
        SharedNotebookFolder,
        deserialize_folder,
        serialize_folder,
    )
    from bit_data_workbench.models import (
        NotebookCellDefinition,
        NotebookDefinition,
    )

    return (
        WorkbenchService,
        NotebookCellDefinition,
        NotebookDefinition,
        SharedNotebookFolder,
        deserialize_folder,
        serialize_folder,
    )


class InMemorySharedNotebookStore:
    def __init__(self, notebooks=None, folders=None):
        self._notebooks = {
            notebook.notebook_id: notebook for notebook in (notebooks or [])
        }
        self._folders = {
            tuple(folder.path): folder for folder in (folders or [])
        }

    def list_notebooks(self):
        return list(self._notebooks.values())

    def upsert_notebook(self, notebook):
        action = (
            "updated"
            if notebook.notebook_id in self._notebooks
            else "created"
        )
        self._notebooks[notebook.notebook_id] = notebook
        return notebook, action

    def delete_notebook(self, notebook_id):
        return self._notebooks.pop(notebook_id)

    def list_folders(self):
        return list(self._folders.values())

    def upsert_folder(self, folder):
        action = "updated" if tuple(folder.path) in self._folders else "created"
        self._folders[tuple(folder.path)] = folder
        return folder, action

    def set_folder_visibility(self, *, path, is_public, display_name=""):
        _, _, _, folder_type, _, _ = import_shared_notebook_components()
        normalized_path = tuple(str(segment).strip() for segment in path if str(segment).strip())
        existing = self._folders.get(normalized_path)
        folder = folder_type(
            path=normalized_path,
            display_name=display_name or (existing.name if existing else normalized_path[-1]),
            is_public=is_public,
            can_edit=True if existing is None else existing.can_edit,
            can_delete=True if existing is None else existing.can_delete,
            updated_at="2026-05-12T00:00:00+00:00",
            version=1 if existing is None else existing.version + 1,
        )
        return self.upsert_folder(folder)


class FakeS3MissingObject(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class FakeSharedNotebookS3Client:
    def __init__(self):
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = bytes(Body)
        return {}

    def get_object(self, *, Bucket, Key):
        try:
            payload = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise FakeS3MissingObject() from exc
        return {"Body": BytesIO(payload)}

    def head_object(self, *, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise FakeS3MissingObject()
        return {}

    def delete_object(self, *, Bucket, Key):
        self.objects.pop((Bucket, Key), None)
        return {}

    def list_objects_v2(self, *, Bucket, Prefix="", MaxKeys=1000, ContinuationToken=None):
        keys = sorted(
            key
            for bucket, key in self.objects
            if bucket == Bucket and key.startswith(Prefix)
        )
        return {
            "Contents": [{"Key": key} for key in keys[:MaxKeys]],
            "IsTruncated": False,
        }


class FakeSharedNotebookS3Settings:
    s3_bucket = "workspace"
    shared_notebooks_bucket = "shared-notebooks"
    s3_endpoint = "http://127.0.0.1:9000"

    def current_s3_access_key_id(self):
        return "access-key"

    def current_s3_secret_access_key(self):
        return "secret-key"


def build_shared_notebook_service(existing_notebooks=None, existing_folders=None):
    WorkbenchService, _, _, _, _, _ = import_shared_notebook_components()
    service = WorkbenchService.__new__(WorkbenchService)
    service._lock = threading.RLock()
    service._condition = threading.Condition()
    service._shared_notebook_store = InMemorySharedNotebookStore(
        existing_notebooks,
        existing_folders,
    )
    rebuild_calls: list[str] = []
    appended_events: list[dict[str, object]] = []
    service._rebuild_notebooks_locked = lambda: rebuild_calls.append("rebuild")
    service._append_notebook_event_locked = (
        lambda **kwargs: appended_events.append(kwargs)
    )
    return service, rebuild_calls, appended_events


class SharedNotebookServiceTests(unittest.TestCase):
    def test_folder_manifest_serialization_round_trip(self) -> None:
        _, _, _, folder_type, deserialize_folder, serialize_folder = (
            import_shared_notebook_components()
        )
        folder = folder_type(
            path=("Team", "Analysis"),
            display_name="Analysis",
            is_public=True,
            can_edit=False,
            can_delete=False,
            updated_at="2026-05-12T08:00:00+00:00",
            version=3,
        )

        serialized = serialize_folder(folder)
        restored = deserialize_folder(serialized)

        self.assertIsNotNone(restored)
        self.assertEqual(restored.path, ("Team", "Analysis"))
        self.assertEqual(restored.name, "Analysis")
        self.assertTrue(restored.is_public)
        self.assertFalse(restored.can_edit)
        self.assertFalse(restored.can_delete)
        self.assertEqual(restored.version, 3)

    def test_upsert_shared_notebook_normalizes_defaults_and_emits_event(
        self,
    ) -> None:
        service, rebuild_calls, appended_events = (
            build_shared_notebook_service()
        )

        result = service.upsert_shared_notebook(
            notebook_id=" ",
            title=" ",
            summary=" ",
            tags=[" analysis ", "", "vat"],
            tree_path=["", " "],
            linked_generator_id=" loader-a ",
            pipeline_mode="pipeline",
            created_at="2026-04-14T10:00:00+00:00",
            cells=[
                {
                    "sql": "select 1",
                    "dataSources": [
                        " pg_oltp.public.tax_assessment ",
                        "",
                        "workspace.s3.vat_smoke",
                    ],
                    "queryOptions": {
                        "duckdb": {"parquetHivePartitioning": "on"},
                    },
                    "stage": {
                        "enabled": True,
                        "stageId": "stage-raw",
                        "alias": "raw",
                        "title": "Raw",
                        "predecessorStageIds": [],
                    },
                },
                "ignored",
            ],
            versions=[
                {
                    "title": "",
                    "summary": "",
                    "tags": [" saved ", ""],
                    "cells": [
                        {
                            "sql": "select 2",
                            "dataSources": [" workspace.s3.vat_smoke ", ""],
                            "queryOptions": {
                                "duckdb": {"parquetHivePartitioning": "off"},
                            },
                            "stage": {
                                "enabled": True,
                                "stageId": "stage-saved",
                                "alias": "saved",
                                "title": "Saved",
                                "predecessorStageIds": ["stage-raw"],
                            },
                        }
                    ],
                }
            ],
            origin_client_id="client-1",
        )

        notebook = result["notebook"]

        self.assertEqual(result["action"], "created")
        self.assertTrue(notebook["notebookId"].startswith("shared-notebook-"))
        self.assertEqual(notebook["title"], "Untitled Notebook")
        self.assertEqual(notebook["summary"], "Describe this notebook.")
        self.assertEqual(notebook["treePath"], ["Shared Notebooks"])
        self.assertEqual(notebook["tags"], ["analysis", "vat"])
        self.assertEqual(notebook["linkedGeneratorId"], "loader-a")
        self.assertEqual(notebook["pipelineMode"], "pipeline")
        self.assertEqual(notebook["createdAt"], "2026-04-14T10:00:00+00:00")
        self.assertEqual(
            notebook["cells"][0]["dataSources"],
            ["pg_oltp.public.tax_assessment", "workspace.s3.vat_smoke"],
        )
        self.assertEqual(
            notebook["cells"][0]["queryOptions"]["duckdb"]["parquetHivePartitioning"],
            "on",
        )
        self.assertEqual(
            notebook["cells"][0]["queryOptions"]["duckdb"]["cacheHydration"]["mode"],
            "off",
        )
        self.assertEqual(notebook["cells"][0]["language"], "sql")
        self.assertEqual(notebook["cells"][0]["stage"]["stageId"], "stage-raw")
        self.assertEqual(notebook["cells"][0]["stage"]["alias"], "raw")
        self.assertTrue(
            notebook["cells"][0]["cellId"].startswith("shared-cell-")
        )
        self.assertEqual(notebook["versions"][0]["title"], "Untitled Notebook")
        self.assertEqual(
            notebook["versions"][0]["summary"],
            "Describe this notebook.",
        )
        self.assertEqual(notebook["versions"][0]["tags"], ["saved"])
        self.assertEqual(
            notebook["versions"][0]["cells"][0]["dataSources"],
            ["workspace.s3.vat_smoke"],
        )
        self.assertEqual(
            notebook["versions"][0]["cells"][0]["queryOptions"]["duckdb"][
                "parquetHivePartitioning"
            ],
            "off",
        )
        self.assertEqual(
            notebook["versions"][0]["cells"][0]["queryOptions"]["duckdb"][
                "cacheHydration"
            ]["mode"],
            "off",
        )
        self.assertEqual(notebook["versions"][0]["cells"][0]["language"], "sql")
        self.assertEqual(
            notebook["versions"][0]["cells"][0]["stage"]["predecessorStageIds"],
            ["stage-raw"],
        )
        self.assertTrue(
            notebook["versions"][0]["versionId"].startswith("shared-version-")
        )
        self.assertTrue(
            notebook["versions"][0]["cells"][0]["cellId"].startswith(
                "shared-cell-"
            )
        )
        self.assertEqual(rebuild_calls, ["rebuild"])
        self.assertEqual(len(appended_events), 1)
        self.assertEqual(appended_events[0]["event_type"], "created")
        self.assertEqual(appended_events[0]["origin_client_id"], "client-1")

    def test_upsert_reuses_existing_tree_path_and_delete_emits_deleted_event(
        self,
    ) -> None:
        _, notebook_cell_type, notebook_type, _, _, _ = (
            import_shared_notebook_components()
        )
        existing_notebook = notebook_type(
            notebook_id="shared-notebook-a",
            title="Original Title",
            summary="Original Summary",
            cells=[notebook_cell_type(cell_id="cell-a", sql="select 1")],
            tree_path=("Pinned", "Team A"),
            shared=True,
            created_at="2025-01-01T00:00:00+00:00",
        )
        service, rebuild_calls, appended_events = (
            build_shared_notebook_service([existing_notebook])
        )

        updated = service.upsert_shared_notebook(
            notebook_id="shared-notebook-a",
            title=" Updated Notebook ",
            summary=" Updated Summary ",
            tags=[" shared ", ""],
            tree_path=[],
            linked_generator_id="",
            cells=[],
            versions=[],
            origin_client_id="client-2",
        )

        notebook = updated["notebook"]

        self.assertEqual(updated["action"], "updated")
        self.assertEqual(notebook["notebookId"], "shared-notebook-a")
        self.assertEqual(notebook["title"], "Updated Notebook")
        self.assertEqual(notebook["summary"], "Updated Summary")
        self.assertEqual(notebook["treePath"], ["Pinned", "Team A"])
        self.assertEqual(notebook["createdAt"], "2025-01-01T00:00:00+00:00")
        self.assertEqual(notebook["tags"], ["shared"])
        self.assertEqual(len(notebook["cells"]), 1)
        self.assertTrue(
            notebook["cells"][0]["cellId"].startswith("shared-cell-")
        )
        self.assertEqual(
            notebook["versions"][0]["versionId"],
            "initial-shared-notebook-a",
        )

        deleted = service.delete_shared_notebook(
            "shared-notebook-a",
            origin_client_id="client-2",
        )

        self.assertEqual(deleted["action"], "deleted")
        self.assertEqual(
            deleted["notebook"]["notebookId"],
            "shared-notebook-a",
        )
        self.assertEqual(rebuild_calls, ["rebuild", "rebuild"])
        self.assertEqual(
            [event["event_type"] for event in appended_events],
            ["updated", "deleted"],
        )
        self.assertEqual(appended_events[1]["origin_client_id"], "client-2")

    def test_shared_notebook_serialization_round_trips_pipeline_paths(self) -> None:
        _, notebook_cell_type, notebook_type, _, _, _ = import_shared_notebook_components()
        from bit_data_workbench.backend.shared_notebooks import (
            deserialize_notebook,
            serialize_notebook,
        )

        notebook = notebook_type(
            notebook_id="shared-notebook-priority",
            title="Priority pipeline",
            summary="Forked priority paths",
            cells=[notebook_cell_type(cell_id="cell-a", sql="select 1")],
            pipeline_mode="pipeline",
            pipeline_paths=[
                {
                    "pathId": "path-stage-status_pressure",
                    "terminalStageId": "stage-status_pressure",
                    "label": "Status Pressure",
                    "priority": 2,
                },
                {
                    "path_id": "path-stage-audit_backlog",
                    "terminal_stage_id": "stage-audit_backlog",
                    "name": "Audit Backlog",
                    "rank": 1,
                },
            ],
            shared=True,
        )

        serialized = serialize_notebook(notebook)
        restored = deserialize_notebook(serialized)

        self.assertEqual(
            serialized["pipelinePaths"],
            [
                {
                    "pathId": "path-stage-audit_backlog",
                    "terminalStageId": "stage-audit_backlog",
                    "label": "Audit Backlog",
                    "priority": 1,
                },
                {
                    "pathId": "path-stage-status_pressure",
                    "terminalStageId": "stage-status_pressure",
                    "label": "Status Pressure",
                    "priority": 2,
                },
            ],
        )
        self.assertIsNotNone(restored)
        self.assertEqual(restored.pipeline_paths, serialized["pipelinePaths"])

    def test_s3_shared_notebook_store_round_trips_pipeline_paths(self) -> None:
        _, notebook_cell_type, notebook_type, _, _, _ = import_shared_notebook_components()
        from bit_data_workbench.backend.shared_notebooks import S3SharedNotebookStore

        fake_s3 = FakeSharedNotebookS3Client()
        store = S3SharedNotebookStore(
            FakeSharedNotebookS3Settings(),
            s3_client_factory=lambda _settings: fake_s3,
            ensure_bucket=lambda _settings, _bucket: None,
        )
        store.initialize()

        notebook = notebook_type(
            notebook_id="shared-notebook-s3-priority",
            title="S3 priority pipeline",
            summary="Pipeline path metadata in S3",
            cells=[notebook_cell_type(cell_id="cell-a", sql="select 1")],
            pipeline_mode="pipeline",
            pipeline_paths=[
                {
                    "pathId": "path-stage-audit",
                    "terminalStageId": "stage-audit",
                    "label": "Audit first",
                    "priority": 1,
                },
                {
                    "pathId": "path-stage-status",
                    "terminalStageId": "stage-status",
                    "label": "Status second",
                    "priority": 2,
                },
            ],
            shared=True,
        )

        refreshed, action = store.upsert_notebook(notebook)
        stored_notebook_payloads = [
            json.loads(payload.decode("utf-8"))
            for (_bucket, key), payload in fake_s3.objects.items()
            if key.startswith("notebooks/")
        ]
        restored = store.list_notebooks()

        self.assertEqual(action, "created")
        self.assertEqual(refreshed.pipeline_paths, notebook.pipeline_paths)
        self.assertEqual(len(stored_notebook_payloads), 1)
        self.assertEqual(
            stored_notebook_payloads[0]["pipelinePaths"],
            notebook.pipeline_paths,
        )
        self.assertEqual(len(restored), 1)
        self.assertEqual(restored[0].pipeline_paths, notebook.pipeline_paths)

    def test_startup_seed_reestablishes_editable_mwa_parquet_pipeline(
        self,
    ) -> None:
        _, notebook_cell_type, notebook_type, _, _, _ = (
            import_shared_notebook_components()
        )
        from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema

        existing_seed = notebook_type(
            notebook_id="mwa-abrechnung-s3-parquet-pipeline",
            title="User edited seed",
            summary="Changed during a session",
            cells=[
                notebook_cell_type(
                    cell_id="edited-cell",
                    sql="select 99 as user_edit",
                )
            ],
            tree_path=(
                "PoC Tests",
                "Performance Evaluation",
                "MWA Abrechnung (3.2)",
            ),
            pipeline_mode="pipeline",
            pipeline_paths=[
                {
                    "pathId": "path-stage-mwa-audit-backlog",
                    "terminalStageId": "stage-mwa-audit-backlog",
                    "label": "Audit first",
                    "priority": 1,
                },
                {
                    "pathId": "path-stage-deleted-terminal",
                    "terminalStageId": "stage-deleted-terminal",
                    "label": "Deleted terminal",
                    "priority": 2,
                },
                {
                    "pathId": "path-stage-mwa-status-pressure",
                    "terminalStageId": "stage-mwa-status-pressure",
                    "label": "Status second",
                    "priority": 3,
                },
            ],
            can_edit=True,
            can_delete=True,
            shared=True,
        )
        service, rebuild_calls, appended_events = build_shared_notebook_service(
            [existing_seed],
        )
        service._catalogs = [
            SourceCatalog(
                name="workspace",
                schemas=[
                    SourceSchema(
                        name="mwa",
                        objects=[
                            SourceObject(
                                name="mwa_abrechnung_entities_parquet",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnung_entities_parquet",
                            ),
                            SourceObject(
                                name="mwa_abrechnungs_ziffern_entities_parquet",
                                kind="view",
                                relation=(
                                    "workspace.mwa."
                                    "mwa_abrechnungs_ziffern_entities_parquet"
                                ),
                            ),
                        ],
                    )
                ],
            )
        ]

        service._ensure_startup_shared_notebook_seeds()

        seeded = service._shared_notebook_store.list_notebooks()[0]
        self.assertEqual(
            seeded.notebook_id,
            "mwa-abrechnung-s3-parquet-pipeline",
        )
        self.assertEqual(
            seeded.title,
            "MWA Abrechnung (3.2) S3 Parquet Pipeline",
        )
        self.assertTrue(seeded.can_edit)
        self.assertTrue(seeded.can_delete)
        self.assertTrue(seeded.shared)
        self.assertEqual(seeded.pipeline_mode, "pipeline")
        self.assertEqual(
            seeded.tree_path,
            ("PoC Tests", "Performance Evaluation", "Data Pipelines"),
        )
        self.assertEqual(
            seeded.pipeline_paths,
            [
                {
                    "pathId": "path-stage-mwa-audit-backlog",
                    "terminalStageId": "stage-mwa-audit-backlog",
                    "label": "Audit first",
                    "priority": 1,
                },
                {
                    "pathId": "path-stage-mwa-status-pressure",
                    "terminalStageId": "stage-mwa-status-pressure",
                    "label": "Status second",
                    "priority": 2,
                },
            ],
        )
        self.assertEqual(len(seeded.cells), 5)
        all_sql = "\n".join(cell.sql for cell in seeded.cells)
        self.assertNotIn("user_edit", all_sql)
        self.assertIn("mwa_abrechnung_entities_parquet", all_sql)
        self.assertIn("mwa_abrechnungs_ziffern_entities_parquet", all_sql)
        self.assertEqual(rebuild_calls, ["rebuild"])
        self.assertEqual(appended_events, [])

    def test_startup_seed_syncs_s3_discovery_before_reestablishing_pipeline(
        self,
    ) -> None:
        from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema

        service, rebuild_calls, _ = build_shared_notebook_service()
        service._catalogs = []

        discovered_catalogs = [
            SourceCatalog(
                name="workspace",
                schemas=[
                    SourceSchema(
                        name="mwa",
                        objects=[
                            SourceObject(
                                name="mwa_abrechnung_entities_parquet",
                                kind="view",
                                relation="workspace.mwa.mwa_abrechnung_entities_parquet",
                            ),
                            SourceObject(
                                name="mwa_abrechnungs_ziffern_entities_parquet",
                                kind="view",
                                relation=(
                                    "workspace.mwa."
                                    "mwa_abrechnungs_ziffern_entities_parquet"
                                ),
                            ),
                        ],
                    )
                ],
            )
        ]

        class FakeStartupDiscovery:
            def __init__(self) -> None:
                self.calls: list[tuple[str, bool]] = []

            def sync_source(self, source_id: str, *, emit_event: bool = True):
                self.calls.append((source_id, emit_event))
                service._catalogs = discovered_catalogs
                return {}

        discovery = FakeStartupDiscovery()
        service._data_source_discovery = discovery

        service._sync_startup_seed_data_sources()
        service._ensure_startup_shared_notebook_seeds()

        self.assertEqual(discovery.calls, [("workspace.s3", False)])
        seeded = service._shared_notebook_store.list_notebooks()[0]
        self.assertEqual(len(seeded.cells), 5)
        self.assertNotIn(
            "Run MWA Loader",
            "\n".join(str(cell.stage.get("title") or "") for cell in seeded.cells),
        )
        self.assertEqual(rebuild_calls, ["rebuild"])

    def test_upsert_shared_notebook_preserves_python_cell_language(self) -> None:
        service, _, _ = build_shared_notebook_service()

        result = service.upsert_shared_notebook(
            notebook_id="shared-notebook-python",
            title="Python notebook",
            summary="Mixed runtime notebook",
            tags=[],
            tree_path=["Shared Notebooks"],
            linked_generator_id="",
            created_at="2026-04-22T10:00:00+00:00",
            cells=[
                {
                    "cellId": "cell-python",
                    "language": "python",
                    "sql": "print('hello')",
                    "dataSources": ["pg_oltp"],
                }
            ],
            versions=[
                {
                    "versionId": "version-python",
                    "createdAt": "2026-04-22T10:05:00+00:00",
                    "title": "Python version",
                    "summary": "Saved Python state",
                    "tags": [],
                    "cells": [
                        {
                            "cellId": "cell-python",
                            "language": "python",
                            "sql": "print('saved')",
                            "dataSources": ["pg_oltp"],
                        }
                    ],
                }
            ],
            origin_client_id="client-python",
        )

        notebook = result["notebook"]

        self.assertEqual(notebook["cells"][0]["language"], "python")
        self.assertEqual(notebook["versions"][0]["cells"][0]["language"], "python")

    def test_shared_folder_visibility_does_not_change_existing_notebooks(self) -> None:
        _, notebook_cell_type, notebook_type, folder_type, _, _ = (
            import_shared_notebook_components()
        )
        existing_notebook = notebook_type(
            notebook_id="local-a",
            title="Local A",
            summary="Local",
            cells=[notebook_cell_type(cell_id="cell-a", sql="select 1")],
            tree_path=("Team",),
            shared=False,
        )
        existing_folder = folder_type(
            path=("Team",),
            display_name="Team",
            is_public=False,
        )
        service, _, _ = build_shared_notebook_service(
            [existing_notebook],
            [existing_folder],
        )
        service._catalogs = []
        service._notebooks = [existing_notebook]

        result = service.set_shared_notebook_folder_visibility(
            path=["Team"],
            is_public=True,
        )

        self.assertTrue(result["folder"]["isPublic"])
        self.assertFalse(existing_notebook.shared)

    def test_shared_folder_metadata_marks_tree_folder_public(self) -> None:
        _, notebook_cell_type, notebook_type, folder_type, _, _ = (
            import_shared_notebook_components()
        )
        notebook = notebook_type(
            notebook_id="shared-a",
            title="Shared A",
            summary="Shared",
            cells=[notebook_cell_type(cell_id="cell-a", sql="select 1")],
            tree_path=("Team",),
            shared=True,
        )
        folder = folder_type(path=("Team",), display_name="Team", is_public=True)
        service, _, _ = build_shared_notebook_service([notebook], [folder])
        service._notebooks = [notebook]

        tree = service.notebook_tree()
        team_folder = next(folder for folder in tree if folder.name == "Team")

        self.assertTrue(team_folder.is_shared)
        self.assertEqual(team_folder.notebooks[0].notebook_id, "shared-a")


if __name__ == "__main__":
    unittest.main()
