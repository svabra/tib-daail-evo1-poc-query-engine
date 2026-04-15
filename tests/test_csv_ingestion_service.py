from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import TestCase


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402


class CsvIngestionServiceTests(TestCase):
    def test_import_csv_files_passes_storage_format_to_ingestion_manager(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        captured_kwargs: dict[str, object] = {}

        def import_csv_files(**kwargs):
            captured_kwargs.update(kwargs)
            return {
                "targetId": "workspace.s3",
                "importedCount": 0,
                "failedCount": 0,
                "imports": [],
            }

        service._csv_ingestion = SimpleNamespace(import_csv_files=import_csv_files)
        service._catalogs = []
        service._data_source_discovery = SimpleNamespace(sync_source=lambda *_args, **_kwargs: None)
        service.refresh_metadata_state = lambda: None

        service.import_csv_files(
            files=[],
            target_id="workspace.s3",
            storage_format="parquet",
        )

        self.assertEqual(captured_kwargs["storage_format"], "parquet")

    def test_import_csv_files_syncs_s3_discovery_before_attaching_query_source(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._csv_ingestion = SimpleNamespace(
            import_csv_files=lambda **_kwargs: {
                "targetId": "workspace.s3",
                "importedCount": 1,
                "failedCount": 0,
                "imports": [
                    {
                        "fileName": "vat-smoke.csv",
                        "status": "imported",
                        "path": "s3://vat-smoke-test/incoming/vat-smoke.csv",
                    }
                ],
            }
        )
        service._catalogs = [
            SourceCatalog(
                name="workspace",
                connection_source_id="workspace.s3",
                schemas=[
                    SourceSchema(
                        name="vat_smoke_test",
                        label="vat-smoke-test",
                        objects=[
                            SourceObject(
                                name="vat_smoke",
                                kind="view",
                                relation="vat_smoke_test.vat_smoke",
                                s3_bucket="vat-smoke-test",
                                s3_key="incoming/vat-smoke.csv",
                                s3_path="s3://vat-smoke-test/incoming/vat-smoke.csv",
                                s3_file_format="csv",
                                s3_downloadable=True,
                            )
                        ],
                    )
                ],
            )
        ]
        calls: list[tuple[str, object]] = []
        service._data_source_discovery = SimpleNamespace(
            sync_source=lambda source_id, emit_event=True: calls.append((source_id, emit_event))
        )
        service.refresh_metadata_state = lambda: calls.append(("refresh", None))

        payload = service.import_csv_files(
            files=[],
            target_id="workspace.s3",
            bucket="vat-smoke-test",
            prefix="incoming",
            has_header=True,
            storage_format="parquet",
        )

        self.assertEqual(calls, [("workspace.s3", True)])
        self.assertEqual(payload["firstQuerySource"]["sourceId"], "workspace.s3")
        self.assertEqual(
            payload["imports"][0]["querySource"]["relation"],
            "vat_smoke_test.vat_smoke",
        )

    def test_import_csv_files_refreshes_metadata_for_postgres_targets(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._csv_ingestion = SimpleNamespace(
            import_csv_files=lambda **_kwargs: {
                "targetId": "pg_oltp",
                "importedCount": 1,
                "failedCount": 0,
                "imports": [
                    {
                        "fileName": "vat-smoke.csv",
                        "status": "imported",
                        "relation": "public.raw_vat_smoke",
                    }
                ],
            }
        )
        service._catalogs = [
            SourceCatalog(
                name="pg_oltp",
                connection_source_id="pg_oltp",
                schemas=[
                    SourceSchema(
                        name="public",
                        objects=[
                            SourceObject(
                                name="raw_vat_smoke",
                                kind="table",
                                relation="pg_oltp.public.raw_vat_smoke",
                            )
                        ],
                    )
                ],
            )
        ]
        calls: list[tuple[str, object]] = []
        service._data_source_discovery = SimpleNamespace(
            sync_source=lambda source_id, emit_event=True: calls.append((source_id, emit_event))
        )
        service.refresh_metadata_state = lambda: calls.append(("refresh", None))

        payload = service.import_csv_files(
            files=[],
            target_id="pg_oltp",
            schema_name="public",
            storage_format="csv",
        )

        self.assertEqual(calls, [("refresh", None)])
        self.assertEqual(payload["firstQuerySource"]["sourceId"], "pg_oltp")
