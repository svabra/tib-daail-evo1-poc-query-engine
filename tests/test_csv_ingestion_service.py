from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.service import WorkbenchService  # noqa: E402
from bit_data_workbench.api.router import (  # noqa: E402
    CsvUploadSessionCompletePayload,
    FileUploadSessionCompletePayload,
)
from bit_data_workbench.models import SourceCatalog, SourceObject, SourceSchema  # noqa: E402


class CsvIngestionServiceTests(TestCase):
    def test_upload_complete_payloads_default_parquet_optimization_off(self) -> None:
        expected = {
            "mode": "off",
            "hivePartitioning": False,
            "partitionColumns": [],
            "sortColumns": [],
            "createDuckdbCache": False,
            "indexColumns": [],
        }

        self.assertEqual(
            CsvUploadSessionCompletePayload().parquet_optimization.model_dump(by_alias=True),
            expected,
        )
        self.assertEqual(
            FileUploadSessionCompletePayload().parquet_optimization.model_dump(by_alias=True),
            expected,
        )

    def test_upload_complete_payload_accepts_recommended_parquet_optimization(self) -> None:
        payload = CsvUploadSessionCompletePayload.model_validate(
            {
                "targetId": "s3",
                "storageFormat": "parquet",
                "parquetOptimization": {"mode": "recommended"},
            }
        )

        self.assertEqual(
            payload.parquet_optimization.model_dump(by_alias=True),
            {
                "mode": "recommended",
                "hivePartitioning": False,
                "partitionColumns": [],
                "sortColumns": [],
                "createDuckdbCache": False,
                "indexColumns": [],
            },
        )

    def test_import_csv_files_passes_storage_format_to_ingestion_manager(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        captured_kwargs: dict[str, object] = {}

        def import_csv_files(**kwargs):
            captured_kwargs.update(kwargs)
            return {
                "targetId": "s3",
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
            target_id="s3",
            storage_format="parquet",
        )

        self.assertEqual(captured_kwargs["storage_format"], "parquet")

    def test_import_csv_files_syncs_s3_discovery_before_attaching_query_source(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._csv_ingestion = SimpleNamespace(
            import_csv_files=lambda **_kwargs: {
                "targetId": "s3",
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
                connection_source_id="s3",
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
            target_id="s3",
            bucket="vat-smoke-test",
            prefix="incoming",
            has_header=True,
            storage_format="parquet",
        )

        self.assertEqual(calls, [("s3", True)])
        self.assertEqual(payload["firstQuerySource"]["sourceId"], "s3")
        self.assertEqual(
            payload["imports"][0]["querySource"]["relation"],
            "vat_smoke_test.vat_smoke",
        )

    def test_import_csv_files_attaches_s3_query_source_from_discovery_specs(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._csv_ingestion = SimpleNamespace(
            import_csv_files=lambda **_kwargs: {
                "targetId": "s3",
                "importedCount": 1,
                "failedCount": 0,
                "imports": [
                    {
                        "fileName": "vat-smoke.csv",
                        "status": "imported",
                        "bucket": "vat-smoke-test",
                        "path": "s3://vat-smoke-test/incoming/vat-smoke.parquet",
                    }
                ],
            }
        )
        service._catalogs = []
        calls: list[tuple[str, object]] = []
        service._data_source_discovery = SimpleNamespace(
            sync_source=lambda source_id, emit_event=True: calls.append((source_id, emit_event)),
            s3_relation_specs=lambda: {
                "vat_smoke_test.vat_smoke": SimpleNamespace(
                    schema_name="vat_smoke_test",
                    relation_name="vat_smoke",
                    object_path="s3://vat-smoke-test/incoming/vat-smoke.parquet",
                    display_name="vat-smoke.parquet",
                )
            },
        )
        service.refresh_metadata_state = lambda: calls.append(("refresh", None))

        payload = service.import_csv_files(
            files=[],
            target_id="s3",
            bucket="vat-smoke-test",
            prefix="incoming",
            storage_format="parquet",
        )

        self.assertEqual(calls, [("s3", True)])
        self.assertEqual(payload["firstQuerySource"]["sourceId"], "s3")
        self.assertEqual(
            payload["imports"][0]["querySource"]["relation"],
            "vat_smoke_test.vat_smoke",
        )
        self.assertEqual(
            payload["imports"][0]["querySource"]["queryAlias"],
            "s3.vat_smoke_test.incoming.vat_smoke.parquet",
        )
        self.assertNotIn("queryUnavailableReason", payload["imports"][0])

    def test_import_csv_files_waits_for_delayed_s3_query_source(self) -> None:
        service = WorkbenchService.__new__(WorkbenchService)
        service._csv_ingestion = SimpleNamespace(
            import_csv_files=lambda **_kwargs: {
                "targetId": "s3",
                "importedCount": 1,
                "failedCount": 0,
                "imports": [
                    {
                        "fileName": "federal_tax_data_10gb.csv",
                        "status": "imported",
                        "bucket": "test",
                        "path": "s3://test/federal_tax_data_10gb.csv",
                    }
                ],
            }
        )
        service._catalogs = []
        calls: list[tuple[str, object]] = []
        specs: dict[str, SimpleNamespace] = {}

        def sync_source(source_id, emit_event=True):
            calls.append((source_id, emit_event))
            if len(calls) == 2:
                specs["test.federal_tax_data_10gb"] = SimpleNamespace(
                    schema_name="test",
                    relation_name="federal_tax_data_10gb",
                    object_path="s3://test/federal_tax_data_10gb.csv",
                    display_name="federal_tax_data_10gb.csv",
                )

        service._data_source_discovery = SimpleNamespace(
            sync_source=sync_source,
            s3_relation_specs=lambda: dict(specs),
        )
        service.refresh_metadata_state = lambda: calls.append(("refresh", None))

        with patch("bit_data_workbench.backend.service.time.sleep", lambda _seconds: None):
            payload = service.import_csv_files(
                files=[],
                target_id="s3",
                bucket="test",
                storage_format="csv",
            )

        self.assertEqual(calls, [("s3", True), ("s3", True)])
        self.assertEqual(payload["firstQuerySource"]["sourceId"], "s3")
        self.assertEqual(
            payload["imports"][0]["querySource"]["relation"],
            "test.federal_tax_data_10gb",
        )
        self.assertEqual(
            payload["imports"][0]["querySource"]["queryAlias"],
            "s3.test.federal_tax_data_10gb.csv",
        )
        self.assertNotIn("queryUnavailableReason", payload["imports"][0])

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
