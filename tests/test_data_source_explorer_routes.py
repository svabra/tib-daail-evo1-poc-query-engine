from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.api.data_source_explorer import (  # noqa: E402
    data_source_explorer_payload,
)
from bit_data_workbench.models import (  # noqa: E402
    SourceCatalog,
    SourceObject,
    SourceSchema,
)
from bit_data_workbench.web.router import (  # noqa: E402
    index,
    query_runs_page,
    query_workbench_data_source_explorer,
    query_workbench_data_sources,
    sidebar_partial,
)


def build_request(path: str, *, partial: bool = False) -> Request:
    headers = [(b"host", b"testserver")]
    if partial:
        headers.append((b"hx-request", b"true"))
    return Request(
        {
            "type": "http",
            "scheme": "http",
            "method": "GET",
            "path": path,
            "headers": headers,
            "server": ("testserver", 80),
        }
    )


class FakeWorkbenchService:
    def __init__(self) -> None:
        self.settings = type(
            "Settings",
            (),
            {
                "pg_host": "localhost",
                "pg_port": "5432",
                "pg_user": "evo1",
                "pg_password": "evo1",
                "pg_oltp_database": "evo1_oltp",
                "pg_olap_database": "evo1_olap",
                "s3_endpoint": "localhost:9000",
                "s3_bucket": "shared-finance",
                "s3_url_style": "path",
                "s3_use_ssl": False,
                "s3_verify_ssl": False,
                "current_s3_access_key_id": lambda self: "minioadmin",
                "current_s3_secret_access_key": lambda self: "minioadmin",
                "effective_s3_ca_cert_file": lambda self: None,
            },
        )()

    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": "0.6.0",
            "hostname": "test-host",
            "pod_name": "bdw-pod",
            "pod_namespace": "bdw-namespace",
            "pod_ip": "127.0.0.1",
            "node_name": "bdw-node",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-04-21T00:00:00+00:00",
        }

    def catalogs(self) -> list[SourceCatalog]:
        return [
            SourceCatalog(
                name="workspace",
                connection_source_id="workspace.s3",
                connection_status="connected",
                connection_label="Connected",
                connection_detail="Shared Workspace is connected.",
                connection_controls_enabled=True,
                schemas=[
                    SourceSchema(
                        name="shared-finance",
                        label="shared-finance",
                        objects=[
                            SourceObject(
                                name="orders.csv",
                                kind="file",
                                relation="workspace.shared_finance.orders",
                                s3_bucket="shared-finance",
                                s3_key="exports/orders.csv",
                                s3_path="s3://shared-finance/exports/orders.csv",
                                s3_file_format="csv",
                                s3_downloadable=True,
                                size_bytes=1024,
                            )
                        ],
                    )
                ],
            ),
            SourceCatalog(
                name="pg_oltp",
                connection_source_id="pg_oltp",
                connection_status="connected",
                connection_label="Connected",
                connection_detail="PostgreSQL OLTP is connected.",
                connection_controls_enabled=True,
                schemas=[
                    SourceSchema(
                        name="public",
                        objects=[
                            SourceObject(
                                name="orders",
                                kind="table",
                                relation="pg_oltp.public.orders",
                            )
                        ],
                    )
                ],
            ),
            SourceCatalog(
                name="pg_olap",
                connection_source_id="pg_olap",
                connection_status="connected",
                connection_label="Connected",
                connection_detail="PostgreSQL OLAP is connected.",
                connection_controls_enabled=True,
                schemas=[
                    SourceSchema(
                        name="mart",
                        objects=[
                            SourceObject(
                                name="orders_summary",
                                kind="view",
                                relation="pg_olap.mart.orders_summary",
                            )
                        ],
                    )
                ],
            ),
            SourceCatalog(
                name="workspace_local",
                connection_source_id="workspace.local",
                connection_status="connected",
                connection_label="Available",
                connection_detail="Local Workspace is available.",
                schemas=[],
            ),
        ]

    def notebooks(self):
        return []

    def notebook_tree(self):
        return []

    def source_options(self):
        return []

    def data_generators(self):
        return []

    def runbook_tree(self):
        return []

    def completion_schema(self):
        return {}

    def data_source_explorer_payload(
        self,
        *,
        source_id: str,
        bucket: str = "",
        prefix: str = "",
    ) -> dict[str, object]:
        normalized_source_id = str(source_id or "").strip()
        if normalized_source_id in {"pg_oltp", "pg_oltp_native"}:
            return {
                "sourceId": "pg_oltp",
                "explorerKind": "postgres",
                "defaultRelation": "pg_oltp.public.orders",
                "schemas": [
                    {
                        "name": "public",
                        "label": "public",
                        "objectCount": 1,
                        "objects": [
                            {
                                "name": "orders",
                                "displayName": "orders",
                                "kind": "table",
                                "relation": "pg_oltp.public.orders",
                            }
                        ],
                    }
                ],
            }

        if normalized_source_id == "workspace.s3":
            return {
                "sourceId": "workspace.s3",
                "explorerKind": "s3",
                "snapshot": {
                    "bucket": bucket,
                    "prefix": prefix,
                    "path": "s3://shared-finance/",
                    "entries": [],
                    "breadcrumbs": [{"label": "Buckets", "bucket": "", "prefix": ""}],
                    "canCreateBucket": True,
                    "canCreateFolder": False,
                    "emptyMessage": "No buckets are available.",
                },
            }

        if normalized_source_id == "workspace.local":
            return {
                "sourceId": "workspace.local",
                "explorerKind": "local-workspace",
            }

        raise KeyError(f"Unsupported source: {source_id}")


class DataSourceExplorerRouteTests(unittest.TestCase):
    def test_home_partial_renders_clickable_data_source_links(self) -> None:
        response = index(
            request=build_request("/", partial=True),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-home-data-source-link', body)
        self.assertIn('data-open-query-data-source="workspace.local"', body)
        self.assertIn(
            'href="/data-sources?source_id=workspace.local"',
            body,
        )

    def test_management_page_uses_standalone_url_and_sidebar_browse_launcher(self) -> None:
        response = query_workbench_data_sources(
            request=build_request("/data-sources", partial=True),
            source_id="pg_olap",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-data-source-management-page', body)
        self.assertIn('data-selected-source-id="pg_olap"', body)
        self.assertIn("Browse Data", body)
        self.assertIn('data-browse-data-source="pg_olap"', body)
        self.assertIn('data-browse-sidebar-source-id="pg_olap"', body)
        self.assertIn(
            'href="/data-sources?source_id=pg_olap&amp;browse=1"',
            body,
        )
        self.assertNotIn('data-data-source-explorer-page', body)
        self.assertNotIn('data-inline-source-browser', body)

    def test_management_full_page_hides_global_sidebar(self) -> None:
        response = query_workbench_data_sources(
            request=build_request("/data-sources"),
            source_id="workspace.s3",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('class="shell shell-sidebar-hidden"', body)

    def test_browse_mode_renders_inline_browser_for_each_source(self) -> None:
        cases = [
            ("workspace.s3", "workspace.s3"),
            ("workspace.local", "workspace.local"),
            ("pg_oltp", "pg_oltp"),
            ("pg_olap", "pg_olap"),
            ("pg_oltp_native", "pg_oltp"),
        ]

        for source_id, sidebar_source_id in cases:
            with self.subTest(source_id=source_id):
                response = query_workbench_data_sources(
                    request=build_request("/data-sources", partial=True),
                    source_id=source_id,
                    browse=True,
                    service=FakeWorkbenchService(),
                )

                self.assertEqual(response.status_code, 200)
                body = response.body.decode("utf-8")
                self.assertIn('data-inline-source-browser', body)
                self.assertIn(f'data-browse-source-id="{source_id}"', body)
                self.assertIn(
                    f'data-browse-sidebar-source-id="{sidebar_source_id}"',
                    body,
                )
                self.assertNotIn('data-data-source-explorer-page', body)

    def test_inline_native_postgres_browser_preserves_native_source_id(self) -> None:
        response = query_workbench_data_sources(
            request=build_request("/data-sources", partial=True),
            source_id="pg_oltp_native",
            browse=True,
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-inline-source-browser', body)
        self.assertIn('data-source-catalog-source-id="pg_oltp"', body)
        self.assertIn('data-source-option-id="pg_oltp_native"', body)

    def test_query_workbench_sidebar_keeps_full_data_source_navigation(self) -> None:
        response = sidebar_partial(
            request=build_request("/sidebar", partial=True),
            mode="notebook",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-data-sources-section', body)
        self.assertIn('data-source-catalog-source-id="workspace.s3"', body)
        self.assertIn('data-source-catalog-source-id="pg_oltp"', body)
        self.assertIn('data-source-catalog-source-id="pg_olap"', body)

    def test_browser_route_reuses_management_page_and_sidebar_source_mapping(self) -> None:
        response = query_workbench_data_source_explorer(
            request=build_request(
                "/data-sources/browser",
                partial=True,
            ),
            source_id="pg_oltp_native",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-data-source-management-page', body)
        self.assertIn('data-selected-source-id="pg_oltp_native"', body)
        self.assertIn('data-browse-source-id="pg_oltp_native"', body)
        self.assertIn('data-browse-sidebar-source-id="pg_oltp"', body)
        self.assertIn('data-browse-data-source="pg_oltp_native"', body)
        self.assertIn('data-open-query-data-source="pg_oltp_native"', body)
        self.assertIn(
            'href="/data-sources?source_id=pg_oltp_native"',
            body,
        )
        self.assertNotIn('data-data-source-explorer-page', body)

    def test_query_runs_page_renders_history_shell(self) -> None:
        response = query_runs_page(
            request=build_request("/query-workbench/query-runs", partial=True),
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn('data-query-runs-page', body)
        self.assertIn('data-query-runs-list', body)

    def test_explorer_api_returns_postgres_catalog_payload(self) -> None:
        response = data_source_explorer_payload(
            source_id="pg_oltp_native",
            bucket="",
            prefix="",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body.decode("utf-8"))
        self.assertEqual(payload["explorerKind"], "postgres")
        self.assertEqual(payload["sourceId"], "pg_oltp")
        self.assertEqual(payload["defaultRelation"], "pg_oltp.public.orders")
        self.assertEqual(payload["schemas"][0]["objects"][0]["kind"], "table")


if __name__ == "__main__":
    unittest.main()
