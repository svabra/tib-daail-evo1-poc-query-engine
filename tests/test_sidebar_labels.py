from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys
import unittest

from starlette.requests import Request


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.web.router import sidebar_partial  # noqa: E402
from bit_data_workbench.web.template_filters import (  # noqa: E402
    format_byte_count,
    truncate_source_navigation_label,
)


def build_request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
        }
    )


class FakeWorkbenchService:
    def runtime_info(self) -> dict[str, str]:
        return {
            "service": "bit-data-workbench",
            "image_version": "0.5.3",
            "hostname": "test-host",
            "pod_name": "unknown",
            "pod_namespace": "unknown",
            "pod_ip": "unknown",
            "node_name": "unknown",
            "duckdb_database": "/tmp/workspace.duckdb",
            "timestamp_utc": "2026-04-16T00:00:00+00:00",
        }

    def catalogs(self):
        source_object = SimpleNamespace(
            kind="view",
            name="abcdefghijklmnopqrstuvwxabcdefghijklmnop_relation",
            display_name="abcdefghijklmnopqrstuvwxabcdefghijklmnop.csv",
            relation="workspace.s3.csv_imports.abcdefghijklmnopqrstuvwx",
            s3_bucket="csv-imports",
            s3_key="prefix/abcdefghijklmnopqrstuvwxabcdefghijklmnop.csv",
            s3_path="s3://csv-imports/prefix/abcdefghijklmnopqrstuvwxabcdefghijklmnop.csv",
            s3_file_format="csv",
            s3_downloadable=True,
            size_bytes=1536,
        )
        schema = SimpleNamespace(
            name="schema_with_an_intentionally_long_name",
            label="schema_with_an_intentionally_long_name",
            objects=[source_object],
        )
        return [
            SimpleNamespace(
                name="workspace",
                connection_source_id="workspace.s3",
                connection_label="Connected",
                connection_status="connected",
                connection_detail="Ready",
                connection_controls_enabled=True,
                schemas=[schema],
            )
        ]

    def notebooks(self):
        return []

    def notebook_tree(self):
        return []

    def data_generators(self):
        return []

    def runbook_tree(self):
        return []


class SidebarLabelTests(unittest.TestCase):
    def test_truncate_source_navigation_label_keeps_file_suffix(self) -> None:
        self.assertEqual(
            truncate_source_navigation_label("abcdefghijklmnopqrstuvwxabcdefghijklmnop.csv"),
            "abcdefghijklmnopqrstuvwxa[..].csv",
        )

    def test_truncate_source_navigation_label_truncates_plain_name(self) -> None:
        self.assertEqual(
            truncate_source_navigation_label("abcdefghijklmnopqrstuvwxyz1234567890"),
            "abcdefghijklmnopqrstuvwxy[..]",
        )

    def test_format_byte_count_formats_kilobytes(self) -> None:
        self.assertEqual(format_byte_count(1536), "1.5 KB")

    def test_sidebar_partial_renders_truncated_source_name_with_full_title(self) -> None:
        response = sidebar_partial(
            request=build_request("/sidebar"),
            active_notebook_id=None,
            mode="notebook",
            service=FakeWorkbenchService(),
        )

        self.assertEqual(response.status_code, 200)
        body = response.body.decode("utf-8")
        self.assertIn("abcdefghijklmnopqrstuvwxa[..].csv", body)
        self.assertIn('title="abcdefghijklmnopqrstuvwxabcdefghijklmnop.csv"', body)
        self.assertIn("CSV", body)
        self.assertIn("1.5 KB", body)

    def test_local_workspace_sidebar_action_labels_match_prompting_behavior(self) -> None:
        sidebar_renderer = (
            REPO_ROOT
            / "bdw"
            / "bit_data_workbench"
            / "static"
            / "js"
            / "local-workspace-sidebar.js"
        ).read_text(encoding="utf-8")

        self.assertIn("Move ...", sidebar_renderer)
        self.assertIn("Delete ...", sidebar_renderer)
        self.assertIn("Download", sidebar_renderer)
        self.assertNotIn("Move local file", sidebar_renderer)
        self.assertNotIn("Delete local file", sidebar_renderer)
        self.assertNotIn("Download local file", sidebar_renderer)

    def test_source_navigation_helper_uses_30_character_truncation(self) -> None:
        helper_source = (
            REPO_ROOT
            / "bdw"
            / "bit_data_workbench"
            / "static"
            / "js"
            / "source-navigation-labels.js"
        ).read_text(encoding="utf-8")

        self.assertIn("const MAX_SOURCE_NAVIGATION_STEM_CHARS = 25;", helper_source)


if __name__ == "__main__":
    unittest.main()
