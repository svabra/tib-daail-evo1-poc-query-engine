from __future__ import annotations

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.notebooks import build_notebooks  # noqa: E402
from bit_data_workbench.backend.python_execution.kernel_sessions import (  # noqa: E402
    KernelSessionManager,
)
from bit_data_workbench.backend.source_references import pg_source_reference  # noqa: E402
from bit_data_workbench.data_generator.helpers import vat_smoke_dataset_select  # noqa: E402
from bit_data_workbench.data_generator.pg_oltp_smoke import GENERATOR  # noqa: E402
from bit_data_workbench.models import (  # noqa: E402
    SourceCatalog,
    SourceObject,
    SourceSchema,
)


class PythonPresetIntegrationTests(unittest.TestCase):
    def test_pandas_and_chart_presets_execute_top_to_bottom_with_real_png(self) -> None:
        query_reference = pg_source_reference(
            source_id="pg_oltp",
            relation=f"pg_oltp.public.{GENERATOR.default_target_name}",
        )
        catalogs = [
            SourceCatalog(
                name="pg_oltp",
                connection_source_id="pg_oltp",
                schemas=[
                    SourceSchema(
                        name="public",
                        objects=[
                            SourceObject(
                                name=GENERATOR.default_target_name,
                                kind="table",
                                relation=f"pg_oltp.public.{GENERATOR.default_target_name}",
                                query_reference=query_reference,
                            )
                        ],
                    )
                ],
            )
        ]
        notebooks = {notebook.notebook_id: notebook for notebook in build_notebooks(catalogs)}
        pandas_notebook = notebooks["python-pandas-vat-demo"]
        chart_notebook = notebooks["python-chart-vat-demo"]

        with TemporaryDirectory(prefix="bdw-python-presets-") as temp_dir:
            parquet_path = Path(temp_dir) / "vat_filing_smoke_generated.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.execute(
                    "COPY ("
                    f"{vat_smoke_dataset_select(0, 1200)}"
                    ") TO ? (FORMAT PARQUET)",
                    [parquet_path.as_posix()],
                )
            finally:
                connection.close()

            physical_relation = f"read_parquet('{parquet_path.as_posix()}')"
            context = {
                "selectedSources": [
                    {
                        "sourceId": "pg_oltp",
                        "canonicalSourceId": "pg_oltp",
                        "label": "PostgreSQL OLTP",
                    }
                ],
                "relations": [
                    {
                        "sourceId": "pg_oltp",
                        "name": GENERATOR.default_target_name,
                        "displayName": GENERATOR.default_target_name,
                        "relation": physical_relation,
                        "catalogRelation": f"pg_oltp.public.{GENERATOR.default_target_name}",
                        "logicalRelation": query_reference,
                        "queryReference": query_reference,
                        "aliases": [
                            query_reference,
                            f"pg_oltp.public.{GENERATOR.default_target_name}",
                            GENERATOR.default_target_name,
                        ],
                        "fields": [],
                    }
                ],
                "localRelationMap": {},
            }

            sessions = KernelSessionManager()
            try:
                pandas_session = sessions.get_session(
                    client_id="python-preset-integration",
                    notebook_id=pandas_notebook.notebook_id,
                )
                pandas_outputs = []
                for cell in pandas_notebook.cells[1:]:
                    pandas_outputs.extend(
                        sessions.execute(
                            pandas_session,
                            code=cell.sql,
                            context=context,
                            is_cancelled=lambda: False,
                        )
                    )

                chart_session = sessions.get_session(
                    client_id="python-preset-integration",
                    notebook_id=chart_notebook.notebook_id,
                )
                chart_outputs = []
                for cell in chart_notebook.cells:
                    chart_outputs.extend(
                        sessions.execute(
                            chart_session,
                            code=cell.sql,
                            context=context,
                            is_cancelled=lambda: False,
                        )
                    )
            finally:
                sessions.shutdown_all()

        self.assertEqual(
            [output.text for output in pandas_outputs if output.output_type == "error"],
            [],
        )
        self.assertTrue(
            any(output.output_type == "table" for output in pandas_outputs),
            "The pandas preset must expose a real DataFrame result.",
        )
        self.assertEqual(
            [output.text for output in chart_outputs if output.output_type == "error"],
            [],
        )
        chart_images = [
            output
            for output in chart_outputs
            if output.output_type == "image" and output.mime_type == "image/png"
        ]
        self.assertEqual(len(chart_images), 1)
        self.assertGreater(len(str(chart_images[0].data)), 10_000)


if __name__ == "__main__":
    unittest.main()
