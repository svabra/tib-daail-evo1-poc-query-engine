from __future__ import annotations

import base64
import csv
from dataclasses import replace
from decimal import Decimal, ROUND_HALF_UP
import io
from pathlib import Path
from types import SimpleNamespace
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
if str(BDW_ROOT) not in sys.path:
    sys.path.insert(0, str(BDW_ROOT))


from bit_data_workbench.backend.notebook_presets import (  # noqa: E402
    build_data_analysts_journey_aggregation_sql,
    build_data_analysts_journey_chart_python,
    build_data_analysts_journey_notebook,
)
from bit_data_workbench.backend.python_execution.kernel_sessions import (  # noqa: E402
    KernelSessionManager,
)
from bit_data_workbench.data_generator.base import DataGeneratorContext  # noqa: E402
from bit_data_workbench.data_generator.data_analysts_journey import (  # noqa: E402
    GENERATOR,
    JOURNEY_AS_OF_DATE,
    JOURNEY_BUCKET,
    JOURNEY_COLUMNS,
    JOURNEY_ELECTRONIC_PREFIX,
    JOURNEY_EXPECTED_AGGREGATE_ROWS,
    JOURNEY_EXPECTED_ELECTRONIC_ROWS,
    JOURNEY_EXPECTED_UNION_ROWS,
    JOURNEY_GENERATOR_ID,
    JOURNEY_MANUAL_FILE_NAME,
    JOURNEY_MANUAL_PATH,
    JOURNEY_NOTEBOOK_ID,
    JOURNEY_POSTGRES_COLUMNS,
    JOURNEY_POSTGRES_QUERY_REFERENCE,
    JOURNEY_POSTGRES_RELATION,
    JOURNEY_PRODUCT_PATH,
    JourneyRow,
    assert_postgres_table_schema,
    journey_rows,
    validate_journey_rows,
)
from bit_data_workbench.data_generator.registry import DataGeneratorRegistry  # noqa: E402


CSV_PATH = (
    BDW_ROOT
    / "bit_data_workbench"
    / "static"
    / "data"
    / JOURNEY_MANUAL_FILE_NAME
)


def rows_frame(rows: list[JourneyRow]) -> pd.DataFrame:
    return pd.DataFrame([dict(zip(JOURNEY_COLUMNS, row.values)) for row in rows])


def aggregate_frame(electronic: pd.DataFrame, aargau_relation: str) -> pd.DataFrame:
    connection = duckdb.connect(":memory:")
    try:
        connection.register("electronic", electronic)
        return connection.execute(
            build_data_analysts_journey_aggregation_sql(
                "electronic",
                aargau_relation,
            )
        ).fetch_df()
    finally:
        connection.close()


class RecordingJourneyConnection:
    def __init__(self, *, schema_columns: tuple[str, ...] | None = None) -> None:
        self._duckdb = duckdb.connect(":memory:")
        self.schema_columns = schema_columns or tuple(
            column.split(" ", 1)[0] for column in JOURNEY_POSTGRES_COLUMNS
        )
        self.executed: list[tuple[str, object]] = []
        self._rows: list[tuple[object, ...]] = []

    def execute(self, sql: str, parameters=None):
        self.executed.append((str(sql), parameters))
        normalized = " ".join(str(sql).split()).lower()
        self._rows = []
        if "from information_schema.columns" in normalized:
            self._rows = [(column,) for column in self.schema_columns]
        elif normalized.startswith("select count(*) from"):
            self._rows = [(JOURNEY_EXPECTED_ELECTRONIC_ROWS,)]
        elif normalized.startswith("copy ("):
            self._duckdb.execute(sql)
        return self

    def fetchall(self):
        return list(self._rows)

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self) -> None:
        self._duckdb.close()


class DataAnalystsJourneyDataTests(unittest.TestCase):
    def test_generator_and_bundled_csv_contract(self) -> None:
        generator = DataGeneratorRegistry().generator(JOURNEY_GENERATOR_ID)
        definition = generator.definition().payload
        self.assertIs(generator, GENERATOR)
        self.assertEqual(definition["title"], "a Data Analyst's Journey")
        self.assertEqual(definition["treePath"], ["Customer Journeys"])
        self.assertEqual(
            definition["downloadableFiles"],
            [
                {
                    "fileName": JOURNEY_MANUAL_FILE_NAME,
                    "label": "Aargau CSV (60 synthetic monthly rows)",
                    "downloadUrl": f"/static/data/{JOURNEY_MANUAL_FILE_NAME}",
                    "targetPath": JOURNEY_MANUAL_PATH,
                    "storageFormat": "csv",
                    "storageFormatLabel": "Plain CSV (.csv)",
                    "storageFormatInstruction": (
                        "Im Ingestion Workbench «Plain CSV» auswählen. Nicht in Parquet "
                        "oder JSON umwandeln; Notebook-Zelle 1 liest exakt diese CSV-Datei."
                    ),
                    "replaceExisting": True,
                }
            ],
        )

        with CSV_PATH.open("r", encoding="utf-8", newline="") as source:
            csv_rows = list(csv.DictReader(source))
        self.assertEqual(len(csv_rows), 60)
        self.assertEqual(tuple(csv_rows[0]), JOURNEY_COLUMNS)
        self.assertEqual({row["canton_code"] for row in csv_rows}, {"AG"})
        self.assertEqual(len({row["record_id"] for row in csv_rows}), 60)
        self.assertEqual(
            {row["status"] for row in csv_rows},
            {"final", "month_to_date", "forecast"},
        )
        self.assertTrue(all(row["is_synthetic"] == "true" for row in csv_rows))
        self.assertEqual(
            [row["actual_receipt_chf"] for row in csv_rows[-4:]],
            ["0.00"] * 4,
        )

    def test_deterministic_monthly_data_is_complete_unique_and_plausible(self) -> None:
        electronic = journey_rows(include_aargau=False)
        aargau = journey_rows(include_aargau=True)
        self.assertEqual(electronic, journey_rows(include_aargau=False))
        self.assertEqual(len(electronic), JOURNEY_EXPECTED_ELECTRONIC_ROWS)
        self.assertEqual(len(aargau), 60)
        self.assertEqual(len(electronic + aargau), JOURNEY_EXPECTED_UNION_ROWS)

        electronic_codes = {row.canton_code for row in electronic}
        self.assertEqual(len(electronic_codes), 25)
        self.assertNotIn("AG", electronic_codes)
        self.assertEqual({row.canton_code for row in aargau}, {"AG"})
        self.assertEqual(
            len({(row.canton_code, row.report_month) for row in electronic + aargau}),
            JOURNEY_EXPECTED_UNION_ROWS,
        )
        self.assertTrue(all(row.planned_monthly_chf > 0 for row in electronic + aargau))
        self.assertTrue(
            all(
                row.actual_receipt_chf == 0
                for row in electronic + aargau
                if row.status == "forecast"
            )
        )
        august_rows = [
            row
            for row in electronic + aargau
            if row.report_month.isoformat() == "2026-08-01"
        ]
        self.assertEqual(len(august_rows), 26)
        self.assertTrue(
            all(0 < row.actual_receipt_chf < row.planned_monthly_chf for row in august_rows)
        )

        validate_journey_rows(
            electronic,
            expected_canton_codes=electronic_codes,
        )
        with self.assertRaisesRegex(ValueError, "row count mismatch"):
            validate_journey_rows(
                electronic[:-1],
                expected_canton_codes=electronic_codes,
            )
        with self.assertRaisesRegex(ValueError, "negative amount"):
            validate_journey_rows(
                [replace(electronic[0], actual_receipt_chf=Decimal("-1.00")), *electronic[1:]],
                expected_canton_codes=electronic_codes,
            )

    def test_sql_union_aggregation_recomputes_all_130_rows(self) -> None:
        electronic_rows = journey_rows(include_aargau=False)
        all_rows = [*electronic_rows, *journey_rows(include_aargau=True)]
        csv_relation = f"read_csv_auto('{CSV_PATH.as_posix()}')"
        result = aggregate_frame(rows_frame(electronic_rows), csv_relation)

        self.assertEqual(len(result), JOURNEY_EXPECTED_AGGREGATE_ROWS)
        self.assertEqual(result["canton_code"].nunique(), 26)
        self.assertEqual(result["tax_year"].nunique(), 5)
        self.assertEqual(
            len(result[["canton_code", "tax_year"]].drop_duplicates()),
            JOURNEY_EXPECTED_AGGREGATE_ROWS,
        )
        self.assertTrue(result["source_month_count"].eq(12).all())
        self.assertTrue(result["distinct_month_count"].eq(12).all())
        self.assertTrue(result["duplicate_month_count"].eq(0).all())
        self.assertTrue(result["negative_amount_count"].eq(0).all())
        self.assertTrue(result["is_synthetic"].all())

        cent = Decimal("0.01")
        for (canton_code, tax_year), group in pd.DataFrame(
            [dict(zip(JOURNEY_COLUMNS, row.values)) for row in all_rows]
        ).groupby(["canton_code", "tax_year"]):
            output = result[
                result["canton_code"].eq(canton_code)
                & result["tax_year"].eq(tax_year)
            ].iloc[0]
            plan = sum(group["planned_monthly_chf"], Decimal("0"))
            actual = sum(group["actual_receipt_chf"], Decimal("0"))
            expected = sum(
                (
                    row.planned_monthly_chf
                    if row.report_month < JOURNEY_AS_OF_DATE.replace(day=1)
                    else (
                        row.planned_monthly_chf
                        * Decimal(JOURNEY_AS_OF_DATE.day)
                        / Decimal("31")
                        if row.report_month == JOURNEY_AS_OF_DATE.replace(day=1)
                        else Decimal("0")
                    )
                )
                for row in all_rows
                if row.canton_code == canton_code and row.tax_year == tax_year
            ).quantize(cent, rounding=ROUND_HALF_UP)
            projection = (
                actual
                if tax_year < 2026
                else (plan * actual / expected).quantize(cent, rounding=ROUND_HALF_UP)
            )
            self.assertAlmostEqual(float(output["annual_plan_chf"]), float(plan), places=2)
            self.assertAlmostEqual(
                float(output["expected_receipts_to_date_chf"]),
                float(expected),
                places=2,
            )
            self.assertAlmostEqual(
                float(output["actual_receipts_to_date_chf"]),
                float(actual),
                places=2,
            )
            self.assertAlmostEqual(
                float(output["annual_projection_chf"]),
                float(projection),
                delta=0.02,
            )

    def test_sql_quality_edges_cover_zero_division_negative_and_missing_canton(self) -> None:
        electronic = rows_frame(journey_rows(include_aargau=False))
        csv_relation = f"read_csv_auto('{CSV_PATH.as_posix()}')"

        zeroed = electronic.copy()
        zero_mask = zeroed["canton_code"].eq("UR") & zeroed["tax_year"].eq(2026)
        zeroed.loc[zero_mask, "planned_monthly_chf"] = Decimal("0")
        zeroed.loc[zero_mask, "actual_receipt_chf"] = Decimal("0")
        zero_result = aggregate_frame(zeroed, csv_relation)
        zero_row = zero_result[
            zero_result["canton_code"].eq("UR") & zero_result["tax_year"].eq(2026)
        ].iloc[0]
        self.assertTrue(pd.isna(zero_row["annual_projection_chf"]))
        self.assertTrue(pd.isna(zero_row["variance_pct"]))

        negative = electronic.copy()
        negative_index = negative[
            negative["canton_code"].eq("BE") & negative["tax_year"].eq(2025)
        ].index[0]
        negative.loc[negative_index, "actual_receipt_chf"] = Decimal("-1")
        negative_result = aggregate_frame(negative, csv_relation)
        negative_row = negative_result[
            negative_result["canton_code"].eq("BE")
            & negative_result["tax_year"].eq(2025)
        ].iloc[0]
        self.assertEqual(int(negative_row["negative_amount_count"]), 1)

        missing = electronic[~electronic["canton_code"].eq("JU")].copy()
        missing_result = aggregate_frame(missing, csv_relation)
        self.assertEqual(len(missing_result), 25 * 5)
        self.assertNotIn("JU", set(missing_result["canton_code"]))


class DataAnalystsJourneyLoaderTests(unittest.TestCase):
    def test_loader_writes_25_sixty_row_parquets_then_loads_1500_postgres_rows(self) -> None:
        connection = RecordingJourneyConnection()
        uploads: list[tuple[str, int]] = []
        progress: list[dict[str, object]] = []

        def capture_upload(_client, *, local_path: Path, bucket: str, key: str) -> None:
            self.assertEqual(bucket, JOURNEY_BUCKET)
            with duckdb.connect(":memory:") as reader:
                row_count = int(
                    reader.execute(
                        "SELECT COUNT(*) FROM read_parquet(?)",
                        [local_path.as_posix()],
                    ).fetchone()[0]
                )
            uploads.append((key, row_count))

        context = DataGeneratorContext(
            settings=SimpleNamespace(pg_oltp_database="oltp", s3_bucket="configured"),
            job_id="journey-job-001",
            requested_size_gb=0.01,
            connection_factory=lambda: connection,
            progress_callback=lambda **changes: progress.append(changes),
            is_cancelled=lambda: False,
        )

        with (
            patch(
                "bit_data_workbench.data_generator.data_analysts_journey.s3_client",
                return_value=object(),
            ),
            patch(
                "bit_data_workbench.data_generator.data_analysts_journey.ensure_s3_bucket"
            ) as ensure_bucket,
            patch(
                "bit_data_workbench.data_generator.data_analysts_journey.delete_s3_prefix",
                return_value=0,
            ) as delete_prefix,
            patch(
                "bit_data_workbench.data_generator.data_analysts_journey.upload_s3_file",
                side_effect=capture_upload,
            ),
        ):
            result = GENERATOR.run(context)

        self.assertEqual(result.generated_rows, JOURNEY_EXPECTED_ELECTRONIC_ROWS)
        self.assertEqual(result.target_relation, JOURNEY_POSTGRES_RELATION)
        self.assertEqual(result.target_path, GENERATOR.electronic_path)
        self.assertEqual(len(uploads), 25)
        self.assertEqual({count for _key, count in uploads}, {60})
        self.assertEqual(len({key for key, _count in uploads}), 25)
        self.assertTrue(
            all(key.startswith(f"{JOURNEY_ELECTRONIC_PREFIX}/") for key, _count in uploads)
        )
        ensure_bucket.assert_called_once_with(context.settings, JOURNEY_BUCKET)
        delete_prefix.assert_called_once_with(
            context.settings,
            JOURNEY_BUCKET,
            f"{JOURNEY_ELECTRONIC_PREFIX}/",
        )
        executed_sql = "\n".join(sql for sql, _params in connection.executed)
        self.assertIn("CREATE TABLE", executed_sql)
        self.assertIn("read_parquet('s3://data-analysts-journey/electronic/cantons/*.parquet'", executed_sql)
        self.assertNotIn("manual/aargau", "\n".join(key for key, _count in uploads))
        self.assertTrue(progress)

    def test_cleanup_uses_fixed_owned_prefix_and_preserves_manual_and_products(self) -> None:
        connection = RecordingJourneyConnection()
        context = DataGeneratorContext(
            settings=SimpleNamespace(pg_oltp_database="oltp", s3_bucket="configured"),
            job_id="journey-cleanup-001",
            requested_size_gb=0.01,
            connection_factory=lambda: connection,
            progress_callback=lambda **_changes: None,
            is_cancelled=lambda: False,
        )
        hostile_snapshot = SimpleNamespace(
            target_relation="pg_oltp.public.someone_elses_table",
            target_path="s3://data-analysts-journey/manual/aargau/",
            target_name="someone-elses-data",
        )
        with patch(
            "bit_data_workbench.data_generator.data_analysts_journey.delete_s3_prefix",
            return_value=25,
        ) as delete_prefix:
            result = GENERATOR.cleanup(context, hostile_snapshot)

        delete_prefix.assert_called_once_with(
            context.settings,
            JOURNEY_BUCKET,
            f"{JOURNEY_ELECTRONIC_PREFIX}/",
        )
        executed_sql = "\n".join(sql for sql, _params in connection.executed)
        self.assertIn("kantonale_gewerbesteuer_electronic", executed_sql)
        self.assertNotIn("someone_elses", executed_sql)
        self.assertIn("manual and product paths were kept", result.message)

    def test_schema_readiness_rejects_legacy_or_partial_table_shape(self) -> None:
        connection = RecordingJourneyConnection(
            schema_columns=("canton_code", "category"),
        )
        with self.assertRaisesRegex(RuntimeError, "readiness failed"):
            assert_postgres_table_schema(
                connection,
                table_name="legacy_table",
            )
        connection.close()


class DataAnalystsJourneyNotebookTests(unittest.TestCase):
    def test_notebook_is_immutable_exactly_two_cells_and_publication_ready(self) -> None:
        notebook = build_data_analysts_journey_notebook()
        self.assertEqual(notebook.notebook_id, JOURNEY_NOTEBOOK_ID)
        self.assertEqual(
            notebook.title,
            "A Data Analyst's Journey – Kantonale Gewerbesteuer",
        )
        self.assertFalse(notebook.can_edit)
        self.assertFalse(notebook.can_delete)
        self.assertTrue(notebook.shared)
        self.assertEqual(len(notebook.cells), 2)
        self.assertEqual([cell.language for cell in notebook.cells], ["sql", "python"])
        self.assertIn("UNION ALL", notebook.cells[0].sql)
        self.assertIn(JOURNEY_POSTGRES_QUERY_REFERENCE, notebook.cells[0].sql)
        self.assertEqual(
            notebook.cells[0].query_options["duckdb"]["resultStorage"]["path"],
            JOURNEY_PRODUCT_PATH,
        )
        self.assertIn(f"read_parquet('{JOURNEY_PRODUCT_PATH}')", notebook.cells[1].sql)
        self.assertIn("ax_hist.hist(", notebook.cells[1].sql)
        self.assertIn("ax_hist.axvline(0", notebook.cells[1].sql)
        self.assertIn("Mrd. CHF", notebook.cells[1].sql)
        self.assertIn("Stichtag 12.08.2026", notebook.cells[1].sql)

    def test_chart_cell_emits_real_png_from_materialized_result(self) -> None:
        electronic = rows_frame(journey_rows(include_aargau=False))
        csv_relation = f"read_csv_auto('{CSV_PATH.as_posix()}')"
        aggregation_sql = build_data_analysts_journey_aggregation_sql(
            "electronic",
            csv_relation,
        )
        with TemporaryDirectory(prefix="bdw-journey-chart-") as temp_dir:
            parquet_path = Path(temp_dir) / "journey-result.parquet"
            connection = duckdb.connect(":memory:")
            try:
                connection.register("electronic", electronic)
                connection.execute(
                    f"COPY ({aggregation_sql.rstrip(';')}) TO ? (FORMAT PARQUET)",
                    [parquet_path.as_posix()],
                )
            finally:
                connection.close()

            sessions = KernelSessionManager()
            session = sessions.get_session(
                client_id="journey-chart-test",
                notebook_id=JOURNEY_NOTEBOOK_ID,
            )
            try:
                outputs = sessions.execute(
                    session,
                    code=build_data_analysts_journey_chart_python(parquet_path.as_posix()),
                    context={
                        "selectedSources": [],
                        "relations": [],
                        "localRelationMap": {},
                    },
                    is_cancelled=lambda: False,
                )
            finally:
                sessions.shutdown_all()

        errors = [output for output in outputs if output.output_type == "error"]
        self.assertEqual(errors, [])
        images = [
            output
            for output in outputs
            if output.output_type == "image" and output.mime_type == "image/png"
        ]
        self.assertEqual(len(images), 1)
        image_bytes = base64.b64decode(str(images[0].data))
        self.assertTrue(image_bytes.startswith(b"\x89PNG\r\n\x1a\n"))
        try:
            from PIL import Image
        except ImportError:  # pragma: no cover - matplotlib normally installs Pillow
            self.assertGreater(len(image_bytes), 20_000)
        else:
            with Image.open(io.BytesIO(image_bytes)) as image:
                self.assertGreaterEqual(image.width, 1200)
                self.assertGreaterEqual(image.height, 500)


if __name__ == "__main__":
    unittest.main()
