from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path
import shutil
import sys
from types import SimpleNamespace

import duckdb
from openpyxl import load_workbook
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bdw"))
from bit_data_workbench.data_generator.mwst_journey import (
    CANTONS,
    COMPANY_KEY,
    EXAMPLE_KEY,
    NATIONAL_KEY,
    company_rows,
    write_datasets,
)
from bit_data_workbench.backend.data_products.downloads import prepare_download
from bit_data_workbench.backend.data_products.tabular import (
    product_scope,
    select_sql,
    validate_scope,
)
from bit_data_workbench.backend.mwst_notebook import build_mwst_journey_notebook


@pytest.fixture
def dataset(tmp_path):
    files = write_datasets(tmp_path)
    database = tmp_path / "products.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.execute(
            "CREATE TABLE companies AS SELECT * FROM read_parquet(?)", [str(files[COMPANY_KEY])]
        )
    return files, lambda: duckdb.connect(str(database))


def product(*, scoped=True):
    return SimpleNamespace(
        slug="mwst-ertraege-firmen-sg-2026" if scoped else "mwst-ertraege-firmen-2016-2026",
        source=SimpleNamespace(
            source_kind="relation", bucket="", key="", relation="main.companies"
        ),
        custom_properties={"cantonCode": "SG", "taxYear": "2026"} if scoped else {},
    )


def test_rolling_scope_is_preserved_without_inventing_fixed_dates_or_mwst_filters():
    selected = product(scoped=False)
    selected.custom_properties = {"basisWindowType":"rolling", "rollingWindowSize":"12", "rollingWindowUnit":"months"}
    assert product_scope(selected) == {"basisWindowType":"rolling", "rollingWindowSize":12, "rollingWindowUnit":"months"}
    assert "WHERE" not in select_sql(selected)
    for invalid in ({"basisFrom":"2026-01-01"}, {"cantonCode":"SG","taxYear":"2026"},
                    {"rollingWindowSize":"0"}, {"rollingWindowSize":"1.5"}, {"rollingWindowUnit":"quarters"},
                    {"basisWindowType":"fixed"}):
        selected.custom_properties = {"basisWindowType":"rolling", "rollingWindowSize":"12", "rollingWindowUnit":"months", **invalid}
        with pytest.raises(ValueError):
            product_scope(selected)


def test_mwst_rows_are_deterministic_complete_and_cent_exact(dataset):
    files, factory = dataset
    rows = company_rows()
    assert rows == company_rows()
    assert len(rows) == 2860
    assert {row[2] for row in rows} == set(CANTONS)
    assert {row[3] for row in rows} == set(range(2016, 2027))
    assert len({row[0] for row in rows}) == 260
    assert all(isinstance(row[6], Decimal) and row[6].as_tuple().exponent == -2 for row in rows)
    with factory() as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM companies WHERE canton_code='SG' AND tax_year=2026"
            ).fetchone()[0]
            == 60
        )
        assert (
            connection.execute(
                "SELECT count(*) FROM companies WHERE tax_year=2026 AND (NOT provisional OR period_end <> DATE '2026-11-12')"
            ).fetchone()[0]
            == 0
        )
        national = connection.execute(
            "SELECT tax_year, vat_revenue_chf FROM read_parquet(?) ORDER BY tax_year",
            [str(files[NATIONAL_KEY])],
        ).fetchall()
        assert len(national) == 11
        assert (
            national
            == connection.execute(
                "SELECT tax_year, sum(vat_revenue_chf) FROM companies GROUP BY tax_year ORDER BY tax_year"
            ).fetchall()
        )
        assert (
            connection.execute(
                "SELECT count(*) FROM read_parquet(?)", [str(files[EXAMPLE_KEY])]
            ).fetchone()[0]
            == 60
        )


@pytest.mark.parametrize("export_format", ["csv", "xlsx", "parquet"])
@pytest.mark.parametrize("scoped, expected", [(True, 60), (False, 2860)])
def test_all_downloads_use_full_typed_product_and_same_amounts(
    dataset, export_format, scoped, expected
):
    _, factory = dataset
    selected = product(scoped=scoped)
    with factory() as connection:
        expected_rows = connection.execute(select_sql(selected)).fetchall()
    artifact = prepare_download(selected, export_format, factory)
    try:
        assert artifact.filename.endswith("." + export_format)
        if export_format == "csv":
            with artifact.local_path.open(encoding="utf-8-sig", newline="") as stream:
                rows = list(csv.DictReader(stream))
            assert sum(Decimal(row["vat_revenue_chf"]) for row in rows) == sum(
                row[6] for row in expected_rows
            )
        elif export_format == "xlsx":
            workbook = load_workbook(artifact.local_path, read_only=True, data_only=True)
            values = list(workbook.active.values)
            rows = [dict(zip(values[0], row)) for row in values[1:]]
            workbook.close()
            assert all(isinstance(row["vat_revenue_chf"], (float, int)) for row in rows)
            assert sum(
                Decimal(str(row["vat_revenue_chf"])).quantize(Decimal("0.01")) for row in rows
            ) == sum(row[6] for row in expected_rows)
        else:
            with duckdb.connect(":memory:") as connection:
                cursor = connection.execute(
                    "SELECT * FROM read_parquet(?)", [str(artifact.local_path)]
                )
                assert str(cursor.description[6][1]) == "DECIMAL(18,2)"
                columns = [column[0] for column in cursor.description]
                rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            assert sum(row["vat_revenue_chf"] for row in rows) == sum(
                row[6] for row in expected_rows
            )
        assert len(rows) == expected
        if scoped:
            assert {row["canton_code"] for row in rows} == {"SG"}
            assert {int(row["tax_year"]) for row in rows} == {2026}
    finally:
        shutil.rmtree(artifact.cleanup_dir)


def test_scope_rejects_unfiltered_source_before_publication(dataset):
    _, factory = dataset
    with factory() as connection:
        with pytest.raises(ValueError, match="ausschliesslich"):
            validate_scope(connection, product())
        connection.execute("DELETE FROM companies WHERE canton_code <> 'SG' OR tax_year <> 2026")
        validate_scope(connection, product())


def test_notebook_is_editable_filter_not_aggregate():
    notebook = build_mwst_journey_notebook()
    assert notebook.can_edit
    assert "canton_code = 'SG'" in notebook.sql and "tax_year = 2026" in notebook.sql
    assert "SUM(" not in notebook.sql
    assert "/products/" in notebook.cells[0].query_options["duckdb"]["resultStorage"]["path"]


def test_general_period_is_publication_evidence_without_a_tax_column_filter(dataset):
    _, factory = dataset
    selected = product(scoped=False)
    selected.custom_properties = {
        "creationRequestId": "11111111-1111-4111-8111-111111111111",
        "basisFrom": "2016-01-01",
        "basisUntil": "2026-12-31",
    }
    assert product_scope(selected) == selected.custom_properties
    assert "WHERE" not in select_sql(selected)
    with factory() as connection:
        validate_scope(connection, selected)
        assert len(connection.execute(select_sql(selected)).fetchall()) == 2860
    selected.custom_properties["basisUntil"] = "2015-01-01"
    with pytest.raises(ValueError, match="end date"):
        product_scope(selected)
