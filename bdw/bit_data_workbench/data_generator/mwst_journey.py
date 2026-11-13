"""Deterministic MWST fixtures. No real companies, tax rates, or official revenue data."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from ..backend.s3_storage import ensure_s3_bucket, s3_client, upload_s3_file
from ..backend.sql_utils import sql_literal
from .base import DataGenerator, DataGeneratorContext, DataGeneratorResult, generation_target

AS_OF = date(2026, 11, 12)
BUCKET = "mwst-journey"
GENERATOR_ID = "mwst_journey_loader"
NOTEBOOK_ID = "mwst-ertraege-st-gallen-2026"
JOURNEY_ID = NOTEBOOK_ID
COMPANY_KEY = "sources/mwst_firmen_2016_2026.parquet"
NATIONAL_KEY = "sources/mwst_ch_2016_2026.parquet"
EXAMPLE_KEY = "examples/mwst_firmen_sg_2026.parquet"
RESULT_KEY = "products/mwst_firmen_sg_2026.parquet"
COMPANY_SLUG = "mwst-ertraege-firmen-2016-2026"
NATIONAL_SLUG = "mwst-ertraege-schweiz-2016-2026"
RESULT_SLUG = "mwst-ertraege-firmen-sg-2026"
EXAMPLE_SLUG = RESULT_SLUG + "-beispiel"
CANTONS = (
    "ZH",
    "BE",
    "LU",
    "UR",
    "SZ",
    "OW",
    "NW",
    "GL",
    "ZG",
    "FR",
    "SO",
    "BS",
    "BL",
    "SH",
    "AR",
    "AI",
    "SG",
    "GR",
    "AG",
    "TG",
    "TI",
    "VD",
    "VS",
    "NE",
    "GE",
    "JU",
)
COMPANY_SCHEMA = (
    "company_id VARCHAR, company_name VARCHAR, canton_code VARCHAR, tax_year INTEGER, "
    "period_start DATE, period_end DATE, vat_revenue_chf DECIMAL(18,2), "
    "currency VARCHAR, provisional BOOLEAN, synthetic BOOLEAN"
)
FIELD_DESCRIPTIONS = {
    "company_id": "Stabile synthetische Firmenkennung; keine reale UID.",
    "company_name": "Fiktiver Firmenname für die Demonstration.",
    "canton_code": "Zweistelliger Code des Sitzkantons der synthetischen Firma.",
    "tax_year": "Berichtsjahr; 2016–2025 vollständig, 2026 vorläufig.",
    "period_start": "Beginn des dargestellten Berichtszeitraums.",
    "period_end": "Ende des Zeitraums; für 2026 der feste Stichtag 12.11.2026.",
    "vat_revenue_chf": "Synthetischer MWST-Ertrag in CHF, auf Rappen genau.",
    "currency": "Währung der Beträge: CHF.",
    "provisional": "True kennzeichnet den noch nicht vollständigen Jahreswert 2026.",
    "synthetic": "Immer true: keine amtlichen Zahlen oder realen Firmendaten.",
    "company_count": "Anzahl der synthetischen Firmen im Jahreswert.",
}
FIELD_TERMS = {
    "company_id": "CompanyId",
    "company_name": "CompanyName",
    "canton_code": "CantonCode",
    "tax_year": "TaxYear",
    "period_start": "ReportingPeriodStart",
    "period_end": "ReportingPeriodEnd",
    "vat_revenue_chf": "VatRevenueAmount",
    "currency": "CurrencyCode",
    "provisional": "IsProvisional",
    "synthetic": "IsSynthetic",
    "company_count": "LegalEntityCount",
}


def company_rows() -> list[tuple]:
    rows = []
    for canton_index, canton in enumerate(CANTONS):
        for company in range(1, (60 if canton == "SG" else 8) + 1):
            for year in range(2016, 2027):
                # Integer arithmetic followed by Decimal keeps repeat runs cent-identical.
                cents = (135_000_17 + canton_index * 871_13 + company * 31_337) * (
                    100 + (year - 2016) * 3
                )
                amount = Decimal(cents) / Decimal(10_000)
                if year == 2026:
                    amount *= Decimal(AS_OF.timetuple().tm_yday) / Decimal(365)
                rows.append(
                    (
                        f"DEMO-{canton}-{company:03d}",
                        f"Demo {canton} Unternehmen {company:03d}",
                        canton,
                        year,
                        date(year, 1, 1),
                        AS_OF if year == 2026 else date(year, 12, 31),
                        amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                        "CHF",
                        year == 2026,
                        True,
                    )
                )
    return rows


def write_datasets(directory: Path) -> dict[str, Path]:
    """Write all fixture files without touching the running workbench database."""
    files = {key: directory / Path(key).name for key in (COMPANY_KEY, NATIONAL_KEY, EXAMPLE_KEY)}
    with duckdb.connect(":memory:") as connection:
        connection.execute(f"CREATE TABLE companies ({COMPANY_SCHEMA})")
        connection.executemany(
            "INSERT INTO companies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", company_rows()
        )
        queries = {
            COMPANY_KEY: "SELECT * FROM companies ORDER BY tax_year, company_id",
            NATIONAL_KEY: (
                "SELECT tax_year, period_start, period_end, SUM(vat_revenue_chf)::DECIMAL(18,2) AS vat_revenue_chf, "
                "currency, provisional, synthetic, COUNT(*)::INTEGER AS company_count FROM companies "
                "GROUP BY ALL ORDER BY tax_year"
            ),
            EXAMPLE_KEY: "SELECT * FROM companies WHERE tax_year = 2026 AND canton_code = 'SG' ORDER BY company_id",
        }
        for key, query in queries.items():
            connection.execute(
                f"COPY ({query}) TO {sql_literal(files[key].as_posix())} (FORMAT PARQUET, COMPRESSION ZSTD)"
            )
    return files


class MwstJourneyGenerator(DataGenerator):
    generator_id = GENERATOR_ID
    title = "MWST-Journey – Jan Waser und Kassandra Valdata"
    description = (
        "Erzeugt 2.860 synthetische Firmen-Jahreswerte 2016–2026, elf Schweizer Jahressummen "
        "und einen getrennten fertigen SG-Beispielstand. 2026 ist vorläufig per 12.11.2026. "
        "Kassandras eigenes SG-Produkt wird erst durch ihre Filterabfrage erstellt."
    )
    target_kind = "journey"
    tree_path = ("Customer Journeys",)
    default_size_gb = min_size_gb = max_size_gb = 0.01
    default_target_name = BUCKET
    supports_cleanup = False
    tags = ("mwst", "synthetic", "s3", "parquet", "st-gallen", "customer-journey")

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        ensure_s3_bucket(context.settings, BUCKET)
        client = s3_client(context.settings)
        targets = []
        with TemporaryDirectory(prefix="bdw-mwst-seed-") as temporary:
            for index, (key, local_path) in enumerate(write_datasets(Path(temporary)).items()):
                context.raise_if_cancelled()
                upload_s3_file(client, bucket=BUCKET, key=key, local_path=local_path)
                targets.append(
                    generation_target(
                        target_kind="s3_object",
                        label=key,
                        location=f"s3://{BUCKET}/{key}",
                        status="completed",
                    )
                )
                context.report(
                    progress=(index + 1) / 3,
                    progress_label="MWST-Beispieldaten bereitstellen",
                    written_targets=targets,
                )
        return DataGeneratorResult(
            target_name=BUCKET,
            target_path=f"s3://{BUCKET}/sources",
            written_targets=targets,
            generated_rows=2931,
            message="MWST-Ausgangsdaten und getrennter SG-Beispielstand sind bereit. Eigene SG-Produkte bleiben unberührt.",
        )

    def cleanup(self, context, job):
        raise ValueError(
            "Die MWST-Ausgangsdaten können von publizierten Produkten referenziert sein und werden nicht automatisch gelöscht."
        )


GENERATOR = MwstJourneyGenerator()
