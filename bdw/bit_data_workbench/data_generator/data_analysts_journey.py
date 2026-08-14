from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from tempfile import TemporaryDirectory
from zoneinfo import ZoneInfo

from ..backend.s3_storage import (
    delete_s3_prefix,
    ensure_s3_bucket,
    s3_bucket_schema_name,
    s3_client,
    upload_s3_file,
)
from ..backend.source_references import pg_source_reference, s3_source_reference
from .base import (
    DataGenerationCancelled,
    DataGenerator,
    DataGeneratorContext,
    DataGeneratorResult,
    generation_target,
    update_generation_target_status,
)
from .helpers import approximate_size_gb, qualified_name, sql_literal


JOURNEY_GENERATOR_ID = "data_analysts_journey_loader"
JOURNEY_NOTEBOOK_ID = "data-analysts-journey-cantonal-business-tax"
JOURNEY_TREE_PATH = ("Customer Journeys",)
JOURNEY_BUCKET = "data-analysts-journey"
JOURNEY_ELECTRONIC_PREFIX = "electronic/cantons"
JOURNEY_MANUAL_PREFIX = "manual/aargau"
JOURNEY_MANUAL_FILE_NAME = "gewerbesteuer_aargau_2022_2026.csv"
JOURNEY_MANUAL_KEY = f"{JOURNEY_MANUAL_PREFIX}/{JOURNEY_MANUAL_FILE_NAME}"
JOURNEY_MANUAL_PATH = f"s3://{JOURNEY_BUCKET}/{JOURNEY_MANUAL_KEY}"
JOURNEY_MANUAL_DOWNLOAD_URL = f"/static/data/{JOURNEY_MANUAL_FILE_NAME}"
JOURNEY_PRODUCT_KEY = (
    "products/kantonale-gewerbesteuer-soll-ist-2022-2026.parquet"
)
JOURNEY_PRODUCT_PATH = f"s3://{JOURNEY_BUCKET}/{JOURNEY_PRODUCT_KEY}"
JOURNEY_PRODUCT_RELATION = (
    f"{s3_bucket_schema_name(JOURNEY_BUCKET)}."
    "kantonale_gewerbesteuer_soll_ist_2022_2026"
)
JOURNEY_PRODUCT_SOURCE_DESCRIPTOR: dict[str, str] = {
    "sourceKind": "relation",
    "sourceId": "s3",
    "relation": JOURNEY_PRODUCT_RELATION,
    "sourceDisplayName": "Kantonale Gewerbesteuer Soll/Ist 2022–2026",
    "sourcePlatform": "s3",
}
JOURNEY_POSTGRES_TABLE = "kantonale_gewerbesteuer_electronic"
JOURNEY_POSTGRES_RELATION = f"pg_oltp.public.{JOURNEY_POSTGRES_TABLE}"
JOURNEY_POSTGRES_QUERY_REFERENCE = pg_source_reference(
    source_id="pg_oltp",
    relation=JOURNEY_POSTGRES_RELATION,
)
JOURNEY_MANUAL_QUERY_REFERENCE = s3_source_reference(
    bucket=JOURNEY_BUCKET,
    key=JOURNEY_MANUAL_KEY,
)
JOURNEY_RESULT_QUERY_REFERENCE = s3_source_reference(
    bucket=JOURNEY_BUCKET,
    key=JOURNEY_PRODUCT_KEY,
)
JOURNEY_AS_OF_DATE = date(2026, 8, 12)
JOURNEY_TIME_ZONE = "Europe/Zurich"
JOURNEY_EXPECTED_ELECTRONIC_ROWS = 25 * 60
JOURNEY_EXPECTED_UNION_ROWS = 26 * 60
JOURNEY_EXPECTED_AGGREGATE_ROWS = 26 * 5


CANTONS: tuple[tuple[str, str, Decimal], ...] = (
    ("ZH", "Zürich", Decimal("260000000")),
    ("BE", "Bern", Decimal("100000000")),
    ("LU", "Luzern", Decimal("55000000")),
    ("UR", "Uri", Decimal("6000000")),
    ("SZ", "Schwyz", Decimal("42000000")),
    ("OW", "Obwalden", Decimal("9000000")),
    ("NW", "Nidwalden", Decimal("11000000")),
    ("GL", "Glarus", Decimal("9000000")),
    ("ZG", "Zug", Decimal("60000000")),
    ("FR", "Freiburg", Decimal("45000000")),
    ("SO", "Solothurn", Decimal("42000000")),
    ("BS", "Basel-Stadt", Decimal("95000000")),
    ("BL", "Basel-Landschaft", Decimal("70000000")),
    ("SH", "Schaffhausen", Decimal("18000000")),
    ("AR", "Appenzell Ausserrhoden", Decimal("9000000")),
    ("AI", "Appenzell Innerrhoden", Decimal("3500000")),
    ("SG", "St. Gallen", Decimal("80000000")),
    ("GR", "Graubünden", Decimal("45000000")),
    ("AG", "Aargau", Decimal("105000000")),
    ("TG", "Thurgau", Decimal("38000000")),
    ("TI", "Tessin", Decimal("75000000")),
    ("VD", "Waadt", Decimal("120000000")),
    ("VS", "Wallis", Decimal("45000000")),
    ("NE", "Neuenburg", Decimal("32000000")),
    ("GE", "Genf", Decimal("140000000")),
    ("JU", "Jura", Decimal("12000000")),
)

JOURNEY_COLUMNS: tuple[str, ...] = (
    "record_id",
    "canton_code",
    "canton_name",
    "report_month",
    "as_of_date",
    "tax_year",
    "planned_monthly_chf",
    "actual_receipt_chf",
    "status",
    "delivery_channel",
    "reported_at",
    "is_synthetic",
)

JOURNEY_POSTGRES_COLUMNS: tuple[str, ...] = (
    "record_id VARCHAR",
    "canton_code VARCHAR",
    "canton_name VARCHAR",
    "report_month DATE",
    "as_of_date DATE",
    "tax_year INTEGER",
    "planned_monthly_chf DECIMAL(18,2)",
    "actual_receipt_chf DECIMAL(18,2)",
    "status VARCHAR",
    "delivery_channel VARCHAR",
    "reported_at TIMESTAMPTZ",
    "is_synthetic BOOLEAN",
)

SEASONAL_FACTORS: tuple[Decimal, ...] = tuple(
    Decimal(value)
    for value in (
        "0.88",
        "0.91",
        "1.02",
        "0.95",
        "1.04",
        "1.10",
        "0.96",
        "1.00",
        "1.08",
        "1.12",
        "0.99",
        "0.95",
    )
)
YEAR_GROWTH: dict[int, Decimal] = {
    2022: Decimal("1.000"),
    2023: Decimal("1.035"),
    2024: Decimal("1.070"),
    2025: Decimal("1.110"),
    2026: Decimal("1.150"),
}
CENT = Decimal("0.01")


@dataclass(frozen=True, slots=True)
class JourneyRow:
    record_id: str
    canton_code: str
    canton_name: str
    report_month: date
    as_of_date: date
    tax_year: int
    planned_monthly_chf: Decimal
    actual_receipt_chf: Decimal
    status: str
    delivery_channel: str
    reported_at: str
    is_synthetic: bool = True

    @property
    def values(self) -> tuple[object, ...]:
        return tuple(getattr(self, column) for column in JOURNEY_COLUMNS)


def _money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _reported_at(report_month: date, status: str) -> str:
    zone = ZoneInfo(JOURNEY_TIME_ZONE)
    if status == "final":
        next_month_year = report_month.year + (1 if report_month.month == 12 else 0)
        next_month = 1 if report_month.month == 12 else report_month.month + 1
        timestamp = datetime(next_month_year, next_month, 5, 8, 30, tzinfo=zone)
    else:
        timestamp = datetime(2026, 8, 12, 9, 15, tzinfo=zone)
    return timestamp.isoformat()


def _status_for_month(report_month: date) -> str:
    current_month = JOURNEY_AS_OF_DATE.replace(day=1)
    if report_month < current_month:
        return "final"
    if report_month == current_month:
        return "month_to_date"
    return "forecast"


def journey_rows(*, include_aargau: bool) -> list[JourneyRow]:
    rows: list[JourneyRow] = []
    for canton_index, (canton_code, canton_name, base_monthly_chf) in enumerate(CANTONS):
        if (canton_code == "AG") != include_aargau:
            continue
        delivery_channel = "manual_csv" if canton_code == "AG" else "electronic_s3"
        canton_bias = Decimal(((canton_index * 7) % 15) - 7) / Decimal("100")
        for year in range(2022, 2027):
            for month in range(1, 13):
                report_month = date(year, month, 1)
                status = _status_for_month(report_month)
                plan = _money(
                    base_monthly_chf
                    * YEAR_GROWTH[year]
                    * SEASONAL_FACTORS[month - 1]
                )
                year_effect = Decimal(year - 2024) * Decimal("0.002")
                month_effect = Decimal(
                    ((canton_index * 5 + month * 3 + year) % 7) - 3
                ) * Decimal("0.003")
                actual_factor = Decimal("1") + canton_bias + year_effect + month_effect
                if status == "forecast":
                    actual = Decimal("0.00")
                elif status == "month_to_date":
                    actual = _money(
                        plan
                        * actual_factor
                        * Decimal(JOURNEY_AS_OF_DATE.day)
                        / Decimal("31")
                    )
                else:
                    actual = _money(plan * actual_factor)
                rows.append(
                    JourneyRow(
                        record_id=f"SYN-{canton_code}-{year}{month:02d}",
                        canton_code=canton_code,
                        canton_name=canton_name,
                        report_month=report_month,
                        as_of_date=JOURNEY_AS_OF_DATE,
                        tax_year=year,
                        planned_monthly_chf=plan,
                        actual_receipt_chf=actual,
                        status=status,
                        delivery_channel=delivery_channel,
                        reported_at=_reported_at(report_month, status),
                    )
                )
    return rows


def validate_journey_rows(
    rows: list[JourneyRow],
    *,
    expected_canton_codes: set[str],
) -> None:
    expected_statuses = {"final", "month_to_date", "forecast"}
    actual_canton_codes = {row.canton_code for row in rows}
    if actual_canton_codes != expected_canton_codes:
        raise ValueError(
            "Journey canton coverage mismatch: "
            f"expected {sorted(expected_canton_codes)!r}, got {sorted(actual_canton_codes)!r}."
        )
    if len(rows) != len(expected_canton_codes) * 60:
        raise ValueError(
            "Journey row count mismatch: expected "
            f"{len(expected_canton_codes) * 60:,}, got {len(rows):,}."
        )

    record_ids = [row.record_id for row in rows]
    month_keys = [(row.canton_code, row.report_month) for row in rows]
    if len(record_ids) != len(set(record_ids)):
        raise ValueError("Journey record_id values must be unique.")
    if len(month_keys) != len(set(month_keys)):
        raise ValueError("Journey canton/report_month keys must be unique.")

    for canton_code in expected_canton_codes:
        canton_rows = [row for row in rows if row.canton_code == canton_code]
        if len(canton_rows) != 60:
            raise ValueError(f"Journey canton {canton_code} must contain exactly 60 months.")
        if canton_rows[0].report_month != date(2022, 1, 1):
            raise ValueError(f"Journey canton {canton_code} must start in January 2022.")
        if canton_rows[-1].report_month != date(2026, 12, 1):
            raise ValueError(f"Journey canton {canton_code} must end in December 2026.")

    for row in rows:
        if row.status not in expected_statuses:
            raise ValueError(f"Unsupported journey status: {row.status}.")
        if row.tax_year != row.report_month.year:
            raise ValueError(f"Tax year does not match report month for {row.record_id}.")
        if row.as_of_date != JOURNEY_AS_OF_DATE:
            raise ValueError(f"Unexpected as-of date for {row.record_id}.")
        if not row.is_synthetic:
            raise ValueError(f"Journey row {row.record_id} must be marked synthetic.")
        if row.planned_monthly_chf < 0 or row.actual_receipt_chf < 0:
            raise ValueError(f"Journey row {row.record_id} contains a negative amount.")
        if row.status == "forecast" and row.actual_receipt_chf != 0:
            raise ValueError(f"Forecast row {row.record_id} must have zero actual receipts.")


def _sql_string(value: object) -> str:
    return sql_literal(str(value))


def journey_values_select(rows: list[JourneyRow]) -> str:
    if not rows:
        raise ValueError("At least one journey row is required.")
    value_rows: list[str] = []
    for row in rows:
        value_rows.append(
            "(" + ", ".join(
                (
                    _sql_string(row.record_id),
                    _sql_string(row.canton_code),
                    _sql_string(row.canton_name),
                    f"DATE {_sql_string(row.report_month.isoformat())}",
                    f"DATE {_sql_string(row.as_of_date.isoformat())}",
                    str(row.tax_year),
                    str(row.planned_monthly_chf),
                    str(row.actual_receipt_chf),
                    _sql_string(row.status),
                    _sql_string(row.delivery_channel),
                    f"TIMESTAMPTZ {_sql_string(row.reported_at)}",
                    "true" if row.is_synthetic else "false",
                )
            ) + ")"
        )
    aliases = ", ".join(JOURNEY_COLUMNS)
    return (
        "SELECT * FROM (VALUES\n  "
        + ",\n  ".join(value_rows)
        + f"\n) AS journey_data({aliases})"
    )


def expected_postgres_column_names() -> tuple[str, ...]:
    return tuple(column.split(" ", 1)[0] for column in JOURNEY_POSTGRES_COLUMNS)


def assert_postgres_table_schema(connection, *, table_name: str) -> None:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = 'pg_oltp'
          AND table_schema = 'public'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    actual_columns = tuple(str(row[0]) for row in rows)
    expected_columns = expected_postgres_column_names()
    if actual_columns != expected_columns:
        raise RuntimeError(
            f"PostgreSQL readiness failed for public.{table_name}: "
            f"expected columns {expected_columns!r}, got {actual_columns!r}."
        )


class DataAnalystsJourneyGenerator(DataGenerator):
    generator_id = JOURNEY_GENERATOR_ID
    title = "a Data Analyst's Journey"
    description = (
        "Creates a fully synthetic 2022–2026 cantonal business-tax journey: "
        "25 electronic canton Parquet files in S3 and a curated PostgreSQL table. "
        "Upload the supplied Aargau CSV manually to complete the cross-source UNION."
    )
    target_kind = "journey"
    tree_path = JOURNEY_TREE_PATH
    default_size_gb = 0.01
    min_size_gb = 0.01
    max_size_gb = 0.01
    approximate_row_bytes = 240
    default_target_name = JOURNEY_POSTGRES_TABLE
    tags = (
        "customer-journey",
        "postgres",
        "s3",
        "csv",
        "union",
        "synthetic",
        "gewerbesteuer",
    )
    downloadable_files = (
        {
            "fileName": JOURNEY_MANUAL_FILE_NAME,
            "label": "Aargau CSV (60 synthetic monthly rows)",
            "downloadUrl": JOURNEY_MANUAL_DOWNLOAD_URL,
            "targetPath": JOURNEY_MANUAL_PATH,
            "replaceExisting": True,
        },
    )

    @property
    def electronic_path(self) -> str:
        return f"s3://{JOURNEY_BUCKET}/{JOURNEY_ELECTRONIC_PREFIX}"

    @property
    def postgres_relation(self) -> str:
        return qualified_name("pg_oltp", "public", JOURNEY_POSTGRES_TABLE)

    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        if not context.settings.pg_oltp_database:
            raise ValueError(
                "PG_OLTP_DATABASE must be configured before running a Data Analyst's Journey."
            )
        if not context.settings.s3_bucket:
            raise ValueError(
                "S3 configuration must be available before running a Data Analyst's Journey."
            )

        connection = context.connect()
        upload_client = s3_client(context.settings)
        written_targets = [
            generation_target(
                target_kind="s3_prefix",
                label="25 electronic canton Parquet files",
                location=self.electronic_path,
            ),
            generation_target(
                target_kind="postgres_table",
                label="Curated PostgreSQL monthly table",
                location=JOURNEY_POSTGRES_RELATION,
            ),
        ]

        try:
            context.report(
                progress=0.0,
                progress_label="Preparing synthetic journey...",
                message=(
                    "Resetting loader-owned electronic artefacts; manual Aargau and "
                    "published products remain untouched."
                ),
                target_name=JOURNEY_POSTGRES_TABLE,
                target_relation=JOURNEY_POSTGRES_RELATION,
                target_path=self.electronic_path,
                written_targets=written_targets,
            )
            ensure_s3_bucket(context.settings, JOURNEY_BUCKET)
            delete_s3_prefix(
                context.settings,
                JOURNEY_BUCKET,
                f"{JOURNEY_ELECTRONIC_PREFIX}/",
            )
            connection.execute(f"DROP TABLE IF EXISTS {self.postgres_relation}")

            electronic_rows = journey_rows(include_aargau=False)
            validate_journey_rows(
                electronic_rows,
                expected_canton_codes={code for code, _name, _base in CANTONS if code != "AG"},
            )
            rows_by_canton = {
                code: [row for row in electronic_rows if row.canton_code == code]
                for code, _name, _base in CANTONS
                if code != "AG"
            }

            with TemporaryDirectory(
                prefix=f"bdw-{self.generator_id}-{context.job_id[:8]}-"
            ) as temp_dir:
                temp_path = Path(temp_dir)
                for canton_number, (canton_code, canton_rows) in enumerate(
                    rows_by_canton.items(),
                    start=1,
                ):
                    context.raise_if_cancelled()
                    object_key = (
                        f"{JOURNEY_ELECTRONIC_PREFIX}/"
                        f"gewerbesteuer_{canton_code.lower()}_2022_2026.parquet"
                    )
                    local_path = temp_path / Path(object_key).name
                    connection.execute(
                        "COPY (\n"
                        f"{journey_values_select(canton_rows)}\n"
                        f") TO {sql_literal(local_path.as_posix())} "
                        "(FORMAT PARQUET, COMPRESSION ZSTD)"
                    )
                    upload_s3_file(
                        upload_client,
                        local_path=local_path,
                        bucket=JOURNEY_BUCKET,
                        key=object_key,
                    )
                    local_path.unlink(missing_ok=True)
                    context.report(
                        progress=(canton_number / 27),
                        progress_label=f"Writing canton {canton_number} / 25",
                        message=(
                            f"Wrote {canton_number * 60:,} synthetic electronic rows "
                            f"to {self.electronic_path}."
                        ),
                        generated_rows=canton_number * 60,
                    )

            written_targets = update_generation_target_status(
                written_targets,
                self.electronic_path,
                status="written",
            )
            connection.execute(
                f"CREATE TABLE {self.postgres_relation} "
                f"({', '.join(JOURNEY_POSTGRES_COLUMNS)})"
            )
            assert_postgres_table_schema(
                connection,
                table_name=JOURNEY_POSTGRES_TABLE,
            )
            source_glob = (
                f"s3://{JOURNEY_BUCKET}/{JOURNEY_ELECTRONIC_PREFIX}/*.parquet"
            )
            connection.execute(
                f"INSERT INTO {self.postgres_relation} "
                f"SELECT {', '.join(JOURNEY_COLUMNS)} "
                f"FROM read_parquet({sql_literal(source_glob)}, union_by_name = true)"
            )
            row_count = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM {self.postgres_relation}"
                ).fetchone()[0]
            )
            if row_count != JOURNEY_EXPECTED_ELECTRONIC_ROWS:
                raise RuntimeError(
                    "PostgreSQL readiness failed: expected "
                    f"{JOURNEY_EXPECTED_ELECTRONIC_ROWS:,} rows, found {row_count:,}."
                )
            written_targets = update_generation_target_status(
                written_targets,
                JOURNEY_POSTGRES_RELATION,
                status="written",
            )

            return DataGeneratorResult(
                target_name=JOURNEY_POSTGRES_TABLE,
                target_relation=JOURNEY_POSTGRES_RELATION,
                target_path=self.electronic_path,
                written_targets=written_targets,
                generated_rows=row_count,
                generated_size_gb=approximate_size_gb(
                    row_count,
                    self.approximate_row_bytes,
                ),
                message=(
                    "Created 25 canton Parquet files and loaded 1,500 synthetic "
                    "monthly rows into PostgreSQL. Upload the supplied Aargau CSV "
                    f"to {JOURNEY_MANUAL_PATH}."
                ),
            )
        except DataGenerationCancelled:
            self._cleanup_owned_artifacts(context=context, connection=connection)
            raise
        except Exception:
            self._cleanup_owned_artifacts(context=context, connection=connection)
            raise
        finally:
            connection.close()

    def cleanup(self, context: DataGeneratorContext, job) -> DataGeneratorResult:
        connection = context.connect()
        try:
            context.report(
                message=(
                    "Removing only loader-owned electronic Parquet files and the "
                    "curated PostgreSQL table."
                )
            )
            deleted_objects = self._cleanup_owned_artifacts(
                context=context,
                connection=connection,
            )
            return DataGeneratorResult(
                target_name=JOURNEY_POSTGRES_TABLE,
                generated_rows=0,
                generated_size_gb=0.0,
                message=(
                    f"Removed {deleted_objects:,} electronic loader object(s) and "
                    f"dropped {JOURNEY_POSTGRES_RELATION}; manual and product paths were kept."
                ),
            )
        finally:
            connection.close()

    def _cleanup_owned_artifacts(self, *, context: DataGeneratorContext, connection) -> int:
        try:
            connection.execute(f"DROP TABLE IF EXISTS {self.postgres_relation}")
        except Exception:
            pass
        try:
            return delete_s3_prefix(
                context.settings,
                JOURNEY_BUCKET,
                f"{JOURNEY_ELECTRONIC_PREFIX}/",
            )
        except Exception:
            return 0


GENERATOR = DataAnalystsJourneyGenerator()
