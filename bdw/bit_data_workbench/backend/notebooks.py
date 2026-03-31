from __future__ import annotations

from typing import Iterable

from ..models import NotebookCellDefinition, NotebookDefinition, NotebookFolder, SourceCatalog


def _find_relation(
    catalogs: Iterable[SourceCatalog],
    *,
    catalog_name: str,
    schema_name: str | None = None,
) -> str | None:
    for catalog in catalogs:
        if catalog.name != catalog_name:
            continue
        for schema in catalog.schemas:
            if schema_name is not None and schema.name != schema_name:
                continue
            if schema.objects:
                return schema.objects[0].relation
    return None


def _find_relation_by_object_name(
    catalogs: Iterable[SourceCatalog],
    *,
    catalog_name: str,
    schema_name: str | None = None,
    object_names: Iterable[str],
) -> str | None:
    preferred = {name.lower(): name for name in object_names}
    for catalog in catalogs:
        if catalog.name != catalog_name:
            continue
        for schema in catalog.schemas:
            if schema_name is not None and schema.name != schema_name:
                continue
            for source_object in schema.objects:
                if source_object.name.lower() in preferred:
                    return source_object.relation
    return None


def _strip_catalog_prefix(relation: str | None, catalog_name: str) -> str | None:
    normalized_relation = (relation or "").strip()
    prefix = f"{catalog_name}."
    if not normalized_relation:
        return None
    if normalized_relation.startswith(prefix):
        return normalized_relation[len(prefix) :]
    return normalized_relation


def build_notebooks(catalogs: list[SourceCatalog]) -> list[NotebookDefinition]:
    preferred_s3_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=("vat_smoke",),
    )
    preferred_postgres_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=("vat_smoke_test_reference",),
    )
    contest_postgres_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=("tax_assessment_pg_vs_s3",),
    )
    contest_s3_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=("tax_assessment_pg_vs_s3",),
    )
    contest_postgres_native_relation = _strip_catalog_prefix(contest_postgres_relation, "pg_oltp")

    s3_sql = (
        "SELECT\n"
        "  filing_id,\n"
        "  company_uid,\n"
        "  canton_code,\n"
        "  tax_period_end,\n"
        "  declared_turnover_chf,\n"
        "  net_vat_due_chf,\n"
        "  refund_claim_chf,\n"
        "  filing_status\n"
        f"FROM {preferred_s3_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        "  AND (net_vat_due_chf > 20000 OR refund_claim_chf > 5000)\n"
        "ORDER BY tax_period_end DESC, net_vat_due_chf DESC, refund_claim_chf DESC\n"
        "LIMIT 100;"
        if preferred_s3_relation
        else "SELECT 'Run the S3 VAT Smoke Loader from the Ingestion Workbench first.' AS status;"
    )

    postgres_sql = (
        "SELECT\n"
        "  filing_id,\n"
        "  company_uid,\n"
        "  canton_code,\n"
        "  tax_period_end,\n"
        "  output_vat_chf,\n"
        "  input_vat_chf,\n"
        "  net_vat_due_chf,\n"
        "  filing_status,\n"
        "  audit_flag\n"
        f"FROM {preferred_postgres_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        "  AND (net_vat_due_chf > 15000 OR audit_flag = true)\n"
        "ORDER BY tax_period_end DESC, net_vat_due_chf DESC, filing_id DESC\n"
        "LIMIT 100;"
        if preferred_postgres_relation
        else "SELECT 'Run the PostgreSQL OLTP VAT Smoke Loader from the Ingestion Workbench first.' AS status;"
    )
    performance_sql_template = (
        "WITH scoped_assessments AS (\n"
        "  SELECT\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    assessment_status,\n"
        "    payment_status,\n"
        "    CAST(date_trunc('quarter', tax_period_end) AS DATE) AS tax_quarter_start,\n"
        "    assessed_tax_chf,\n"
        "    collected_tax_chf,\n"
        "    open_balance_chf,\n"
        "    taxable_base_chf,\n"
        "    declared_deduction_chf,\n"
        "    audit_risk_score,\n"
        "    CASE\n"
        "      WHEN payment_status IN ('overdue', 'enforcement') THEN 1\n"
        "      ELSE 0\n"
        "    END AS enforcement_risk_flag\n"
        "  FROM {relation}\n"
        "  WHERE tax_period_end >= DATE '2024-01-01'\n"
        "    AND tax_type IN ('VAT', 'COMPANY_TAX', 'ALCOHOL_TAX', 'INCOME_TAX')\n"
        "    AND assessment_status IN ('under_review', 'assessed', 'appealed', 'enforced')\n"
        "),\n"
        "quarterly_pressure AS (\n"
        "  SELECT\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    tax_quarter_start,\n"
        "    COUNT(*) AS assessment_count,\n"
        "    CAST(ROUND(SUM(assessed_tax_chf), 2) AS DECIMAL(18,2)) AS assessed_tax_total_chf,\n"
        "    CAST(ROUND(SUM(collected_tax_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(ROUND(SUM(open_balance_chf), 2) AS DECIMAL(18,2)) AS open_balance_total_chf,\n"
        "    CAST(CAST(AVG(taxable_base_chf - declared_deduction_chf) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_net_tax_base_chf,\n"
        "    CAST(CAST(AVG(audit_risk_score) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_audit_risk_score,\n"
        "    SUM(enforcement_risk_flag) AS enforcement_risk_count\n"
        "  FROM scoped_assessments\n"
        "  GROUP BY canton_code, tax_type, industry_sector, tax_quarter_start\n"
        ")\n"
        "SELECT\n"
        "  canton_code,\n"
        "  tax_type,\n"
        "  industry_sector,\n"
        "  tax_quarter_start,\n"
        "  assessment_count,\n"
        "  assessed_tax_total_chf,\n"
        "  collected_tax_total_chf,\n"
        "  open_balance_total_chf,\n"
        "  avg_net_tax_base_chf,\n"
        "  avg_audit_risk_score,\n"
        "  enforcement_risk_count\n"
        "FROM quarterly_pressure\n"
        "WHERE assessed_tax_total_chf >= 750000\n"
        "ORDER BY open_balance_total_chf DESC, avg_audit_risk_score DESC, assessment_count DESC\n"
        "LIMIT 30;"
    )
    contest_postgres_sql = (
        performance_sql_template.format(relation=contest_postgres_relation)
        if contest_postgres_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Ingestion Workbench first.' AS status;"
    )
    contest_s3_sql = (
        performance_sql_template.format(relation=contest_s3_relation)
        if contest_s3_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Ingestion Workbench first.' AS status;"
    )
    contest_postgres_native_sql = (
        performance_sql_template.format(relation=contest_postgres_native_relation)
        if contest_postgres_native_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Ingestion Workbench first.' AS status;"
    )
    oltp_write_test_table = "public.notebook_oltp_write_test"
    oltp_write_test_setup_sql = (
        "-- Reset the OLTP write-test table so the notebook can be rerun safely.\n"
        f"DROP TABLE IF EXISTS {oltp_write_test_table};\n"
        f"CREATE TABLE {oltp_write_test_table} (\n"
        "  id INTEGER PRIMARY KEY,\n"
        "  taxpayer_uid TEXT NOT NULL,\n"
        "  canton_code TEXT NOT NULL,\n"
        "  declared_turnover_chf NUMERIC(12,2) NOT NULL,\n"
        "  note TEXT NOT NULL,\n"
        "  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP\n"
        ");"
    )
    oltp_write_test_insert_sql = (
        f"INSERT INTO {oltp_write_test_table} (\n"
        "  id,\n"
        "  taxpayer_uid,\n"
        "  canton_code,\n"
        "  declared_turnover_chf,\n"
        "  note\n"
        ")\n"
        "SELECT\n"
        "  series_id,\n"
        "  'UID-' || LPAD(series_id::text, 5, '0') AS taxpayer_uid,\n"
        "  CASE MOD(series_id, 5)\n"
        "    WHEN 0 THEN 'ZH'\n"
        "    WHEN 1 THEN 'BE'\n"
        "    WHEN 2 THEN 'GE'\n"
        "    WHEN 3 THEN 'VD'\n"
        "    ELSE 'TI'\n"
        "  END AS canton_code,\n"
        "  ROUND((1500 + series_id * 87.5)::numeric, 2) AS declared_turnover_chf,\n"
        "  'OLTP write test row ' || series_id::text AS note\n"
        "FROM generate_series(1, 20) AS series(series_id);"
    )
    oltp_write_test_verify_summary_sql = (
        "SELECT\n"
        "  COUNT(*) AS inserted_rows,\n"
        "  MIN(id) AS min_id,\n"
        "  MAX(id) AS max_id,\n"
        "  CAST(ROUND(SUM(declared_turnover_chf), 2) AS NUMERIC(12,2)) AS turnover_total_chf\n"
        f"FROM {oltp_write_test_table};"
    )
    oltp_write_test_verify_rows_sql = (
        "SELECT\n"
        "  id,\n"
        "  taxpayer_uid,\n"
        "  canton_code,\n"
        "  declared_turnover_chf,\n"
        "  note,\n"
        "  created_at\n"
        f"FROM {oltp_write_test_table}\n"
        "ORDER BY id;"
    )
    oltp_write_test_cleanup_sql = (
        "-- Optional cleanup when you are done validating OLTP write access.\n"
        f"DROP TABLE IF EXISTS {oltp_write_test_table};"
    )

    return [
        NotebookDefinition(
            notebook_id="s3-smoke-test",
            title="S3 Smoke Test",
            summary="Reviews VAT filing smoke data from S3 through DuckDB for Federal Tax Administration analysis.",
            cells=[
                NotebookCellDefinition(
                    cell_id="s3-smoke-test-cell-1",
                    data_sources=["workspace.s3"],
                    sql=s3_sql,
                )
            ],
            tags=["smoke", "s3"],
            tree_path=("PoC Tests", "Smoke Tests", "Object Storage"),
            linked_generator_id="s3_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-smoke-test",
            title="PostgreSQL Smoke Test",
            summary="Queries VAT filing reference data in PostgreSQL OLTP for Federal Tax Administration smoke testing.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-smoke-test-cell-1",
                    data_sources=["pg_oltp"],
                    sql=postgres_sql,
                )
            ],
            tags=["smoke", "postgres"],
            tree_path=("PoC Tests", "Smoke Tests", "Relational"),
            linked_generator_id="postgres_oltp_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-oltp-write-test",
            title="PostgreSQL OLTP Write Test",
            summary="Creates a PostgreSQL OLTP test table, inserts 20 rows with pure SQL, verifies the inserted data, and includes an optional cleanup cell.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_setup_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-2",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_insert_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-3",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_verify_summary_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-4",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_verify_rows_sql,
                ),
                NotebookCellDefinition(
                    cell_id="postgres-oltp-write-test-cell-5",
                    data_sources=["pg_oltp_native"],
                    sql=oltp_write_test_cleanup_sql,
                ),
            ],
            tags=["smoke", "write-test", "postgres", "oltp"],
            tree_path=("PoC Tests", "Smoke Tests", "Write Access"),
            linked_generator_id="postgres_oltp_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-oltp",
            title="PG vs S3 Contest OLTP via DuckDB",
            summary="Runs a complex tax-assessment benchmark query against PostgreSQL OLTP using the mirrored contest dataset.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=contest_postgres_sql,
                )
            ],
            tags=["performance", "contest", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-s3",
            title="PG vs S3 Contest S3 via DuckDB",
            summary="Runs the same complex tax-assessment benchmark query against the mirrored S3-backed dataset.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-s3-cell-1",
                    data_sources=["workspace.s3"],
                    sql=contest_s3_sql,
                )
            ],
            tags=["performance", "contest", "s3"],
            tree_path=("PoC Tests", "Performance Evaluation"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-pg-native",
            title="PG vs S3 Contest OLTP via Native",
            summary="Runs the same complex tax-assessment benchmark query directly on PostgreSQL OLTP, without DuckDB in the execution path.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=contest_postgres_native_sql,
                )
            ],
            tags=["performance", "contest", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
    ]


def _source_option(
    source_id: str,
    label: str,
    classification: str = "Internal",
    computation_mode: str = "VMTP",
) -> dict[str, str]:
    return {
        "source_id": source_id,
        "label": label,
        "classification": classification,
        "computation_mode": computation_mode,
    }


def build_source_options(catalogs: list[SourceCatalog]) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []

    for catalog in catalogs:
        if catalog.name == "workspace":
            if catalog.schemas:
                options.append(_source_option("workspace.s3", "MinIO / S3"))
            continue

        if catalog.name == "pg_oltp":
            options.append(_source_option("pg_oltp", "PostgreSQL OLTP"))
            options.append(
                _source_option(
                    "pg_oltp_native",
                    "PostgreSQL OLTP Direct",
                    computation_mode="PostgreSQL Native",
                )
            )
            continue

        if catalog.name == "pg_olap":
            options.append(_source_option("pg_olap", "PostgreSQL OLAP"))

    return options


def build_completion_schema(catalogs: list[SourceCatalog]) -> dict[str, object]:
    schema: dict[str, object] = {}

    for catalog in catalogs:
        if catalog.name == "workspace":
            for source_schema in catalog.schemas:
                schema[source_schema.name] = [item.name for item in source_schema.objects]
            continue

        schema[catalog.name] = {
            source_schema.name: [item.name for item in source_schema.objects]
            for source_schema in catalog.schemas
        }

    return schema


def build_notebook_tree(notebooks: list[NotebookDefinition]) -> list[NotebookFolder]:
    roots: list[NotebookFolder] = []
    folder_index: dict[tuple[str, ...], NotebookFolder] = {}
    protected_roots = {"PoC Tests"}

    for notebook in notebooks:
        if not notebook.tree_path:
            continue

        path_key: tuple[str, ...] = ()
        parent_folder: NotebookFolder | None = None

        for segment in notebook.tree_path:
            path_key = (*path_key, segment)
            folder = folder_index.get(path_key)
            if folder is None:
                is_protected_path = bool(path_key) and path_key[0] in protected_roots
                folder = NotebookFolder(
                    folder_id="-".join(part.lower().replace(" ", "-") for part in path_key),
                    name=segment,
                    can_edit=not is_protected_path,
                    can_delete=not is_protected_path,
                )
                folder_index[path_key] = folder
                if parent_folder is None:
                    roots.append(folder)
                else:
                    parent_folder.folders.append(folder)
            parent_folder = folder

        parent_folder.notebooks.append(notebook)

    return roots
