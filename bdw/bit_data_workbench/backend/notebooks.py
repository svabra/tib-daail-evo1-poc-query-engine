from __future__ import annotations

from typing import Iterable

from ..models import (
    LinkedNotebookReference,
    NotebookCellDefinition,
    NotebookDefinition,
    NotebookFolder,
    SourceCatalog,
)


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


def _find_generated_s3_relation_by_object_name(
    catalogs: Iterable[SourceCatalog],
    *,
    object_names: Iterable[str],
) -> str | None:
    preferred = {name.lower(): name for name in object_names}
    fallback_relation: str | None = None
    for catalog in catalogs:
        if catalog.name != "workspace":
            continue
        for schema in catalog.schemas:
            for source_object in schema.objects:
                if source_object.name.lower() not in preferred:
                    continue
                if fallback_relation is None:
                    fallback_relation = source_object.relation
                normalized_key = str(source_object.s3_key or "").strip().lower()
                if normalized_key.startswith("generated/"):
                    return source_object.relation
    return fallback_relation


def _find_relations_by_object_names(
    catalogs: Iterable[SourceCatalog],
    *,
    catalog_name: str,
    schema_name: str | None = None,
    object_names: Iterable[str],
) -> dict[str, str | None]:
    requested_names = tuple(object_names)
    preferred = {name.lower(): name for name in requested_names}
    relations: dict[str, str | None] = {name: None for name in requested_names}
    for catalog in catalogs:
        if catalog.name != catalog_name:
            continue
        for schema in catalog.schemas:
            if schema_name is not None and schema.name != schema_name:
                continue
            for source_object in schema.objects:
                preferred_name = preferred.get(source_object.name.lower())
                if preferred_name is None or relations[preferred_name] is not None:
                    continue
                relations[preferred_name] = source_object.relation
    return relations


def _strip_catalog_prefix(relation: str | None, catalog_name: str) -> str | None:
    normalized_relation = (relation or "").strip()
    prefix = f"{catalog_name}."
    if not normalized_relation:
        return None
    if normalized_relation.startswith(prefix):
        return normalized_relation[len(prefix) :]
    return normalized_relation


def _build_multi_table_performance_sql(
    *,
    taxpayers_relation: str,
    filings_relation: str,
    assessments_relation: str,
    payments_relation: str,
    audits_relation: str,
    enforcements_relation: str,
    appeals_relation: str,
) -> str:
    return (
        "-- Approximation: highlight quarterly cantonal tax hotspots with high open exposure.\n"
        "-- Logic: join taxpayers, filings, assessments, payments, audits, enforcements, and appeals.\n"
        "-- Result: surface compliance-pressure and appeal-heavy segments by canton, tax type, and sector.\n"
        "WITH active_taxpayers AS (\n"
        "  SELECT\n"
        "    taxpayer_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    industry_sector,\n"
        "    taxpayer_type,\n"
        "    registration_status,\n"
        "    risk_tier\n"
        f"  FROM {taxpayers_relation}\n"
        "  WHERE registration_status IN ('active', 'watchlist')\n"
        "),\n"
        "filing_scope AS (\n"
        "  SELECT\n"
        "    f.filing_id,\n"
        "    f.taxpayer_id,\n"
        "    atp.taxpayer_uid,\n"
        "    atp.canton_code,\n"
        "    atp.industry_sector,\n"
        "    atp.taxpayer_type,\n"
        "    atp.risk_tier,\n"
        "    f.tax_type,\n"
        "    f.filing_status,\n"
        "    CAST(date_trunc('quarter', f.tax_period_end) AS DATE) AS tax_quarter_start,\n"
        "    f.declared_revenue_chf,\n"
        "    f.declared_deduction_chf,\n"
        "    a.assessment_id,\n"
        "    a.assessment_status,\n"
        "    a.assessed_tax_chf,\n"
        "    a.surcharge_chf,\n"
        "    a.waiver_chf,\n"
        "    a.due_date\n"
        f"  FROM {filings_relation} AS f\n"
        "  JOIN active_taxpayers AS atp\n"
        "    ON atp.taxpayer_id = f.taxpayer_id\n"
        f"  JOIN {assessments_relation} AS a\n"
        "    ON a.filing_id = f.filing_id\n"
        "  WHERE f.tax_period_end >= DATE '2024-01-01'\n"
        "    AND f.tax_type IN ('VAT', 'COMPANY_TAX', 'WITHHOLDING_TAX', 'STAMP_DUTY')\n"
        "    AND f.filing_status IN ('under_review', 'assessed', 'escalated', 'closed')\n"
        "),\n"
        "payment_rollup AS (\n"
        "  SELECT\n"
        "    assessment_id,\n"
        "    CAST(ROUND(SUM(collected_tax_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(\n"
        "      ROUND(\n"
        "        SUM(CASE WHEN payment_status IN ('late', 'partial', 'pending') THEN collected_tax_chf ELSE 0 END),\n"
        "        2\n"
        "      )\n"
        "      AS DECIMAL(18,2)\n"
        "    ) AS stressed_collection_chf,\n"
        "    COUNT(*) AS payment_event_count\n"
        f"  FROM {payments_relation}\n"
        "  WHERE settled_at >= TIMESTAMP '2024-01-01 00:00:00'\n"
        "  GROUP BY assessment_id\n"
        "),\n"
        "audit_rollup AS (\n"
        "  SELECT\n"
        "    filing_id,\n"
        "    MAX(audit_risk_score) AS max_audit_risk_score,\n"
        "    CAST(ROUND(SUM(additional_tax_chf), 2) AS DECIMAL(18,2)) AS additional_tax_total_chf,\n"
        "    SUM(CASE WHEN finding_severity IN ('high', 'critical') THEN 1 ELSE 0 END) AS severe_finding_count\n"
        f"  FROM {audits_relation}\n"
        "  WHERE audit_status IN ('open', 'closed', 'escalated')\n"
        "  GROUP BY filing_id\n"
        "),\n"
        "enforcement_rollup AS (\n"
        "  SELECT\n"
        "    assessment_id,\n"
        "    COUNT(*) AS enforcement_action_count,\n"
        "    CAST(ROUND(SUM(enforced_amount_chf), 2) AS DECIMAL(18,2)) AS enforced_amount_total_chf,\n"
        "    MAX(CASE WHEN action_status IN ('active', 'escalated') THEN 1 ELSE 0 END) AS has_active_enforcement\n"
        f"  FROM {enforcements_relation}\n"
        "  WHERE action_stage IN ('notice', 'collection', 'legal')\n"
        "  GROUP BY assessment_id\n"
        "),\n"
        "appeal_rollup AS (\n"
        "  SELECT\n"
        "    assessment_id,\n"
        "    COUNT(*) AS appeal_count,\n"
        "    CAST(ROUND(SUM(contested_amount_chf), 2) AS DECIMAL(18,2)) AS contested_amount_total_chf,\n"
        "    MAX(CASE WHEN appeal_status IN ('open', 'escalated') THEN 1 ELSE 0 END) AS has_open_appeal\n"
        f"  FROM {appeals_relation}\n"
        "  WHERE ruling_stage IN ('cantonal', 'federal', 'tribunal')\n"
        "  GROUP BY assessment_id\n"
        "),\n"
        "joined_positions AS (\n"
        "  SELECT\n"
        "    fs.canton_code,\n"
        "    fs.tax_type,\n"
        "    fs.industry_sector,\n"
        "    fs.taxpayer_type,\n"
        "    fs.risk_tier,\n"
        "    fs.tax_quarter_start,\n"
        "    fs.filing_id,\n"
        "    fs.assessment_id,\n"
        "    fs.assessed_tax_chf,\n"
        "    fs.surcharge_chf,\n"
        "    fs.waiver_chf,\n"
        "    COALESCE(pr.collected_tax_total_chf, 0) AS collected_tax_total_chf,\n"
        "    COALESCE(pr.stressed_collection_chf, 0) AS stressed_collection_chf,\n"
        "    COALESCE(pr.payment_event_count, 0) AS payment_event_count,\n"
        "    COALESCE(ar.max_audit_risk_score, 0) AS max_audit_risk_score,\n"
        "    COALESCE(ar.additional_tax_total_chf, 0) AS additional_tax_total_chf,\n"
        "    COALESCE(ar.severe_finding_count, 0) AS severe_finding_count,\n"
        "    COALESCE(er.enforcement_action_count, 0) AS enforcement_action_count,\n"
        "    COALESCE(er.enforced_amount_total_chf, 0) AS enforced_amount_total_chf,\n"
        "    COALESCE(er.has_active_enforcement, 0) AS has_active_enforcement,\n"
        "    COALESCE(apr.appeal_count, 0) AS appeal_count,\n"
        "    COALESCE(apr.contested_amount_total_chf, 0) AS contested_amount_total_chf,\n"
        "    COALESCE(apr.has_open_appeal, 0) AS has_open_appeal,\n"
        "    GREATEST(\n"
        "      (fs.assessed_tax_chf + fs.surcharge_chf - fs.waiver_chf) - COALESCE(pr.collected_tax_total_chf, 0),\n"
        "      0\n"
        "    ) AS open_tax_exposure_chf\n"
        "  FROM filing_scope AS fs\n"
        "  LEFT JOIN payment_rollup AS pr\n"
        "    ON pr.assessment_id = fs.assessment_id\n"
        "  LEFT JOIN audit_rollup AS ar\n"
        "    ON ar.filing_id = fs.filing_id\n"
        "  LEFT JOIN enforcement_rollup AS er\n"
        "    ON er.assessment_id = fs.assessment_id\n"
        "  LEFT JOIN appeal_rollup AS apr\n"
        "    ON apr.assessment_id = fs.assessment_id\n"
        "),\n"
        "compliance_pressure AS (\n"
        "  SELECT\n"
        "    'compliance_pressure' AS segment,\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    tax_quarter_start,\n"
        "    COUNT(*) AS assessment_count,\n"
        "    CAST(ROUND(SUM(assessed_tax_chf + surcharge_chf - waiver_chf), 2) AS DECIMAL(18,2)) AS gross_assessed_total_chf,\n"
        "    CAST(ROUND(SUM(collected_tax_total_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(ROUND(SUM(open_tax_exposure_chf), 2) AS DECIMAL(18,2)) AS open_tax_exposure_total_chf,\n"
        "    CAST(CAST(AVG(max_audit_risk_score) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_audit_risk_score,\n"
        "    SUM(severe_finding_count) AS severe_finding_count,\n"
        "    SUM(enforcement_action_count) AS enforcement_action_count,\n"
        "    SUM(appeal_count) AS appeal_count,\n"
        "    SUM(CASE WHEN has_active_enforcement = 1 OR has_open_appeal = 1 THEN 1 ELSE 0 END) AS escalated_case_count\n"
        "  FROM joined_positions\n"
        "  WHERE taxpayer_type IN ('Corporation', 'SME', 'Importer')\n"
        "    AND risk_tier IN ('high', 'critical', 'medium')\n"
        "  GROUP BY canton_code, tax_type, industry_sector, tax_quarter_start\n"
        "  HAVING COUNT(*) >= 20\n"
        "    AND SUM(open_tax_exposure_chf) >= 250000\n"
        "),\n"
        "appeal_exposure AS (\n"
        "  SELECT\n"
        "    'appeal_exposure' AS segment,\n"
        "    canton_code,\n"
        "    tax_type,\n"
        "    industry_sector,\n"
        "    tax_quarter_start,\n"
        "    COUNT(*) AS assessment_count,\n"
        "    CAST(ROUND(SUM(assessed_tax_chf + surcharge_chf - waiver_chf), 2) AS DECIMAL(18,2)) AS gross_assessed_total_chf,\n"
        "    CAST(ROUND(SUM(collected_tax_total_chf), 2) AS DECIMAL(18,2)) AS collected_tax_total_chf,\n"
        "    CAST(ROUND(SUM(open_tax_exposure_chf), 2) AS DECIMAL(18,2)) AS open_tax_exposure_total_chf,\n"
        "    CAST(CAST(AVG(max_audit_risk_score) AS DECIMAL(18,2)) AS DOUBLE PRECISION) AS avg_audit_risk_score,\n"
        "    SUM(severe_finding_count) AS severe_finding_count,\n"
        "    SUM(enforcement_action_count) AS enforcement_action_count,\n"
        "    SUM(appeal_count) AS appeal_count,\n"
        "    SUM(CASE WHEN has_active_enforcement = 1 OR has_open_appeal = 1 THEN 1 ELSE 0 END) AS escalated_case_count\n"
        "  FROM joined_positions\n"
        "  WHERE has_open_appeal = 1 OR has_active_enforcement = 1\n"
        "  GROUP BY canton_code, tax_type, industry_sector, tax_quarter_start\n"
        "  HAVING COUNT(*) >= 8\n"
        "    AND SUM(contested_amount_total_chf) >= 100000\n"
        ")\n"
        "SELECT\n"
        "  segment,\n"
        "  canton_code,\n"
        "  tax_type,\n"
        "  industry_sector,\n"
        "  tax_quarter_start,\n"
        "  assessment_count,\n"
        "  gross_assessed_total_chf,\n"
        "  collected_tax_total_chf,\n"
        "  open_tax_exposure_total_chf,\n"
        "  avg_audit_risk_score,\n"
        "  severe_finding_count,\n"
        "  enforcement_action_count,\n"
        "  appeal_count,\n"
        "  escalated_case_count\n"
        "FROM compliance_pressure\n"
        "UNION ALL\n"
        "SELECT\n"
        "  segment,\n"
        "  canton_code,\n"
        "  tax_type,\n"
        "  industry_sector,\n"
        "  tax_quarter_start,\n"
        "  assessment_count,\n"
        "  gross_assessed_total_chf,\n"
        "  collected_tax_total_chf,\n"
        "  open_tax_exposure_total_chf,\n"
        "  avg_audit_risk_score,\n"
        "  severe_finding_count,\n"
        "  enforcement_action_count,\n"
        "  appeal_count,\n"
        "  escalated_case_count\n"
        "FROM appeal_exposure\n"
        "WHERE gross_assessed_total_chf >= 500000\n"
        "ORDER BY open_tax_exposure_total_chf DESC, avg_audit_risk_score DESC, gross_assessed_total_chf DESC\n"
        "LIMIT 40;"
    )


def build_generator_notebook_links(
    notebooks: Iterable[NotebookDefinition],
) -> dict[str, list[LinkedNotebookReference]]:
    linked_notebooks: dict[str, list[LinkedNotebookReference]] = {}
    seen_notebook_ids: dict[str, set[str]] = {}

    for notebook in notebooks:
        generator_id = str(notebook.linked_generator_id or "").strip()
        notebook_id = str(notebook.notebook_id or "").strip()
        if not generator_id or not notebook_id:
            continue

        generator_seen = seen_notebook_ids.setdefault(generator_id, set())
        if notebook_id in generator_seen:
            continue

        generator_seen.add(notebook_id)
        linked_notebooks.setdefault(generator_id, []).append(
            LinkedNotebookReference(
                notebook_id=notebook_id,
                title=notebook.title,
            )
        )

    return linked_notebooks


def build_notebooks(catalogs: list[SourceCatalog]) -> list[NotebookDefinition]:
    multi_table_object_names = (
        "federal_tax_taxpayers_mt",
        "federal_tax_filings_mt",
        "federal_tax_assessments_mt",
        "federal_tax_payments_mt",
        "federal_tax_audits_mt",
        "federal_tax_enforcements_mt",
        "federal_tax_appeals_mt",
    )
    preferred_s3_relation = _find_generated_s3_relation_by_object_name(
        catalogs,
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
    multi_table_postgres_relations = _find_relations_by_object_names(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=multi_table_object_names,
    )
    multi_table_s3_relations = _find_relations_by_object_names(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=multi_table_object_names,
    )
    union_oltp_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=("pg_union_tax_reference",),
    )
    union_olap_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="pg_olap",
        schema_name="public",
        object_names=("pg_union_tax_reference",),
    )
    union_oltp_s3_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=("pg_union_tax_reference_s3",),
    )
    union_s3_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=("pg_union_tax_reference_s3",),
    )
    contest_postgres_native_relation = _strip_catalog_prefix(contest_postgres_relation, "pg_oltp")
    multi_table_postgres_native_relations = {
        object_name: _strip_catalog_prefix(relation, "pg_oltp")
        for object_name, relation in multi_table_postgres_relations.items()
    }

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
        "-- Approximation: summarize quarterly tax-assessment pressure across cantons and sectors.\n"
        "-- Logic: filter recent assessments, aggregate assessed, collected, and open balances, and keep only large exposure groups.\n"
        "-- Result: rank the tax segments with the highest open balance and audit pressure.\n"
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
    multi_table_status_sql = (
        "SELECT 'Run the PG vs S3 Multi-Table Federal Tax Loader from the Ingestion Workbench first.' AS status;"
    )
    multi_table_postgres_sql = (
        _build_multi_table_performance_sql(
            taxpayers_relation=multi_table_postgres_relations["federal_tax_taxpayers_mt"],
            filings_relation=multi_table_postgres_relations["federal_tax_filings_mt"],
            assessments_relation=multi_table_postgres_relations["federal_tax_assessments_mt"],
            payments_relation=multi_table_postgres_relations["federal_tax_payments_mt"],
            audits_relation=multi_table_postgres_relations["federal_tax_audits_mt"],
            enforcements_relation=multi_table_postgres_relations["federal_tax_enforcements_mt"],
            appeals_relation=multi_table_postgres_relations["federal_tax_appeals_mt"],
        )
        if all(multi_table_postgres_relations.values())
        else multi_table_status_sql
    )
    multi_table_s3_sql = (
        _build_multi_table_performance_sql(
            taxpayers_relation=multi_table_s3_relations["federal_tax_taxpayers_mt"],
            filings_relation=multi_table_s3_relations["federal_tax_filings_mt"],
            assessments_relation=multi_table_s3_relations["federal_tax_assessments_mt"],
            payments_relation=multi_table_s3_relations["federal_tax_payments_mt"],
            audits_relation=multi_table_s3_relations["federal_tax_audits_mt"],
            enforcements_relation=multi_table_s3_relations["federal_tax_enforcements_mt"],
            appeals_relation=multi_table_s3_relations["federal_tax_appeals_mt"],
        )
        if all(multi_table_s3_relations.values())
        else multi_table_status_sql
    )
    multi_table_postgres_native_sql = (
        _build_multi_table_performance_sql(
            taxpayers_relation=multi_table_postgres_native_relations["federal_tax_taxpayers_mt"],
            filings_relation=multi_table_postgres_native_relations["federal_tax_filings_mt"],
            assessments_relation=multi_table_postgres_native_relations["federal_tax_assessments_mt"],
            payments_relation=multi_table_postgres_native_relations["federal_tax_payments_mt"],
            audits_relation=multi_table_postgres_native_relations["federal_tax_audits_mt"],
            enforcements_relation=multi_table_postgres_native_relations["federal_tax_enforcements_mt"],
            appeals_relation=multi_table_postgres_native_relations["federal_tax_appeals_mt"],
        )
        if all(multi_table_postgres_native_relations.values())
        else multi_table_status_sql
    )
    cross_database_union_sql = (
        "-- Approximation: compare how the same tax-position reference shape behaves across OLTP and OLAP.\n"
        "-- Logic: UNION identical columns from PostgreSQL OLTP and PostgreSQL OLAP, then roll them up by source and risk slice.\n"
        "-- Result: highlight which database contributes the largest net tax totals per canton, status, and risk band.\n"
        "WITH combined_tax_positions AS (\n"
        "  SELECT\n"
        "    'OLTP' AS database_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_oltp_relation}\n"
        "  UNION\n"
        "  SELECT\n"
        "    'OLAP' AS database_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_olap_relation}\n"
        "),\n"
        "aggregated_positions AS (\n"
        "  SELECT\n"
        "    database_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    COUNT(*) AS record_count,\n"
        "    CAST(ROUND(SUM(net_tax_amount_chf), 2) AS DECIMAL(18,2)) AS net_tax_amount_total_chf,\n"
        "    MIN(tax_period_end) AS earliest_tax_period_end,\n"
        "    MAX(tax_period_end) AS latest_tax_period_end\n"
        "  FROM combined_tax_positions\n"
        "  GROUP BY database_name, canton_code, processing_status, risk_band\n"
        "),\n"
        "ranked_positions AS (\n"
        "  SELECT\n"
        "    database_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    record_count,\n"
        "    net_tax_amount_total_chf,\n"
        "    earliest_tax_period_end,\n"
        "    latest_tax_period_end,\n"
        "    ROW_NUMBER() OVER (\n"
        "      PARTITION BY database_name\n"
        "      ORDER BY net_tax_amount_total_chf DESC, canton_code, processing_status, risk_band\n"
        "    ) AS source_rank\n"
        "  FROM aggregated_positions\n"
        ")\n"
        "SELECT\n"
        "  database_name,\n"
        "  canton_code,\n"
        "  processing_status,\n"
        "  risk_band,\n"
        "  record_count,\n"
        "  net_tax_amount_total_chf,\n"
        "  earliest_tax_period_end,\n"
        "  latest_tax_period_end\n"
        "FROM ranked_positions\n"
        "WHERE source_rank <= 30\n"
        "ORDER BY database_name, source_rank, canton_code, processing_status, risk_band;"
        if union_oltp_relation and union_olap_relation
        else "SELECT 'Run the PostgreSQL OLTP + OLAP UNION Loader from the Ingestion Workbench first.' AS status;"
    )
    cross_source_union_sql = (
        "-- Approximation: compare the same tax-position reference shape across OLTP and S3-backed object storage.\n"
        "-- Logic: UNION matching OLTP rows with the mirrored S3 dataset through DuckDB, then roll them up by source and risk slice.\n"
        "-- Result: show which source contributes the largest net tax totals per canton, status, and risk band.\n"
        "WITH combined_tax_positions AS (\n"
        "  SELECT\n"
        "    'OLTP' AS source_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_oltp_s3_relation}\n"
        "  UNION\n"
        "  SELECT\n"
        "    'S3' AS source_name,\n"
        "    record_id,\n"
        "    taxpayer_uid,\n"
        "    canton_code,\n"
        "    tax_period_end,\n"
        "    net_tax_amount_chf,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    updated_at\n"
        f"  FROM {union_s3_relation}\n"
        "),\n"
        "aggregated_positions AS (\n"
        "  SELECT\n"
        "    source_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    COUNT(*) AS record_count,\n"
        "    CAST(ROUND(SUM(net_tax_amount_chf), 2) AS DECIMAL(18,2)) AS net_tax_amount_total_chf,\n"
        "    MIN(tax_period_end) AS earliest_tax_period_end,\n"
        "    MAX(tax_period_end) AS latest_tax_period_end,\n"
        "    MIN(updated_at) AS first_update_at,\n"
        "    MAX(updated_at) AS last_update_at\n"
        "  FROM combined_tax_positions\n"
        "  GROUP BY source_name, canton_code, processing_status, risk_band\n"
        "),\n"
        "ranked_positions AS (\n"
        "  SELECT\n"
        "    source_name,\n"
        "    canton_code,\n"
        "    processing_status,\n"
        "    risk_band,\n"
        "    record_count,\n"
        "    net_tax_amount_total_chf,\n"
        "    earliest_tax_period_end,\n"
        "    latest_tax_period_end,\n"
        "    first_update_at,\n"
        "    last_update_at,\n"
        "    ROW_NUMBER() OVER (\n"
        "      PARTITION BY source_name\n"
        "      ORDER BY net_tax_amount_total_chf DESC, canton_code, processing_status, risk_band\n"
        "    ) AS source_rank\n"
        "  FROM aggregated_positions\n"
        ")\n"
        "SELECT\n"
        "  source_name,\n"
        "  canton_code,\n"
        "  processing_status,\n"
        "  risk_band,\n"
        "  record_count,\n"
        "  net_tax_amount_total_chf,\n"
        "  earliest_tax_period_end,\n"
        "  latest_tax_period_end,\n"
        "  first_update_at,\n"
        "  last_update_at\n"
        "FROM ranked_positions\n"
        "WHERE source_rank <= 30\n"
        "ORDER BY source_name, source_rank, canton_code, processing_status, risk_band;"
        if union_oltp_s3_relation and union_s3_relation
        else "SELECT 'Run the PostgreSQL OLTP + S3 UNION Loader from the Ingestion Workbench first.' AS status;"
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
            notebook_id="postgres-oltp-olap-union-test",
            title="PostgreSQL OLTP + OLAP UNION",
            summary="Executes a UNION across PostgreSQL OLTP and PostgreSQL OLAP using the same reference structure in both databases.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-oltp-olap-union-test-cell-1",
                    data_sources=["pg_oltp", "pg_olap"],
                    sql=cross_database_union_sql,
                )
            ],
            tags=["sql", "union", "postgres", "oltp", "olap"],
            tree_path=("PoC Tests", "SQL Functionalities"),
            linked_generator_id="pg_union_sql_functionality_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-oltp-s3-union-test",
            title="PostgreSQL OLTP + S3 UNION",
            summary="Executes a UNION across PostgreSQL OLTP and mirrored S3-backed reference data through DuckDB using the same structure in both sources.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-oltp-s3-union-test-cell-1",
                    data_sources=["pg_oltp", "workspace.s3"],
                    sql=cross_source_union_sql,
                )
            ],
            tags=["sql", "union", "postgres", "oltp", "s3"],
            tree_path=("PoC Tests", "SQL Functionalities"),
            linked_generator_id="pg_union_sql_functionality_s3_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-oltp",
            title="PG vs S3 Contest OLTP via DuckDB",
            summary="Approximates a quarterly tax-pressure dashboard by aggregating recent tax assessments in PostgreSQL OLTP and ranking cantons, tax types, and sectors with high open balances and audit pressure.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=contest_postgres_sql,
                )
            ],
            tags=["performance", "contest", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "Single-Table Test"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-s3",
            title="PG vs S3 Contest S3 via DuckDB",
            summary="Runs the same quarterly tax-pressure aggregation against the mirrored S3 dataset to compare how the same high-exposure tax segments perform through DuckDB on object storage.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-s3-cell-1",
                    data_sources=["workspace.s3"],
                    sql=contest_s3_sql,
                )
            ],
            tags=["performance", "contest", "s3"],
            tree_path=("PoC Tests", "Performance Evaluation", "Single-Table Test"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-pg-native",
            title="PG vs S3 Contest OLTP via Native",
            summary="Runs the same quarterly tax-pressure aggregation directly inside PostgreSQL OLTP, without DuckDB, to compare native execution on the high-exposure tax segments.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=contest_postgres_native_sql,
                )
            ],
            tags=["performance", "contest", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Single-Table Test"),
            linked_generator_id="pg_vs_s3_contest_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-multi-table-oltp",
            title="Multi-Table Test OLTP via DuckDB",
            summary="Approximates a federal-tax risk dashboard by joining taxpayers, filings, assessments, payments, audits, enforcements, and appeals, then ranking quarterly cantonal segments with the highest open exposure through DuckDB on PostgreSQL OLTP.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-multi-table-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=multi_table_postgres_sql,
                )
            ],
            tags=["performance", "multi-table", "contest", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "Multi-Table Test"),
            linked_generator_id="pg_vs_s3_multi_table_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-multi-table-s3",
            title="Multi-Table Test S3 via DuckDB",
            summary="Runs the same federal-tax risk dashboard query against the mirrored S3-backed tables through DuckDB to compare quarterly compliance-pressure and appeal-heavy segments on object storage.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-multi-table-s3-cell-1",
                    data_sources=["workspace.s3"],
                    sql=multi_table_s3_sql,
                )
            ],
            tags=["performance", "multi-table", "contest", "s3"],
            tree_path=("PoC Tests", "Performance Evaluation", "Multi-Table Test"),
            linked_generator_id="pg_vs_s3_multi_table_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-multi-table-pg-native",
            title="Multi-Table Test OLTP via Native",
            summary="Runs the same federal-tax risk dashboard query directly inside PostgreSQL OLTP, without DuckDB, to compare native execution for the same joined quarterly exposure analysis.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-multi-table-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=multi_table_postgres_native_sql,
                )
            ],
            tags=["performance", "multi-table", "contest", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Multi-Table Test"),
            linked_generator_id="pg_vs_s3_multi_table_loader",
            can_edit=False,
            can_delete=False,
        ),
    ]


def _source_option(
    source_id: str,
    label: str,
    classification: str = "Internal",
    computation_mode: str = "VMTP",
    storage_tooltip: str = "",
) -> dict[str, str]:
    return {
        "source_id": source_id,
        "label": label,
        "classification": classification,
        "computation_mode": computation_mode,
        "storage_tooltip": storage_tooltip,
    }


def build_source_options(catalogs: list[SourceCatalog]) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []

    for catalog in catalogs:
        if catalog.connection_source_id == "workspace.local":
            options.append(
                _source_option(
                    "workspace.local",
                    "Local Workspace",
                    classification="Workspace Storage",
                    computation_mode="Browser-managed",
                    storage_tooltip=(
                        "Stored in this browser profile under this app's "
                        "origin using IndexedDB."
                    ),
                )
            )
            continue

        if catalog.name == "workspace":
            if catalog.schemas:
                options.append(
                    _source_option(
                        "workspace.s3",
                        "Shared Workspace",
                        classification="Workspace Storage",
                        storage_tooltip=(
                            "Stored in the configured MinIO / S3 bucket and "
                            "available to the running workbench instance."
                        ),
                    )
                )
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
