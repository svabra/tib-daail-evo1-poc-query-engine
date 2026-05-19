# flake8: noqa
# ruff: noqa: E501

from __future__ import annotations

import re

from ..models import NotebookCellDefinition, NotebookDefinition


KOSTENBELEGE_3_1_SOURCE_COLUMNS = {
    "KBKP": (
        "KBKP_Belegnummer",
        "DOCO_Belegart",
        "KBKP_BelegDt",
        "KBKP_BuchungDt",
        "KBKP_ErstellungVon",
        "KBKP_StorniertBelegNummer",
        "KBKP_StornoBelegNummer",
        "DOCO_BelegHerkunft",
        "DOCO_Buchunggrund",
        "KBKP_TechBeginnDt",
        "KBKP_TechEndeDt",
    ),
    "KBPO": (
        "KBPO_PositionId",
        "KBKP_AusgleichBelegnummer",
        "KBPO_VtgKtoWiederholPos",
        "KBPO_VtgKtoPositionNr",
        "KBPO_Teilposition",
        "GEFA_GeschaeftFall",
        "PART_Partner",
        "KBPO_KtoFindMerkmal",
        "DOCO_Hauptvorgang",
        "DOCO_Teilvorgang",
        "DOCO_Belegtyp",
        "DOCO_VtrKtoTyp",
        "DOCO_Waehrung",
        "DOCO_FormArt",
        "KBPO_GesamtBetrag",
        "KBPO_TWhrBetrag",
        "KBPO_HbWaehrung",
        "KBPO_HbBetrag",
        "KBPO_HWhrBetrag1",
        "KBPO_Umrechnungkurs",
        "KBPO_NettoFaelligkeitDT",
        "VTGP_VtrGegenstand",
        "KBPO_VtrKtoNummer",
        "KBPO_AusgleichStatus",
        "KBPO_Ausgleichgrund",
        "KBPO_AusgleichDt",
        "KBPO_AusgleichBuchungDt",
        "KBPO_HBSachkto",
        "KBPO_Beschreibung",
        "DOCO_SteuerCd",
        "KBPO_WertInternDt",
        "KBPO_Bankverbindung",
        "DOCO_RecordArt",
        "KBPO_TechBeginnDt",
        "KBPO_TechEndeDt",
    ),
    "KBHP": (
        "KBHP_Id",
        "KBKP_BelegNummer",
        "KBHP_VTGKtoPositionNr",
        "KBHP_SachKto",
        "KBHP_HBAbstimmschluessel",
        "KBHP_TechBeginnDt",
        "KBHP_TechEndeDt",
    ),
    "KBHH": (
        "KBHP_Id",
        "KBKP_BelegNummer",
        "KBHP_VTGKtoPositionNr",
        "KBHP_SachKto",
        "KBHP_HBAbstimmschluessel",
        "KBHP_TechBeginnDt",
        "KBHP_TechEndeDt",
    ),
    "KALE": ("Datum",),
}
KOSTENBELEGE_3_1_SOURCE_COLUMN_LOOKUP = {
    alias: {column.lower(): column for column in columns}
    for alias, columns in KOSTENBELEGE_3_1_SOURCE_COLUMNS.items()
}


def _quote_kostenbelege_3_1_source_columns(sql: str) -> str:
    def replace(match: re.Match[str]) -> str:
        alias = match.group(1)
        column = match.group(2)
        canonical_column = KOSTENBELEGE_3_1_SOURCE_COLUMN_LOOKUP.get(alias, {}).get(
            column.lower(),
            column,
        )
        return f'{alias}."{canonical_column}"'

    return re.sub(
        r"\b(KBKP|KBPO|KBHP|KBHH|KALE)\.([A-Za-z_][A-Za-z0-9_]*)",
        replace,
        sql,
    )


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


def _build_mwa_abrechnung_performance_sql(
    *,
    abrechnung_relation: str,
    ziffern_relation: str,
) -> str:
    return (
        "-- Approximation: summarize MWA Abrechnung submissions with their ziffer totals.\n"
        "-- Logic: join Abrechnung parent rows to Abrechnungs-Ziffern children and aggregate by quarter, status, and approval readiness.\n"
        "-- Result: rank the MWA segments with the largest submitted turnover and tax totals.\n"
        "WITH scoped_abrechnungen AS (\n"
        "  SELECT\n"
        "    id_,\n"
        "    status,\n"
        "    CAST(date_trunc('quarter', einreiche_datum) AS DATE) AS submission_quarter,\n"
        "    contact_role,\n"
        "    is_approved,\n"
        "    is_ready_for_audit,\n"
        "    is_sub_form1056_needed,\n"
        "    tax_period_refer,\n"
        "    partner_id,\n"
        "    CAST(CAST(rounded_total AS DOUBLE PRECISION) AS DECIMAL(18,2)) AS rounded_total_chf\n"
        f"  FROM {abrechnung_relation}\n"
        "  WHERE deleted_at IS NULL\n"
        "    AND einreiche_datum >= TIMESTAMP '2024-01-01 00:00:00'\n"
        "),\n"
        "ziffer_rollup AS (\n"
        "  SELECT\n"
        "    abrechnung_refer,\n"
        "    COUNT(*) AS ziffer_count,\n"
        "    CAST(SUM(umsatz) AS DECIMAL(18,2)) AS umsatz_total,\n"
        "    CAST(SUM(steuer) AS DECIMAL(18,2)) AS steuer_total,\n"
        "    CAST(AVG(satz) AS DECIMAL(18,2)) AS avg_satz,\n"
        "    SUM(CASE WHEN satz_editable = 'true' THEN 1 ELSE 0 END) AS editable_ziffer_count\n"
        f"  FROM {ziffern_relation}\n"
        "  WHERE deleted_at IS NULL\n"
        "  GROUP BY abrechnung_refer\n"
        "),\n"
        "joined_abrechnungen AS (\n"
        "  SELECT\n"
        "    a.status,\n"
        "    a.submission_quarter,\n"
        "    a.contact_role,\n"
        "    a.is_approved,\n"
        "    a.is_ready_for_audit,\n"
        "    a.is_sub_form1056_needed,\n"
        "    a.tax_period_refer,\n"
        "    a.partner_id,\n"
        "    a.rounded_total_chf,\n"
        "    COALESCE(z.ziffer_count, 0) AS ziffer_count,\n"
        "    COALESCE(z.umsatz_total, 0) AS umsatz_total,\n"
        "    COALESCE(z.steuer_total, 0) AS steuer_total,\n"
        "    COALESCE(z.avg_satz, 0) AS avg_satz,\n"
        "    COALESCE(z.editable_ziffer_count, 0) AS editable_ziffer_count\n"
        "  FROM scoped_abrechnungen AS a\n"
        "  LEFT JOIN ziffer_rollup AS z\n"
        "    ON z.abrechnung_refer = a.id_\n"
        ")\n"
        "SELECT\n"
        "  submission_quarter,\n"
        "  status,\n"
        "  is_approved,\n"
        "  is_ready_for_audit,\n"
        "  is_sub_form1056_needed,\n"
        "  COUNT(*) AS abrechnung_count,\n"
        "  SUM(ziffer_count) AS ziffer_count,\n"
        "  CAST(SUM(rounded_total_chf) AS DECIMAL(18,2)) AS rounded_total_chf,\n"
        "  CAST(SUM(umsatz_total) AS DECIMAL(18,2)) AS umsatz_total,\n"
        "  CAST(SUM(steuer_total) AS DECIMAL(18,2)) AS steuer_total,\n"
        "  CAST(AVG(avg_satz) AS DECIMAL(18,2)) AS avg_satz,\n"
        "  SUM(editable_ziffer_count) AS editable_ziffer_count,\n"
        "  COUNT(DISTINCT partner_id) AS partner_count\n"
        "FROM joined_abrechnungen\n"
        "GROUP BY submission_quarter, status, is_approved, is_ready_for_audit, is_sub_form1056_needed\n"
        "HAVING COUNT(*) >= 10\n"
        "ORDER BY rounded_total_chf DESC, steuer_total DESC, abrechnung_count DESC\n"
        "LIMIT 40;"
    )


def _build_kostenbelege_3_1_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
    quote_source_columns: bool = False,
) -> str:
    sql = f"""
WITH UNIO AS
    (
    SELECT
          KBKP.KBKP_Belegnummer                                                   AS KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos                                            AS KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart                                                      AS DOCO_Belegart
        , KBKP.KBKP_BelegDt                                                       AS KBKP_BelegDt
        , KBKP.KBKP_BuchungDt                                                     AS KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon                                                 AS KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer                                          AS KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer                                             AS KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft                                                 AS DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr                                              AS KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition                                                  AS KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall                                                 AS GEFA_GeschaeftFall
        , KBPO.PART_Partner                                                       AS PART_Partner
        , KBPO.KBPO_KtoFindMerkmal                                                AS KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang                                                  AS DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang                                                   AS DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp                                                      AS DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp                                                     AS DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung                                                      AS DOCO_Waehrung
        , KBPO.DOCO_FormArt                                                       AS DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag                                                  AS KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag                                                    AS KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung                                                    AS KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag                                                      AS KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1                                                   AS KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs                                                AS KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT                                            AS KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand                                                 AS VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer                                                  AS KBPO_VtrKtoNummer
        , KBPO.KBKP_ausgleichbelegnummer                                          AS KBKP_ausgleichbelegnummer
        , KBPO.KBPO_ausgleichstatus                                               AS KBPO_ausgleichstatus
        , KBPO.KBPO_Ausgleichgrund                                                AS KBPO_Ausgleichgrund
        , KBPO.KBPO_AusgleichDt                                                   AS KBPO_AusgleichDt
        , KBPO.KBPO_AusgleichBuchungDt                                            AS KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto                                                     AS KBPO_HBSachkto
        , COALESCE(KBHP.KBHP_SachKto, KBHH.KBHP_SachKto)                          AS KBHP_SachKto
        , COALESCE(KBHP.KBHP_HBAbstimmschluessel, KBHH.KBHP_HBAbstimmschluessel)  AS KBHP_HBAbstimmschluessel
        , KBPO.KBPO_Beschreibung                                                  AS KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd                                                      AS DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt                                                  AS KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung                                                AS KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt                                                     AS DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund                                                  AS DOCO_Buchunggrund
        , CAST('Originalposition' AS VARCHAR(20))                                 AS PositionsArt
        , KALE.Datum                                                              AS Datum
    FROM {kbkp_relation} KBKP
    INNER JOIN {kalender_relation} KALE
        ON  KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
        AND KALE.Datum = CURRENT_DATE
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHP
        ON  KBKP.KBKP_BelegNummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = KBPO.KBPO_VtgKtoPositionNr
        AND KALE.Datum BETWEEN KBHP.KBHP_TechBeginnDt AND KBHP.KBHP_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHH
        ON  KBKP.KBKP_BelegNummer = KBHH.KBKP_BelegNummer
        AND KALE.Datum BETWEEN KBHH.KBHP_TechBeginnDt AND KBHH.KBHP_TechEndeDt
        AND KBHP.KBKP_BelegNummer IS NULL
        AND KBHH.KBHP_VTGKtoPositionNr = 1
    UNION ALL
    SELECT
          KBKP.KBKP_Belegnummer                                                   AS KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos                                            AS KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart                                                      AS DOCO_Belegart
        , KBKP.KBKP_BelegDt                                                       AS KBKP_BelegDt
        , KBKP.KBKP_BuchungDt                                                     AS KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon                                                 AS KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer                                          AS KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer                                             AS KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft                                                 AS DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr                                              AS KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition                                                  AS KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall                                                 AS GEFA_GeschaeftFall
        , KBPO.PART_Partner                                                       AS PART_Partner
        , KBPO.KBPO_KtoFindMerkmal                                                AS KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang                                                  AS DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang                                                   AS DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp                                                      AS DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp                                                     AS DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung                                                      AS DOCO_Waehrung
        , KBPO.DOCO_FormArt                                                       AS DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag                                                  AS KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag        * -1                                        AS KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung                                                    AS KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag          * -1                                        AS KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1       * -1                                        AS KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs                                                AS KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT                                            AS KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand                                                 AS VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer                                                  AS KBPO_VtrKtoNummer
        , KBPO.KBKP_AusgleichBelegnummer                                          AS KBKP_AusgleichBelegnummer
        , KBPO.KBPO_AusgleichStatus                                               AS KBPO_AusgleichStatus
        , KBPO.KBPO_Ausgleichgrund                                                AS KBPO_Ausgleichgrund
        , KBKP.KBKP_BelegDt                                                       AS KBPO_AusgleichDt
        , KBKP.KBKP_BuchungDt                                                     AS KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto                                                     AS KBPO_HBSachkto
        , COALESCE(KBHP.KBHP_SachKto, KBHH.KBHP_SachKto)                          AS KBHP_SachKto
        , COALESCE(KBHP.KBHP_HBAbstimmschluessel, KBHH.KBHP_HBAbstimmschluessel)  AS KBHP_HBAbstimmschluessel
        , KBPO.KBPO_Beschreibung                                                  AS KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd                                                      AS DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt                                                  AS KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung                                                AS KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt                                                     AS DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund                                                  AS DOCO_Buchunggrund
        , CAST('Ausgleichsposition' AS VARCHAR(20))                               AS PositionsArt
        , KALE.Datum                                                              AS Datum
    FROM {kbkp_relation} KBKP
    INNER JOIN {kalender_relation} KALE
        ON  KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
        AND KALE.Datum = CURRENT_DATE
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHP
        ON  KBKP.KBKP_BelegNummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = KBPO.KBPO_VtgKtoPositionNr
        AND KALE.Datum BETWEEN KBHP.KBHP_TechBeginnDt AND KBHP.KBHP_TechEndeDt
    LEFT JOIN {kbhp_relation} KBHH
        ON  KBKP.KBKP_BelegNummer = KBHH.KBKP_BelegNummer
        AND KALE.Datum BETWEEN KBHH.KBHP_TechBeginnDt AND KBHH.KBHP_TechEndeDt
        AND KBHP.KBKP_BelegNummer IS NULL
        AND KBHH.KBHP_VTGKtoPositionNr = 1
    )
SELECT
      UNIO.KBKP_Belegnummer                     AS Belegnummer
    , UNIO.KBPO_VtgKtoWiederholPos              AS Wiederholungsposition
    , UNIO.KBPO_VtgKtoPositionNr                AS Belegposition
    , UNIO.KBPO_Teilposition                    AS Belegteilposition
    , UNIO.DOCO_Belegart                        AS BelegartID
    , UNIO.DOCO_BelegHerkunft                   AS HerkunftID
    , UNIO.KBKP_ErstellungVon                   AS AngelegtVonID
    , UNIO.KBKP_StornoBelegNummer               AS StorniertDurch
    , UNIO.KBKP_StorniertBelegNummer            AS StornobelegZu
    , UNIO.GEFA_GeschaeftFall                   AS GeschaeftsfallID
    , UNIO.DOCO_Hauptvorgang                    AS HauptvorgangID
    , UNIO.PART_Partner                         AS PartnerID
    , UNIO.KBHP_SachKto                         AS SachkontoHBID
    , UNIO.DOCO_Teilvorgang                     AS TeilvorgangID
    , UNIO.DOCO_VtrKtoTyp                       AS VertragskontotypID
    , UNIO.KBKP_ausgleichbelegnummer            AS Ausgleichsbelegnummer
    , UNIO.KBPO_AusgleichDt                     AS Ausgleichsbelegdatum
    , UNIO.KBPO_AusgleichBuchungDt              AS Ausgleichsbuchungsdatum
    , UNIO.KBPO_Ausgleichgrund                  AS AusgleichsgrundID
    , UNIO.KBKP_BelegDt                         AS Belegdatum
    , UNIO.KBKP_BuchungDt                       AS Buchungsdatum
    , UNIO.KBPO_NettofaelligkeitDt              AS Nettofaelligkeitsdatum
    , UNIO.DOCO_SteuerCd                        AS SteuercodeAusFachsystem
    , UNIO.KBPO_ausgleichstatus                 AS Ausgleichsstatus
    , UNIO.KBPO_HbWaehrung                      AS WaehrungHauptbuchID
    , UNIO.KBPO_HbBetrag                        AS BetragHauptbuch
    , CAST('CHF' AS VARCHAR(3))                 AS HauswaehrungID
    , UNIO.KBPO_HWhrBetrag1                     AS BetragHauswaehrung
    , UNIO.KBPO_GesamtBetrag                    AS Gesamtbetrag
    , UNIO.DOCO_Waehrung                        AS TransaktionWaehrung
    , UNIO.KBPO_TWhrBetrag                      AS BetragTransaktionswaehrung
    , UNIO.DOCO_FormArt                         AS Formart
    , UNIO.KBPO_Umrechnungkurs                  AS Umrechnungskurs
    , UNIO.KBKP_ausgleichbelegnummer            AS Ausgleichsbeleg
    , UNIO.VTGP_VtrGegenstand                   AS Vertragsgegenstand
    , UNIO.KBPO_VtrKtoNummer                    AS Vertragskontonummer
    , UNIO.KBHP_HBAbstimmschluessel             AS Abstimmschluessel
    , UNIO.KBPO_Beschreibung                    AS TextZurPosition
    , UNIO.Positionsart                         AS Positionsart
    , UNIO.KBPO_HBSachkto                       AS SachkontoNBID
    , UNIO.KBPO_WertInternDt                    AS Zinsvalutadatum
    , UNIO.KBPO_Bankverbindung                  AS BankverbindungID
    , UNIO.DOCO_RecordArt                       AS RecordArt
    , UNIO.KBPO_KtoFindMerkmal                  AS Kontenfindung
    , UNIO.DOCO_Buchunggrund                    AS BuchungsgrundID
    , UNIO.Datum                                AS TechnischesDatum
FROM UNIO;
""".strip()
    if not quote_source_columns:
        return sql
    return _quote_kostenbelege_3_1_source_columns(sql)


def _build_kostenbelege_3_1_optimized_sql(
    *,
    kbkp_relation: str,
    kbpo_relation: str,
    kbhp_relation: str,
    kalender_relation: str,
) -> str:
    return f"""
WITH current_kalender AS (
    SELECT Datum
    FROM {kalender_relation}
    WHERE Datum = CURRENT_DATE
),
base_positions AS (
    SELECT
          KBKP.KBKP_Belegnummer
        , KBPO.KBPO_VtgKtoWiederholPos
        , KBKP.DOCO_Belegart
        , KBKP.KBKP_BelegDt
        , KBKP.KBKP_BuchungDt
        , KBKP.KBKP_ErstellungVon
        , KBKP.KBKP_StorniertBelegNummer
        , KBKP.KBKP_StornoBelegNummer
        , KBKP.DOCO_BelegHerkunft
        , KBPO.KBPO_VtgKtoPositionNr
        , KBPO.KBPO_Teilposition
        , KBPO.GEFA_GeschaeftFall
        , KBPO.PART_Partner
        , KBPO.KBPO_KtoFindMerkmal
        , KBPO.DOCO_Hauptvorgang
        , KBPO.DOCO_Teilvorgang
        , KBPO.DOCO_Belegtyp
        , KBPO.DOCO_VtrKtoTyp
        , KBPO.DOCO_Waehrung
        , KBPO.DOCO_FormArt
        , KBPO.KBPO_GesamtBetrag
        , KBPO.KBPO_TWhrBetrag
        , KBPO.KBPO_HbWaehrung
        , KBPO.KBPO_HbBetrag
        , KBPO.KBPO_HWhrBetrag1
        , KBPO.KBPO_Umrechnungkurs
        , KBPO.KBPO_NettoFaelligkeitDT
        , KBPO.VTGP_VtrGegenstand
        , KBPO.KBPO_VtrKtoNummer
        , KBPO.KBKP_AusgleichBelegnummer
        , KBPO.KBPO_AusgleichStatus
        , KBPO.KBPO_Ausgleichgrund
        , KBPO.KBPO_AusgleichDt
        , KBPO.KBPO_AusgleichBuchungDt
        , KBPO.KBPO_HBSachkto
        , KBPO.KBPO_Beschreibung
        , KBPO.DOCO_SteuerCd
        , KBPO.KBPO_WertInternDt
        , KBPO.KBPO_Bankverbindung
        , KBPO.DOCO_RecordArt
        , KBKP.DOCO_Buchunggrund
        , KALE.Datum
    FROM {kbkp_relation} KBKP
    INNER JOIN current_kalender KALE
        ON KALE.Datum BETWEEN KBKP.KBKP_TechBeginnDt AND KBKP.KBKP_TechEndeDt
    INNER JOIN {kbpo_relation} KBPO
        ON  KBKP.KBKP_Belegnummer = KBPO.KBKP_AusgleichBelegnummer
        AND KALE.Datum BETWEEN KBPO.KBPO_TechBeginnDt AND KBPO.KBPO_TechEndeDt
),
position_specific AS (
    SELECT
          BP.*
        , KBHP.KBKP_BelegNummer AS KBHP_MatchedBelegNummer
        , KBHP.KBHP_SachKto AS KBHP_MatchedSachKto
        , KBHP.KBHP_HBAbstimmschluessel AS KBHP_MatchedHBAbstimmschluessel
    FROM base_positions BP
    LEFT JOIN {kbhp_relation} KBHP
        ON  BP.KBKP_Belegnummer = KBHP.KBKP_BelegNummer
        AND KBHP.KBHP_VTGKtoPositionNr = BP.KBPO_VtgKtoPositionNr
        AND BP.Datum BETWEEN KBHP.KBHP_TechBeginnDt AND KBHP.KBHP_TechEndeDt
),
resolved_positions AS (
    SELECT
          PS.*
        , PS.KBHP_MatchedSachKto AS KBHP_SachKto
        , PS.KBHP_MatchedHBAbstimmschluessel AS KBHP_HBAbstimmschluessel
    FROM position_specific PS
    WHERE PS.KBHP_MatchedBelegNummer IS NOT NULL
    UNION ALL
    SELECT
          PS.*
        , KBHH.KBHP_SachKto AS KBHP_SachKto
        , KBHH.KBHP_HBAbstimmschluessel AS KBHP_HBAbstimmschluessel
    FROM position_specific PS
    LEFT JOIN {kbhp_relation} KBHH
        ON  PS.KBKP_Belegnummer = KBHH.KBKP_BelegNummer
        AND PS.Datum BETWEEN KBHH.KBHP_TechBeginnDt AND KBHH.KBHP_TechEndeDt
        AND KBHH.KBHP_VTGKtoPositionNr = 1
    WHERE PS.KBHP_MatchedBelegNummer IS NULL
)
SELECT
      RP.KBKP_Belegnummer                     AS Belegnummer
    , RP.KBPO_VtgKtoWiederholPos              AS Wiederholungsposition
    , RP.KBPO_VtgKtoPositionNr                AS Belegposition
    , RP.KBPO_Teilposition                    AS Belegteilposition
    , RP.DOCO_Belegart                        AS BelegartID
    , RP.DOCO_BelegHerkunft                   AS HerkunftID
    , RP.KBKP_ErstellungVon                   AS AngelegtVonID
    , RP.KBKP_StornoBelegNummer               AS StorniertDurch
    , RP.KBKP_StorniertBelegNummer            AS StornobelegZu
    , RP.GEFA_GeschaeftFall                   AS GeschaeftsfallID
    , RP.DOCO_Hauptvorgang                    AS HauptvorgangID
    , RP.PART_Partner                         AS PartnerID
    , RP.KBHP_SachKto                         AS SachkontoHBID
    , RP.DOCO_Teilvorgang                     AS TeilvorgangID
    , RP.DOCO_VtrKtoTyp                       AS VertragskontotypID
    , RP.KBKP_AusgleichBelegnummer            AS Ausgleichsbelegnummer
    , CASE
          WHEN Positions.PositionsArt = 'Originalposition' THEN RP.KBPO_AusgleichDt
          ELSE RP.KBKP_BelegDt
      END                                     AS Ausgleichsbelegdatum
    , CASE
          WHEN Positions.PositionsArt = 'Originalposition' THEN RP.KBPO_AusgleichBuchungDt
          ELSE RP.KBKP_BuchungDt
      END                                     AS Ausgleichsbuchungsdatum
    , RP.KBPO_Ausgleichgrund                  AS AusgleichsgrundID
    , RP.KBKP_BelegDt                         AS Belegdatum
    , RP.KBKP_BuchungDt                       AS Buchungsdatum
    , RP.KBPO_NettoFaelligkeitDT              AS Nettofaelligkeitsdatum
    , RP.DOCO_SteuerCd                        AS SteuercodeAusFachsystem
    , RP.KBPO_AusgleichStatus                 AS Ausgleichsstatus
    , RP.KBPO_HbWaehrung                      AS WaehrungHauptbuchID
    , RP.KBPO_HbBetrag * Positions.AmountSign AS BetragHauptbuch
    , CAST('CHF' AS VARCHAR(3))               AS HauswaehrungID
    , RP.KBPO_HWhrBetrag1 * Positions.AmountSign AS BetragHauswaehrung
    , RP.KBPO_GesamtBetrag                    AS Gesamtbetrag
    , RP.DOCO_Waehrung                        AS TransaktionWaehrung
    , RP.KBPO_TWhrBetrag * Positions.AmountSign AS BetragTransaktionswaehrung
    , RP.DOCO_FormArt                         AS Formart
    , RP.KBPO_Umrechnungkurs                  AS Umrechnungskurs
    , RP.KBKP_AusgleichBelegnummer            AS Ausgleichsbeleg
    , RP.VTGP_VtrGegenstand                   AS Vertragsgegenstand
    , RP.KBPO_VtrKtoNummer                    AS Vertragskontonummer
    , RP.KBHP_HBAbstimmschluessel             AS Abstimmschluessel
    , RP.KBPO_Beschreibung                    AS TextZurPosition
    , Positions.PositionsArt                  AS Positionsart
    , RP.KBPO_HBSachkto                       AS SachkontoNBID
    , RP.KBPO_WertInternDt                    AS Zinsvalutadatum
    , RP.KBPO_Bankverbindung                  AS BankverbindungID
    , RP.DOCO_RecordArt                       AS RecordArt
    , RP.KBPO_KtoFindMerkmal                  AS Kontenfindung
    , RP.DOCO_Buchunggrund                    AS BuchungsgrundID
    , RP.Datum                                AS TechnischesDatum
FROM resolved_positions RP
CROSS JOIN (VALUES
    (CAST('Originalposition' AS VARCHAR(20)), 1),
    (CAST('Ausgleichsposition' AS VARCHAR(20)), -1)
) AS Positions(PositionsArt, AmountSign);
""".strip()


def build_static_notebooks(
    *,
    preferred_s3_relation: str | None,
    preferred_postgres_relation: str | None,
    contest_postgres_relation: str | None,
    contest_s3_relation: str | None,
    contest_postgres_native_relation: str | None,
    multi_table_postgres_relations: dict[str, str | None],
    multi_table_s3_relations: dict[str, str | None],
    multi_table_postgres_native_relations: dict[str, str | None],
    mwa_postgres_relations: dict[str, str | None],
    mwa_postgres_native_relations: dict[str, str | None],
    mwa_s3_parquet_relations: dict[str, str | None],
    mwa_s3_csv_relations: dict[str, str | None],
    mwa_s3_json_relations: dict[str, str | None],
    kostenbelege_3_1_oltp_relations: dict[str, str | None],
    kostenbelege_3_1_olap_relations: dict[str, str | None],
    kostenbelege_3_1_oltp_native_relations: dict[str, str | None],
    kostenbelege_3_1_olap_native_relations: dict[str, str | None],
    kostenbelege_3_1_s3_relations: dict[str, str | None],
    union_oltp_relation: str | None,
    union_olap_relation: str | None,
    union_oltp_s3_relation: str | None,
    union_s3_relation: str | None,
) -> list[NotebookDefinition]:
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
        else "SELECT 'Run the S3 VAT Smoke Loader from the Loader Workbench first.' AS status;"
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
        else "SELECT 'Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.' AS status;"
    )
    pandas_preview_sql = (
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
        "ORDER BY tax_period_end DESC, net_vat_due_chf DESC, filing_id DESC\n"
        "LIMIT 25;"
        if preferred_postgres_relation
        else "SELECT 'Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.' AS status;"
    )
    pandas_python_load_code = (
        "# Load the static VAT smoke table into pandas via the explicit source helper.\n"
        f'vat_df = source("{preferred_postgres_relation}").df()\n'
        'vat_df["tax_period_end"] = pd.to_datetime(vat_df["tax_period_end"])\n'
        'vat_df["net_gap_chf"] = vat_df["output_vat_chf"] - vat_df["input_vat_chf"]\n'
        "vat_df = (\n"
        '    vat_df.loc[vat_df["tax_period_end"] >= "2025-01-01"]\n'
        "    .sort_values([\"tax_period_end\", \"net_vat_due_chf\"], ascending=[False, False])\n"
        "    .reset_index(drop=True)\n"
        ")\n"
        "vat_df.head(12)\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    pandas_python_wrangle_code = (
        "# Reuse the DataFrame from the previous cell and wrangle it with pandas.\n"
        "canton_summary = (\n"
        "    vat_df.assign(\n"
        '        quarter=vat_df["tax_period_end"].dt.to_period("Q").astype(str),\n'
        '        needs_attention=vat_df["audit_flag"] | vat_df["net_vat_due_chf"].gt(15000),\n'
        "    )\n"
        "    .groupby([\"quarter\", \"canton_code\", \"filing_status\"], as_index=False)\n"
        "    .agg(\n"
        '        filing_count=("filing_id", "count"),\n'
        '        net_vat_total_chf=("net_vat_due_chf", "sum"),\n'
        '        average_gap_chf=("net_gap_chf", "mean"),\n'
        '        attention_cases=("needs_attention", "sum"),\n'
        "    )\n"
        "    .sort_values([\"net_vat_total_chf\", \"filing_count\"], ascending=[False, False])\n"
        "    .reset_index(drop=True)\n"
        ")\n"
        "canton_summary.head(15)\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    chart_python_load_code = (
        "# Use the sql(...) helper to pull an aggregated slice into pandas.\n"
        'quarterly_vat = sql("""\n'
        "SELECT\n"
        "  canton_code,\n"
        "  CAST(date_trunc('quarter', tax_period_end) AS DATE) AS tax_quarter_start,\n"
        "  net_vat_due_chf,\n"
        "  filing_status,\n"
        "  audit_flag\n"
        f"FROM {preferred_postgres_relation}\n"
        "WHERE tax_period_end >= DATE '2025-01-01'\n"
        '""")\n'
        'quarterly_vat["tax_quarter_start"] = pd.to_datetime(quarterly_vat["tax_quarter_start"])\n'
        "quarterly_vat = (\n"
        "    quarterly_vat.groupby([\"tax_quarter_start\", \"canton_code\"], as_index=False)\n"
        "    .agg(\n"
        '        net_vat_total_chf=("net_vat_due_chf", "sum"),\n'
        '        filing_count=("net_vat_due_chf", "size"),\n'
        '        audit_cases=("audit_flag", "sum"),\n'
        "    )\n"
        "    .sort_values([\"tax_quarter_start\", \"net_vat_total_chf\"], ascending=[True, False])\n"
        "    .reset_index(drop=True)\n"
        ")\n"
        "quarterly_vat.head(12)\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
    )
    chart_python_plot_code = (
        "import matplotlib.pyplot as plt\n\n"
        "# Reuse the aggregated DataFrame from the previous cell and render a chart.\n"
        "top_cantons = (\n"
        "    quarterly_vat.groupby(\"canton_code\", as_index=False)[\"net_vat_total_chf\"]\n"
        "    .sum()\n"
        "    .sort_values(\"net_vat_total_chf\", ascending=False)\n"
        "    .head(4)[\"canton_code\"]\n"
        "    .tolist()\n"
        ")\n\n"
        "chart_ready = (\n"
        "    quarterly_vat[quarterly_vat[\"canton_code\"].isin(top_cantons)]\n"
        "    .assign(quarter_label=quarterly_vat[quarterly_vat[\"canton_code\"].isin(top_cantons)][\"tax_quarter_start\"].dt.to_period(\"Q\").astype(str))\n"
        "    .pivot(index=\"quarter_label\", columns=\"canton_code\", values=\"net_vat_total_chf\")\n"
        "    .fillna(0)\n"
        "    .sort_index()\n"
        ")\n\n"
        'ax = chart_ready.plot(kind="bar", figsize=(10, 5), rot=0)\n'
        'ax.set_title("Quarterly VAT Total by Canton")\n'
        'ax.set_xlabel("Quarter")\n'
        'ax.set_ylabel("Net VAT due (CHF)")\n'
        'ax.legend(title="Canton", ncols=2)\n'
        "plt.tight_layout()\n"
        "plt.show()\n"
        if preferred_postgres_relation
        else 'print("Run the PostgreSQL OLTP VAT Smoke Loader from the Loader Workbench first.")\n'
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
        else "SELECT 'Run the PG vs S3 Contest Loader from the Loader Workbench first.' AS status;"
    )
    contest_s3_sql = (
        performance_sql_template.format(relation=contest_s3_relation)
        if contest_s3_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Loader Workbench first.' AS status;"
    )
    contest_postgres_native_sql = (
        performance_sql_template.format(relation=contest_postgres_native_relation)
        if contest_postgres_native_relation
        else "SELECT 'Run the PG vs S3 Contest Loader from the Loader Workbench first.' AS status;"
    )
    multi_table_status_sql = (
        "SELECT 'Run the PG vs S3 Multi-Table Federal Tax Loader from the Loader Workbench first.' AS status;"
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
    mwa_status_sql = (
        "SELECT 'Run the MWA Abrechnung Multi-Format Loader (3.2) from the Loader Workbench first.' AS status;"
    )
    mwa_postgres_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_postgres_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_postgres_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_postgres_relations.values())
        else mwa_status_sql
    )
    mwa_postgres_native_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_postgres_native_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_postgres_native_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_postgres_native_relations.values())
        else mwa_status_sql
    )
    mwa_s3_parquet_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_s3_parquet_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_s3_parquet_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_s3_parquet_relations.values())
        else mwa_status_sql
    )
    mwa_s3_csv_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_s3_csv_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_s3_csv_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_s3_csv_relations.values())
        else mwa_status_sql
    )
    mwa_s3_json_sql = (
        _build_mwa_abrechnung_performance_sql(
            abrechnung_relation=mwa_s3_json_relations["mwa_abrechnung_entities"],
            ziffern_relation=mwa_s3_json_relations["mwa_abrechnungs_ziffern_entities"],
        )
        if all(mwa_s3_json_relations.values())
        else mwa_status_sql
    )
    kostenbelege_status_sql = (
        "SELECT 'Run the Kostenbelege Multi-Source Loader (3.1) from the Loader Workbench first.' AS status;"
    )
    kostenbelege_3_1_oltp_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_oltp_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_oltp_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_oltp_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_oltp_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_oltp_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_olap_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_olap_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_olap_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_olap_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_olap_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_olap_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_oltp_native_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_oltp_native_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_oltp_native_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_oltp_native_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_oltp_native_relations["dim_kalender"],
            quote_source_columns=True,
        )
        if all(kostenbelege_3_1_oltp_native_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_olap_native_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_olap_native_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_olap_native_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_olap_native_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_olap_native_relations["dim_kalender"],
            quote_source_columns=True,
        )
        if all(kostenbelege_3_1_olap_native_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_s3_sql = (
        _build_kostenbelege_3_1_sql(
            kbkp_relation=kostenbelege_3_1_s3_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_s3_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_s3_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_s3_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_s3_relations.values())
        else kostenbelege_status_sql
    )
    kostenbelege_3_1_s3_optimized_sql = (
        _build_kostenbelege_3_1_optimized_sql(
            kbkp_relation=kostenbelege_3_1_s3_relations["kbkp_2019"],
            kbpo_relation=kostenbelege_3_1_s3_relations["kbpo_2019"],
            kbhp_relation=kostenbelege_3_1_s3_relations["kbhp_2019"],
            kalender_relation=kostenbelege_3_1_s3_relations["dim_kalender"],
        )
        if all(kostenbelege_3_1_s3_relations.values())
        else kostenbelege_status_sql
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
        else "SELECT 'Run the PostgreSQL OLTP + OLAP UNION Loader from the Loader Workbench first.' AS status;"
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
        else "SELECT 'Run the PostgreSQL OLTP + S3 UNION Loader from the Loader Workbench first.' AS status;"
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
            notebook_id="python-pandas-vat-demo",
            title="Python Pandas VAT Demo",
            summary="Shows how an immutable notebook can combine SQL with Python and pandas on the static PostgreSQL VAT smoke reference table, including persistent state across Python cells.",
            cells=[
                NotebookCellDefinition(
                    cell_id="python-pandas-vat-demo-cell-1",
                    data_sources=["pg_oltp"],
                    sql=pandas_preview_sql,
                ),
                NotebookCellDefinition(
                    cell_id="python-pandas-vat-demo-cell-2",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=pandas_python_load_code,
                ),
                NotebookCellDefinition(
                    cell_id="python-pandas-vat-demo-cell-3",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=pandas_python_wrangle_code,
                ),
            ],
            tags=["python", "pandas", "demo", "postgres"],
            tree_path=("PoC Tests", "General Functionalities"),
            linked_generator_id="postgres_oltp_smoke_orders",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="python-chart-vat-demo",
            title="Python Chart VAT Demo",
            summary="Shows how an immutable notebook can use Python, pandas, and matplotlib to chart the static PostgreSQL VAT smoke reference table inside a headless Jupyter kernel.",
            cells=[
                NotebookCellDefinition(
                    cell_id="python-chart-vat-demo-cell-1",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=chart_python_load_code,
                ),
                NotebookCellDefinition(
                    cell_id="python-chart-vat-demo-cell-2",
                    data_sources=["pg_oltp"],
                    language="python",
                    sql=chart_python_plot_code,
                ),
            ],
            tags=["python", "pandas", "chart", "matplotlib", "demo", "postgres"],
            tree_path=("PoC Tests", "General Functionalities"),
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
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-oltp",
            title="Kostenbelege (3.1) OLTP via DuckDB",
            summary="Runs the Kostenbelege 3.1 union query against generated KBKP, KBPO, KBHP, and calendar tables in PostgreSQL OLTP through DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=kostenbelege_3_1_oltp_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-olap",
            title="Kostenbelege (3.1) OLAP via DuckDB",
            summary="Runs the Kostenbelege 3.1 union query against generated KBKP, KBPO, KBHP, and calendar tables in PostgreSQL OLAP through DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-olap-cell-1",
                    data_sources=["pg_olap"],
                    sql=kostenbelege_3_1_olap_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "olap"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-s3-parquet",
            title="Kostenbelege (3.1) S3 Parquet via DuckDB",
            summary="Runs the Kostenbelege 3.1 union query against generated S3-backed Parquet views for KBKP, KBPO, KBHP, and calendar data.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-s3-parquet-cell-1",
                    data_sources=["workspace.s3"],
                    sql=kostenbelege_3_1_s3_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "s3", "parquet"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-s3-parquet-optimized",
            title="Kostenbelege (3.1) S3 Parquet Optimized via DuckDB",
            summary="Runs the rewritten Kostenbelege 3.1 query against generated S3-backed Parquet views, resolving the KBHP fallback in hash-joinable branches.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-s3-parquet-optimized-cell-1",
                    data_sources=["workspace.s3"],
                    sql=kostenbelege_3_1_s3_optimized_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "s3", "parquet", "optimized"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-oltp-native",
            title="Kostenbelege (3.1) OLTP via Native PostgreSQL",
            summary="Runs the Kostenbelege 3.1 union query directly in PostgreSQL OLTP with quoted source columns for the generated mixed-case schema.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-oltp-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=kostenbelege_3_1_oltp_native_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="kostenbelege-3-1-olap-native",
            title="Kostenbelege (3.1) OLAP via Native PostgreSQL",
            summary="Runs the Kostenbelege 3.1 union query directly in PostgreSQL OLAP with quoted source columns for the generated mixed-case schema.",
            cells=[
                NotebookCellDefinition(
                    cell_id="kostenbelege-3-1-olap-native-cell-1",
                    data_sources=["pg_olap_native"],
                    sql=kostenbelege_3_1_olap_native_sql,
                )
            ],
            tags=["performance", "kostenbelege", "3.1", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "Kostenbelege (3.1)"),
            linked_generator_id="kostenbelege_3_1_multi_source_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-oltp",
            title="MWA Abrechnung (3.2) OLTP via DuckDB",
            summary="Aggregates generated MWA Abrechnung and Abrechnungs-Ziffern records from PostgreSQL OLTP through DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=mwa_postgres_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "postgres", "oltp"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-pg-native",
            title="MWA Abrechnung (3.2) OLTP via Native",
            summary="Runs the same MWA Abrechnung aggregation directly inside PostgreSQL OLTP, without DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=mwa_postgres_native_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "postgres", "native"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-s3-parquet",
            title="MWA Abrechnung (3.2) S3 Parquet via DuckDB",
            summary="Runs the MWA Abrechnung aggregation against the Parquet S3 views, using columnar storage as the primary object-storage benchmark.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-s3-parquet-cell-1",
                    data_sources=["workspace.s3"],
                    sql=mwa_s3_parquet_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "s3", "parquet"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-s3-csv",
            title="MWA Abrechnung (3.2) S3 CSV via DuckDB",
            summary="Runs the MWA Abrechnung aggregation against the CSV S3 views to compare text parsing overhead against Parquet.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-s3-csv-cell-1",
                    data_sources=["workspace.s3"],
                    sql=mwa_s3_csv_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "s3", "csv"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="mwa-abrechnung-s3-json",
            title="MWA Abrechnung (3.2) S3 JSONL via DuckDB",
            summary="Runs the MWA Abrechnung aggregation against the JSONL S3 views to compare row-oriented JSON parsing against Parquet.",
            cells=[
                NotebookCellDefinition(
                    cell_id="mwa-abrechnung-s3-json-cell-1",
                    data_sources=["workspace.s3"],
                    sql=mwa_s3_json_sql,
                )
            ],
            tags=["performance", "mwa", "abrechnung", "s3", "json"],
            tree_path=("PoC Tests", "Performance Evaluation", "MWA Abrechnung (3.2)"),
            linked_generator_id="mwa_abrechnung_multi_format_loader",
            can_edit=False,
            can_delete=False,
        ),
    ]
