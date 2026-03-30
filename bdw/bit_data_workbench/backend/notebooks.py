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
        object_names=("generated_orders_pg_vs_s3",),
    )
    contest_s3_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=("generated_orders_pg_vs_s3",),
    )
    contest_postgres_native_relation = _strip_catalog_prefix(contest_postgres_relation, "pg_oltp")

    s3_sql = (
        "SELECT\n"
        "  vat_id,\n"
        "  canton_code,\n"
        "  category\n"
        f"FROM {preferred_s3_relation}\n"
        "ORDER BY vat_id\n"
        "LIMIT 100;"
        if preferred_s3_relation
        else "SELECT 'Load the S3 VAT smoke data from the Ingestion Workbench first.' AS status;"
    )

    postgres_sql = (
        "SELECT\n"
        "  vat_id,\n"
        "  canton_code,\n"
        "  category,\n"
        "  effective_from\n"
        f"FROM {preferred_postgres_relation}\n"
        "ORDER BY vat_id\n"
        "LIMIT 100;"
        if preferred_postgres_relation
        else "SELECT 'Load the PostgreSQL OLTP VAT smoke data from the Ingestion Workbench first.' AS status;"
    )
    performance_sql_template = (
        "SELECT\n"
        "  canton_code,\n"
        "  product_category,\n"
        "  COUNT(*) AS order_count,\n"
        "  CAST(ROUND(SUM(amount_chf), 2) AS DECIMAL(18,2)) AS total_amount_chf,\n"
        "  CAST(ROUND(AVG(quantity), 2) AS DOUBLE PRECISION) AS avg_quantity\n"
        "FROM {relation}\n"
        "WHERE order_date >= DATE '2025-01-01'\n"
        "  AND priority_flag = true\n"
        "GROUP BY canton_code, product_category\n"
        "ORDER BY total_amount_chf DESC, order_count DESC\n"
        "LIMIT 25;"
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

    return [
        NotebookDefinition(
            notebook_id="s3-smoke-test",
            title="S3 Smoke Test",
            summary="Reads the preconfigured smoke-test object through DuckDB.",
            cells=[
                NotebookCellDefinition(
                    cell_id="s3-smoke-test-cell-1",
                    data_sources=["workspace.s3"],
                    sql=s3_sql,
                )
            ],
            tags=["smoke", "s3"],
            tree_path=("Smoke Tests", "Object Storage"),
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="postgres-smoke-test",
            title="PostgreSQL Smoke Test",
            summary="Queries the OLTP reference table through the attached PostgreSQL database.",
            cells=[
                NotebookCellDefinition(
                    cell_id="postgres-smoke-test-cell-1",
                    data_sources=["pg_oltp"],
                    sql=postgres_sql,
                )
            ],
            tags=["smoke", "postgres"],
            tree_path=("Smoke Tests", "Relational"),
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-oltp",
            title="PG vs S3 Contest OLTP",
            summary="Runs the contest benchmark query against PostgreSQL OLTP using the mirrored contest dataset.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-oltp-cell-1",
                    data_sources=["pg_oltp"],
                    sql=contest_postgres_sql,
                )
            ],
            tags=["performance", "contest", "oltp"],
            tree_path=("Performance Evaluation",),
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-s3",
            title="PG vs S3 Contest S3",
            summary="Runs the same contest benchmark query against the mirrored S3-backed dataset.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-s3-cell-1",
                    data_sources=["workspace.s3"],
                    sql=contest_s3_sql,
                )
            ],
            tags=["performance", "contest", "s3"],
            tree_path=("Performance Evaluation",),
            can_edit=False,
            can_delete=False,
        ),
        NotebookDefinition(
            notebook_id="pg-vs-s3-contest-pg-native",
            title="PG vs S3 Contest PostgreSQL Native",
            summary="Runs the same contest benchmark query directly on PostgreSQL OLTP, without DuckDB in the execution path.",
            cells=[
                NotebookCellDefinition(
                    cell_id="pg-vs-s3-contest-pg-native-cell-1",
                    data_sources=["pg_oltp_native"],
                    sql=contest_postgres_native_sql,
                )
            ],
            tags=["performance", "contest", "postgres", "native"],
            tree_path=("Performance Evaluation",),
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
    protected_roots = {"Smoke Tests", "Performance Evaluation"}

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
