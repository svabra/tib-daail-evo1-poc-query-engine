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


def build_notebooks(catalogs: list[SourceCatalog]) -> list[NotebookDefinition]:
    s3_relation = _find_relation(catalogs, catalog_name="workspace", schema_name="s3")
    postgres_relation = _find_relation(catalogs, catalog_name="pg_oltp", schema_name="public")

    s3_sql = (
        "SELECT\n"
        "  vat_id,\n"
        "  canton_code,\n"
        "  category\n"
        f"FROM {s3_relation}\n"
        "ORDER BY vat_id\n"
        "LIMIT 100;"
        if s3_relation
        else "SELECT 'Configure S3_STARTUP_VIEWS to expose a smoke-test object.' AS status;"
    )

    postgres_sql = (
        "SELECT\n"
        "  vat_id,\n"
        "  canton_code,\n"
        "  category,\n"
        "  effective_from\n"
        f"FROM {postgres_relation}\n"
        "ORDER BY vat_id\n"
        "LIMIT 100;"
        if postgres_relation
        else "SELECT 'Configure PG_* variables to attach PostgreSQL OLTP.' AS status;"
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
            if any(schema.name == "s3" and schema.objects for schema in catalog.schemas):
                options.append(_source_option("workspace.s3", "MinIO / S3"))
            elif catalog.schemas:
                options.append(_source_option("workspace", "Workspace"))
            continue

        if catalog.name == "pg_oltp":
            options.append(_source_option("pg_oltp", "PostgreSQL OLTP"))
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

    for notebook in notebooks:
        if not notebook.tree_path:
            continue

        path_key: tuple[str, ...] = ()
        parent_folder: NotebookFolder | None = None

        for segment in notebook.tree_path:
            path_key = (*path_key, segment)
            folder = folder_index.get(path_key)
            if folder is None:
                is_protected_path = bool(path_key) and path_key[0] == "Smoke Tests"
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
