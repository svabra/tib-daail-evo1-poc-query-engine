from __future__ import annotations

from typing import Iterable

from ..models import (
    LinkedNotebookReference,
    NotebookDefinition,
    NotebookFolder,
    SourceCatalog,
)
from .notebook_presets import build_static_notebooks


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


def _strip_catalog_prefix(
    relation: str | None, catalog_name: str
) -> str | None:
    normalized_relation = (relation or "").strip()
    prefix = f"{catalog_name}."
    if not normalized_relation:
        return None
    if normalized_relation.startswith(prefix):
        return normalized_relation[len(prefix):]
    return normalized_relation


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
    mwa_object_names = (
        "mwa_abrechnung_entities",
        "mwa_abrechnungs_ziffern_entities",
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
    mwa_postgres_relations = _find_relations_by_object_names(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=mwa_object_names,
    )
    mwa_s3_parquet_relations = {
        object_name: _find_relation_by_object_name(
            catalogs,
            catalog_name="workspace",
            schema_name=None,
            object_names=(f"{object_name}_parquet",),
        )
        for object_name in mwa_object_names
    }
    mwa_s3_csv_relations = {
        object_name: _find_relation_by_object_name(
            catalogs,
            catalog_name="workspace",
            schema_name=None,
            object_names=(f"{object_name}_csv",),
        )
        for object_name in mwa_object_names
    }
    mwa_s3_json_relations = {
        object_name: _find_relation_by_object_name(
            catalogs,
            catalog_name="workspace",
            schema_name=None,
            object_names=(f"{object_name}_json",),
        )
        for object_name in mwa_object_names
    }
    contest_postgres_native_relation = _strip_catalog_prefix(
        contest_postgres_relation, "pg_oltp"
    )
    multi_table_postgres_native_relations = {
        object_name: _strip_catalog_prefix(relation, "pg_oltp")
        for object_name, relation in multi_table_postgres_relations.items()
    }
    mwa_postgres_native_relations = {
        object_name: _strip_catalog_prefix(relation, "pg_oltp")
        for object_name, relation in mwa_postgres_relations.items()
    }

    notebooks = build_static_notebooks(
        preferred_s3_relation=preferred_s3_relation,
        preferred_postgres_relation=preferred_postgres_relation,
        contest_postgres_relation=contest_postgres_relation,
        contest_s3_relation=contest_s3_relation,
        contest_postgres_native_relation=contest_postgres_native_relation,
        multi_table_postgres_relations=multi_table_postgres_relations,
        multi_table_s3_relations=multi_table_s3_relations,
        multi_table_postgres_native_relations=(
            multi_table_postgres_native_relations
        ),
        mwa_postgres_relations=mwa_postgres_relations,
        mwa_postgres_native_relations=mwa_postgres_native_relations,
        mwa_s3_parquet_relations=mwa_s3_parquet_relations,
        mwa_s3_csv_relations=mwa_s3_csv_relations,
        mwa_s3_json_relations=mwa_s3_json_relations,
        union_oltp_relation=union_oltp_relation,
        union_olap_relation=union_olap_relation,
        union_oltp_s3_relation=union_oltp_s3_relation,
        union_s3_relation=union_s3_relation,
    )
    for notebook in notebooks:
        if not notebook.can_edit:
            notebook.shared = True
    return notebooks


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


def _folder_id_for_path(path_key: tuple[str, ...]) -> str:
    return "-".join(part.lower().replace(" ", "-") for part in path_key)


def _folder_path_from_metadata(folder: object) -> tuple[str, ...]:
    path = getattr(folder, "path", ())
    if not isinstance(path, (list, tuple)):
        return ()
    return tuple(str(segment).strip() for segment in path if str(segment).strip())


def _folder_metadata_by_path(folder_metadata: Iterable[object]) -> dict[tuple[str, ...], object]:
    metadata_by_path: dict[tuple[str, ...], object] = {}
    for folder in folder_metadata:
        path = _folder_path_from_metadata(folder)
        if path:
            metadata_by_path[path] = folder
    return metadata_by_path


def build_notebook_tree(
    notebooks: list[NotebookDefinition],
    *,
    folder_metadata: Iterable[object] = (),
) -> list[NotebookFolder]:
    roots: list[NotebookFolder] = []
    folder_index: dict[tuple[str, ...], NotebookFolder] = {}
    protected_roots = {"PoC Tests"}
    metadata_by_path = _folder_metadata_by_path(folder_metadata)

    def ensure_folder_path(path: Iterable[str]) -> NotebookFolder | None:
        path_key: tuple[str, ...] = ()
        parent_folder: NotebookFolder | None = None

        for segment in path:
            path_key = (*path_key, segment)
            folder = folder_index.get(path_key)
            folder_metadata_entry = metadata_by_path.get(path_key)
            if folder is None:
                is_protected_path = bool(path_key) and path_key[0] in protected_roots
                folder = NotebookFolder(
                    folder_id=_folder_id_for_path(path_key),
                    name=str(getattr(folder_metadata_entry, "name", "") or segment),
                    can_edit=(
                        False
                        if is_protected_path
                        else bool(getattr(folder_metadata_entry, "can_edit", True))
                    ),
                    can_delete=(
                        False
                        if is_protected_path
                        else bool(getattr(folder_metadata_entry, "can_delete", True))
                    ),
                    is_shared=(
                        True
                        if is_protected_path
                        else bool(getattr(folder_metadata_entry, "is_public", False))
                    ),
                )
                folder_index[path_key] = folder
                if parent_folder is None:
                    roots.append(folder)
                else:
                    parent_folder.folders.append(folder)
            elif folder_metadata_entry is not None:
                is_protected_path = bool(path_key) and path_key[0] in protected_roots
                folder.name = str(getattr(folder_metadata_entry, "name", "") or folder.name)
                folder.can_edit = (
                    False
                    if is_protected_path
                    else bool(getattr(folder_metadata_entry, "can_edit", folder.can_edit))
                )
                folder.can_delete = (
                    False
                    if is_protected_path
                    else bool(getattr(folder_metadata_entry, "can_delete", folder.can_delete))
                )
                folder.is_shared = (
                    True
                    if is_protected_path
                    else bool(getattr(folder_metadata_entry, "is_public", folder.is_shared))
                )
            parent_folder = folder

        return parent_folder

    for folder_path in sorted(metadata_by_path, key=lambda value: (len(value), value)):
        ensure_folder_path(folder_path)

    for notebook in notebooks:
        if not notebook.tree_path:
            continue

        parent_folder = ensure_folder_path(notebook.tree_path)
        if parent_folder is not None:
            parent_folder.notebooks.append(notebook)

    return roots
