from __future__ import annotations

from dataclasses import replace
from typing import Iterable

from ..models import (
    LinkedNotebookReference,
    NotebookDefinition,
    NotebookFolder,
    SourceObject,
    SourceCatalog,
)
from ..data_generator.result_set_storage_sample import (
    RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME,
)
from .notebook_presets import (
    build_kostenbelege_3_1_problem_solving_notebook,
    build_kostenbelege_3_1_s3_parquet_pipeline_notebook,
    build_mwa_s3_parquet_pipeline_notebook,
    build_result_set_storage_s3_demo_notebook,
    build_static_notebooks,
)
from .s3_storage import parse_s3_url, s3_bucket_schema_name
from .source_references import (
    parse_source_reference,
    pg_source_reference,
    s3_source_reference,
    s3_table_function_sql,
)
KOSTENBELEGE_3_1_GENERATED_BUCKET = (
    "poc-tests-performance-evaluation-kostenbelege-3-1"
)
KOSTENBELEGE_3_1_GENERATED_DATASET = "kostenbelege_3_1"
KOSTENBELEGE_3_1_LOADER_SCHEMA = "s3_3_1_imports_a08e7385"
KOSTENBELEGE_3_1_OBJECT_NAMES = (
    "kbkp_2019",
    "kbpo_2019",
    "kbhp_2019",
    "dim_kalender",
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
                return schema.objects[0].query_reference or schema.objects[0].relation
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
                    return source_object.query_reference or source_object.relation
    return None


def _find_generated_s3_relation_by_object_name(
    catalogs: Iterable[SourceCatalog],
    *,
    object_names: Iterable[str],
    executable_only: bool = False,
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
                executable_relation = _s3_object_executable_relation(source_object)
                relation = (
                    executable_relation
                    or (
                        None
                        if executable_only
                        else source_object.query_reference or source_object.relation
                    )
                )
                if fallback_relation is None:
                    fallback_relation = relation
                normalized_key = str(source_object.s3_key or "").strip().lower()
                if normalized_key.startswith("generated/"):
                    return relation
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
                relations[preferred_name] = source_object.query_reference or source_object.relation
    return relations


def _s3_object_executable_relation(source_object: SourceObject) -> str | None:
    query_sql = str(getattr(source_object, "query_sql", "") or "").strip().rstrip(";")
    if query_sql:
        normalized_query_sql = query_sql.lower()
        if normalized_query_sql.startswith("select * from "):
            return query_sql[len("SELECT * FROM ") :].strip()
        if normalized_query_sql.startswith(("select", "with", "values")):
            return f"({query_sql})"
        return query_sql

    query_reference = str(source_object.query_reference or "").strip()
    if query_reference.startswith(("s3.", "s3.")):
        return query_reference

    bucket = str(source_object.s3_bucket or "").strip()
    key = str(source_object.s3_key or "").strip()
    if (not bucket or not key) and source_object.s3_path:
        try:
            bucket, key = parse_s3_url(str(source_object.s3_path))
        except ValueError:
            bucket, key = "", ""
    if bucket and key:
        return s3_table_function_sql(
            bucket=bucket,
            key=key,
            file_format=str(source_object.s3_file_format or "parquet"),
        )
    return None


def _resolve_kostenbelege_3_1_s3_relations(
    catalogs: Iterable[SourceCatalog],
    *,
    object_names: Iterable[str],
) -> dict[str, str | None]:
    requested_names = tuple(object_names)
    generated_relations = _find_kostenbelege_3_1_generated_s3_relations(
        catalogs,
        object_names=requested_names,
    )
    if all(generated_relations.values()):
        return generated_relations

    loader_relations = _find_executable_s3_relations_by_object_names(
        catalogs,
        schema_name=KOSTENBELEGE_3_1_LOADER_SCHEMA,
        object_names=requested_names,
    )
    if all(loader_relations.values()):
        return {
            object_name: kostenbelege_3_1_loader_virtual_relation(object_name)
            for object_name in requested_names
        }

    return generated_relations


def _find_executable_s3_relations_by_object_names(
    catalogs: Iterable[SourceCatalog],
    *,
    schema_name: str | None = None,
    object_names: Iterable[str],
) -> dict[str, str | None]:
    requested_names = tuple(object_names)
    preferred = {name.lower(): name for name in requested_names}
    relations: dict[str, str | None] = {name: None for name in requested_names}
    for catalog in catalogs:
        if catalog.name != "workspace":
            continue
        for schema in catalog.schemas:
            if schema_name is not None and schema.name != schema_name:
                continue
            for source_object in schema.objects:
                preferred_name = preferred.get(source_object.name.lower())
                if preferred_name is None or relations[preferred_name] is not None:
                    continue
                relations[preferred_name] = _s3_object_executable_relation(source_object)
    return relations


def _workspace_schema_exists(
    catalogs: Iterable[SourceCatalog],
    *,
    schema_name: str,
) -> bool:
    for catalog in catalogs:
        if catalog.name != "workspace":
            continue
        if any(schema.name == schema_name for schema in catalog.schemas):
            return True
    return False


def kostenbelege_3_1_generated_parquet_key(table_name: str) -> str:
    return (
        f"generated/{KOSTENBELEGE_3_1_GENERATED_DATASET}/"
        f"parquet/{table_name}/*.parquet"
    )


def _kostenbelege_3_1_generated_parquet_scan(table_name: str) -> str:
    return s3_table_function_sql(
        bucket=KOSTENBELEGE_3_1_GENERATED_BUCKET,
        key=kostenbelege_3_1_generated_parquet_key(table_name),
        file_format="parquet",
        hive_partitioning=False,
    )


def kostenbelege_3_1_generated_parquet_scan(table_name: str) -> str:
    return _kostenbelege_3_1_generated_parquet_scan(table_name)


def kostenbelege_3_1_loader_virtual_relation(table_name: str) -> str:
    return s3_source_reference(
        bucket=KOSTENBELEGE_3_1_GENERATED_BUCKET,
        key=kostenbelege_3_1_generated_parquet_key(table_name),
    )


def kostenbelege_3_1_loader_query_sql(table_name: str) -> str:
    scan_sql = _kostenbelege_3_1_generated_parquet_scan(table_name)
    if str(table_name or "").strip().lower() != "kbpo_2019":
        return scan_sql
    return (
        "(SELECT *, "
        '"KBKP_AusgleichBelegnummer" AS "KBKP_Belegnummer" '
        f"FROM {scan_sql})"
    )


def _find_kostenbelege_3_1_generated_s3_relations(
    catalogs: Iterable[SourceCatalog],
    *,
    object_names: Iterable[str],
) -> dict[str, str | None]:
    requested_names = tuple(object_names)
    generated_bucket_schema_name = s3_bucket_schema_name(
        KOSTENBELEGE_3_1_GENERATED_BUCKET
    )
    discovered_names = {
        object_name: False
        for object_name in requested_names
    }
    for catalog in catalogs:
        if catalog.name != "workspace":
            continue
        for schema in catalog.schemas:
            if schema.name != generated_bucket_schema_name:
                continue
            schema_object_names = {
                source_object.name.lower()
                for source_object in schema.objects
            }
            for object_name in requested_names:
                if (
                    object_name.lower() in schema_object_names
                    or f"{object_name}_parquet".lower() in schema_object_names
                ):
                    discovered_names[object_name] = True

    if not all(discovered_names.values()):
        return {object_name: None for object_name in requested_names}

    return {
        object_name: kostenbelege_3_1_loader_virtual_relation(object_name)
        for object_name in requested_names
    }


def _strip_catalog_prefix(
    relation: str | None, catalog_name: str
) -> str | None:
    normalized_relation = (relation or "").strip()
    prefix = f"{catalog_name}."
    if not normalized_relation:
        return None
    source_reference = parse_source_reference(normalized_relation)
    if (
        source_reference is not None
        and source_reference.root == "pg"
        and source_reference.container == catalog_name
    ):
        return source_reference.object_name
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
    kostenbelege_3_1_object_names = (
        "kbkp_2019",
        "kbpo_2019",
        "kbhp_2019",
        "dim_kalender",
    )
    parquet_performance_option_object_names = (
        "federal_tax_parquet_off",
        "federal_tax_parquet_recommended",
        "federal_tax_parquet_manual_partition",
        "federal_tax_parquet_manual_hive",
        "federal_tax_parquet_manual_cache",
    )
    preferred_s3_relation = _find_generated_s3_relation_by_object_name(
        catalogs,
        object_names=("vat_smoke",),
    )
    preferred_postgres_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=("vat_filing_smoke_generated",),
    )
    preferred_postgres_olap_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="pg_olap",
        schema_name="public",
        object_names=("tax_assessment_olap_smoke",),
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
    parquet_performance_option_relations = _find_relations_by_object_names(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=parquet_performance_option_object_names,
    )
    result_set_storage_source_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=(RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME,),
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
    kostenbelege_3_1_oltp_relations = _find_relations_by_object_names(
        catalogs,
        catalog_name="pg_oltp",
        schema_name="public",
        object_names=kostenbelege_3_1_object_names,
    )
    kostenbelege_3_1_olap_relations = _find_relations_by_object_names(
        catalogs,
        catalog_name="pg_olap",
        schema_name="public",
        object_names=kostenbelege_3_1_object_names,
    )
    kostenbelege_3_1_s3_relations = _resolve_kostenbelege_3_1_s3_relations(
        catalogs,
        object_names=kostenbelege_3_1_object_names,
    )
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
    kostenbelege_3_1_oltp_native_relations = {
        object_name: _strip_catalog_prefix(relation, "pg_oltp")
        for object_name, relation in kostenbelege_3_1_oltp_relations.items()
    }
    kostenbelege_3_1_olap_native_relations = {
        object_name: _strip_catalog_prefix(relation, "pg_olap")
        for object_name, relation in kostenbelege_3_1_olap_relations.items()
    }

    notebooks = build_static_notebooks(
        preferred_s3_relation=preferred_s3_relation,
        preferred_postgres_relation=preferred_postgres_relation,
        preferred_postgres_olap_relation=preferred_postgres_olap_relation,
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
        kostenbelege_3_1_oltp_relations=kostenbelege_3_1_oltp_relations,
        kostenbelege_3_1_olap_relations=kostenbelege_3_1_olap_relations,
        kostenbelege_3_1_oltp_native_relations=kostenbelege_3_1_oltp_native_relations,
        kostenbelege_3_1_olap_native_relations=kostenbelege_3_1_olap_native_relations,
        kostenbelege_3_1_s3_relations=kostenbelege_3_1_s3_relations,
        union_oltp_relation=union_oltp_relation,
        union_olap_relation=union_olap_relation,
        union_oltp_s3_relation=union_oltp_s3_relation,
        union_s3_relation=union_s3_relation,
        parquet_performance_option_relations=parquet_performance_option_relations,
        result_set_storage_source_relation=result_set_storage_source_relation,
    )
    for notebook in notebooks:
        if not notebook.can_edit:
            notebook.shared = True
    return notebooks


def build_restart_seeded_shared_notebooks(
    catalogs: list[SourceCatalog],
) -> list[NotebookDefinition]:
    mwa_object_names = (
        "mwa_abrechnung_entities",
        "mwa_abrechnungs_ziffern_entities",
    )
    kostenbelege_3_1_object_names = (
        "kbkp_2019",
        "kbpo_2019",
        "kbhp_2019",
        "dim_kalender",
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
    kostenbelege_3_1_s3_relations = _resolve_kostenbelege_3_1_s3_relations(
        catalogs,
        object_names=kostenbelege_3_1_object_names,
    )
    result_set_storage_source_relation = _find_relation_by_object_name(
        catalogs,
        catalog_name="workspace",
        schema_name=None,
        object_names=(RESULT_SET_STORAGE_SAMPLE_SOURCE_NAME,),
    )
    return [
        build_mwa_s3_parquet_pipeline_notebook(
            mwa_s3_parquet_relations=mwa_s3_parquet_relations,
        ),
        build_kostenbelege_3_1_s3_parquet_pipeline_notebook(
            kostenbelege_3_1_s3_relations=kostenbelege_3_1_s3_relations,
        ),
        build_kostenbelege_3_1_problem_solving_notebook(
            kostenbelege_3_1_s3_relations=kostenbelege_3_1_s3_relations,
        ),
        build_result_set_storage_s3_demo_notebook(
            result_set_storage_source_relation=result_set_storage_source_relation,
        ),
    ]


def _notebook_terminal_stage_ids(notebook: NotebookDefinition) -> set[str]:
    stage_ids: set[str] = set()
    successors: dict[str, set[str]] = {}
    for cell in notebook.cells:
        stage = cell.stage if isinstance(cell.stage, dict) else {}
        stage_id = str(stage.get("stageId") or "").strip()
        if not stage_id:
            continue
        stage_ids.add(stage_id)
        successors.setdefault(stage_id, set())

    for cell in notebook.cells:
        stage = cell.stage if isinstance(cell.stage, dict) else {}
        stage_id = str(stage.get("stageId") or "").strip()
        if not stage_id:
            continue
        for predecessor_id in stage.get("predecessorStageIds", []) or []:
            normalized_predecessor_id = str(predecessor_id or "").strip()
            if normalized_predecessor_id in stage_ids:
                successors.setdefault(normalized_predecessor_id, set()).add(stage_id)

    return {
        stage_id
        for stage_id in stage_ids
        if not [successor_id for successor_id in successors.get(stage_id, set()) if successor_id in stage_ids]
    }


def _preserved_seed_pipeline_paths(
    seed: NotebookDefinition,
    existing: NotebookDefinition | None,
) -> list[dict[str, object]]:
    if existing is None or not existing.pipeline_paths:
        return []

    terminal_stage_ids = _notebook_terminal_stage_ids(seed)
    if not terminal_stage_ids:
        return []

    def priority_sort_key(item: object) -> int:
        if not isinstance(item, dict):
            return 1_000_000
        try:
            return int(item.get("priority") or item.get("rank") or 1_000_000)
        except (TypeError, ValueError):
            return 1_000_000

    preserved: list[dict[str, object]] = []
    seen: set[str] = set()
    ordered_paths = sorted(
        existing.pipeline_paths,
        key=priority_sort_key,
    )
    for path in ordered_paths:
        if not isinstance(path, dict):
            continue
        terminal_stage_id = str(
            path.get("terminalStageId")
            or path.get("terminal_stage_id")
            or ""
        ).strip()
        path_id = str(path.get("pathId") or path.get("path_id") or "").strip()
        if not terminal_stage_id and path_id.startswith("path-"):
            terminal_stage_id = path_id[5:]
        if terminal_stage_id not in terminal_stage_ids or terminal_stage_id in seen:
            continue
        seen.add(terminal_stage_id)
        preserved.append(
            {
                "pathId": path_id or f"path-{terminal_stage_id}",
                "terminalStageId": terminal_stage_id,
                "label": str(path.get("label") or path.get("name") or "").strip(),
                "priority": len(preserved) + 1,
            }
        )
    return preserved


def merge_restart_seeded_shared_notebook(
    seed: NotebookDefinition,
    existing: NotebookDefinition | None,
) -> NotebookDefinition:
    return replace(
        seed,
        pipeline_paths=_preserved_seed_pipeline_paths(seed, existing),
    )


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


def _s3_source_option_id(source_object: SourceObject) -> str:
    query_reference = str(source_object.query_reference or "").strip()
    if query_reference.startswith("s3."):
        return query_reference
    bucket = str(source_object.s3_bucket or "").strip()
    key = str(source_object.s3_key or "").strip()
    if bucket and key:
        return s3_source_reference(bucket=bucket, key=key)
    return ""


def build_source_options(catalogs: list[SourceCatalog]) -> list[dict[str, str]]:
    options: list[dict[str, str]] = []
    seen_s3_options: set[str] = set()

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
            for schema in catalog.schemas:
                for source_object in schema.objects:
                    source_id = _s3_source_option_id(source_object)
                    if not source_id or source_id in seen_s3_options:
                        continue
                    seen_s3_options.add(source_id)
                    label = str(
                        source_object.display_name
                        or source_object.name
                        or source_id
                    ).strip()
                    bucket = str(source_object.s3_bucket or schema.label or schema.name or "").strip()
                    key = str(source_object.s3_key or "").strip()
                    location = f"s3://{bucket}/{key}" if bucket and key else source_id
                    options.append(
                        _source_option(
                            source_id,
                            label,
                            classification="Workspace Storage",
                            storage_tooltip=f"Stored in S3 Object Storage at {location}.",
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

    _add_source_reference_completion_lists(schema, catalogs)
    return schema


def _completion_namespace_from_list(items: list[object]) -> dict[str, object]:
    namespace: dict[str, object] = {}
    for item in items:
        label = str(item or "").strip()
        if label:
            namespace.setdefault(label, [])
    return namespace


def _ensure_completion_namespace(
    parent: dict[str, object],
    label: str,
) -> dict[str, object]:
    child = parent.get(label)
    if isinstance(child, dict):
        return child

    namespace: dict[str, object]
    if isinstance(child, list):
        namespace = _completion_namespace_from_list(child)
    else:
        namespace = {}

    parent[label] = namespace
    return namespace


def _add_s3_query_alias_path(
    schema: dict[str, object],
    query_alias: str,
) -> None:
    parts = [
        part.strip()
        for part in str(query_alias or "").split(".")
        if part.strip()
    ]
    if len(parts) < 4 or parts[0] != "s3":
        return

    node = _ensure_completion_namespace(schema, "s3")
    for part in parts[1:-1]:
        node = _ensure_completion_namespace(node, part)

    node.setdefault(parts[-1], [])


def _add_s3_query_alias_completion_paths(
    schema: dict[str, object],
    catalogs: list[SourceCatalog],
) -> None:
    aliases: set[str] = set()
    for catalog in catalogs:
        if catalog.name != "workspace":
            continue
        for source_schema in catalog.schemas:
            for source_object in source_schema.objects:
                query_alias = str(source_object.query_alias or "").strip()
                if query_alias.startswith("s3."):
                    aliases.add(query_alias)

    for query_alias in sorted(aliases):
        _add_s3_query_alias_path(schema, query_alias)


def _add_source_reference_completion_lists(
    schema: dict[str, object],
    catalogs: list[SourceCatalog],
) -> None:
    s3_references: dict[str, dict[str, object]] = {}
    pg_references: dict[str, dict[str, object]] = {}
    for catalog in catalogs:
        for source_schema in catalog.schemas:
            for source_object in source_schema.objects:
                if catalog.name == "workspace":
                    query_reference = str(source_object.query_reference or "").strip()
                    if not query_reference and source_object.s3_bucket and source_object.s3_key:
                        query_reference = s3_source_reference(
                            bucket=source_object.s3_bucket,
                            key=source_object.s3_key,
                        )
                    if query_reference.startswith("s3."):
                        s3_references.setdefault(
                            query_reference,
                            {
                                "label": query_reference,
                                "detail": str(source_object.s3_file_format or "S3 object"),
                                "bucket": str(source_object.s3_bucket or ""),
                                "object": str(source_object.s3_key or ""),
                                "relation": str(source_object.relation or ""),
                            },
                        )
                    continue
                if catalog.name in {"pg_oltp", "pg_olap"}:
                    query_reference = str(source_object.query_reference or "").strip()
                    if not query_reference:
                        query_reference = pg_source_reference(
                            source_id=catalog.name,
                            relation=source_object.relation,
                        )
                    if query_reference.startswith("pg."):
                        pg_references.setdefault(
                            query_reference,
                            {
                                "label": query_reference,
                                "detail": "PostgreSQL relation",
                                "connection": catalog.name,
                                "relation": str(source_object.relation or ""),
                            },
                        )
    if s3_references:
        schema["s3References"] = [
            s3_references[key] for key in sorted(s3_references, key=str.lower)
        ]
    if pg_references:
        schema["pgReferences"] = [
            pg_references[key] for key in sorted(pg_references, key=str.lower)
        ]


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
