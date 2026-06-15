from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
import re

from ..models import NotebookDefinition


PipelineGraphProvider = Callable[[NotebookDefinition], dict[str, object]]


BAD_STAGE_STATUSES = {"failed", "cancelled", "aborted", "incomplete", "invalid"}
WARNING_STAGE_STATUSES = {"obsolete", "warning"}
ACTIVE_STAGE_STATUSES = {"running", "queued"}
COMPLETED_STAGE_STATUSES = {"materialized", "valid", "completed"}


@dataclass(slots=True)
class _GraphSummary:
    notebook: NotebookDefinition
    graph: dict[str, object]
    stage_count: int
    source_count: int
    output_count: int
    published_count: int
    warning_count: int
    error_count: int
    active: bool
    completed: bool
    latest_run_label: str


def build_home_data_flows(
    *,
    notebooks: Iterable[NotebookDefinition],
    data_products: Iterable[dict[str, object]],
    graph_provider: PipelineGraphProvider,
    max_items: int = 8,
) -> list[dict[str, object]]:
    """Build compact, homepage-safe product lineage summaries.

    The home page needs provenance and status, not raw SQL or full graph payloads.
    This function keeps notebook internals collapsed into lifecycle nodes.
    """

    normalized_products = [_normalized_product(product) for product in data_products]
    normalized_products = [product for product in normalized_products if product.get("productId")]

    pipeline_summaries: list[_GraphSummary] = []
    for notebook in notebooks:
        if not _is_pipeline_notebook(notebook):
            continue
        try:
            graph = graph_provider(notebook)
        except Exception as exc:  # pragma: no cover - defensive home-page fallback
            graph = {
                "nodes": [],
                "sourceNodes": [],
                "diagnostics": [
                    {
                        "severity": "warning",
                        "message": f"Could not summarize pipeline graph: {exc}",
                    }
                ],
            }
        pipeline_summaries.append(_summarize_graph(notebook, graph))

    linked_product_flows: dict[str, dict[str, object]] = {}
    for summary in pipeline_summaries:
        for node in _graph_nodes(summary.graph):
            for product in _node_products(node):
                normalized_product = _normalized_product(product)
                product_id = str(normalized_product.get("productId") or "").strip()
                if not product_id or product_id in linked_product_flows:
                    continue
                linked_product_flows[product_id] = _flow_from_pipeline_product(
                    summary=summary,
                    product=normalized_product,
                    published_node=node,
                )

    flows: list[dict[str, object]] = []
    seen_product_ids: set[str] = set()
    for product in normalized_products:
        product_id = str(product.get("productId") or "").strip()
        if not product_id or product_id in seen_product_ids:
            continue
        seen_product_ids.add(product_id)
        flows.append(
            linked_product_flows.get(product_id)
            or _flow_from_product_without_pipeline(product)
        )

    for product_id, flow in linked_product_flows.items():
        if product_id in seen_product_ids:
            continue
        seen_product_ids.add(product_id)
        flows.append(flow)

    for summary in pipeline_summaries:
        if any(_node_products(node) for node in _graph_nodes(summary.graph)):
            continue
        flows.append(_flow_from_pipeline(summary))

    if not flows:
        flows.append(_empty_flow())

    normalized_limit = max(1, int(max_items or 8))
    return flows[:normalized_limit]


def _is_pipeline_notebook(notebook: NotebookDefinition) -> bool:
    if str(notebook.pipeline_mode or "").strip().lower() == "pipeline":
        return True
    return any((cell.stage or {}).get("stageId") for cell in notebook.cells)


def _summarize_graph(
    notebook: NotebookDefinition,
    graph: dict[str, object],
) -> _GraphSummary:
    nodes = _graph_nodes(graph)
    diagnostics = [
        item
        for item in graph.get("diagnostics", []) or []
        if isinstance(item, dict)
    ]
    active_runs = [
        item for item in graph.get("activeRuns", []) or [] if isinstance(item, dict)
    ]
    statuses = {str(node.get("status") or "").strip().lower() for node in nodes}
    warning_count = sum(
        1
        for item in diagnostics
        if str(item.get("severity") or "").strip().lower() == "warning"
    )
    warning_count += sum(1 for node in nodes if str(node.get("runWarning") or "").strip())
    warning_count += sum(1 for status in statuses if status in WARNING_STAGE_STATUSES)
    error_count = sum(
        1
        for item in diagnostics
        if str(item.get("severity") or "").strip().lower() == "error"
    )
    error_count += sum(1 for status in statuses if status in BAD_STAGE_STATUSES)
    active = bool(active_runs) or any(status in ACTIVE_STAGE_STATUSES for status in statuses)
    completed_nodes = [
        node
        for node in nodes
        if str(node.get("status") or "").strip().lower() in COMPLETED_STAGE_STATUSES
        or node.get("latestRevision")
    ]
    completed = bool(nodes) and len(completed_nodes) == len(nodes) and not error_count
    output_count = sum(1 for node in nodes if isinstance(node.get("outputSource"), dict) and node.get("outputSource"))
    published_count = sum(len(_node_products(node)) for node in nodes)
    latest_run_label = _latest_run_label(nodes, active=active, completed=completed, error_count=error_count)
    return _GraphSummary(
        notebook=notebook,
        graph=graph,
        stage_count=len(nodes),
        source_count=len(graph.get("sourceNodes", []) or []),
        output_count=output_count,
        published_count=published_count,
        warning_count=warning_count,
        error_count=error_count,
        active=active,
        completed=completed,
        latest_run_label=latest_run_label,
    )


def _flow_from_pipeline_product(
    *,
    summary: _GraphSummary,
    product: dict[str, object],
    published_node: dict[str, object],
) -> dict[str, object]:
    title = str(product.get("title") or "").strip() or summary.notebook.title
    output_label = (
        str(published_node.get("title") or "").strip()
        or str((published_node.get("outputSource") or {}).get("sourceDisplayName") or "").strip()
        or "Shared Workspace Parquet"
    )
    flow = _base_flow(
        flow_id=f"product:{product.get('productId')}",
        kind="dataProduct",
        title=title,
        subtitle=f"Published from {summary.notebook.title}",
        summary=summary,
        product=product,
        output_label=output_label,
        published=True,
    )
    return flow


def _flow_from_product_without_pipeline(product: dict[str, object]) -> dict[str, object]:
    source_label = (
        str(product.get("sourceDisplayName") or "").strip()
        or str(product.get("relation") or "").strip()
        or str(product.get("key") or "").strip()
        or str(product.get("bucket") or "").strip()
        or "Published source"
    )
    status = {
        "label": "Published",
        "tone": "success",
        "message": "Published data product is available.",
    }
    return {
        "flowId": f"product:{product.get('productId')}",
        "kind": "dataProduct",
        "title": str(product.get("title") or "").strip() or source_label,
        "subtitle": f"Published from {source_label}",
        "status": status,
        "badges": [
            {"label": "Published endpoint", "detail": "Published catalog", "tone": "success"},
            {"label": "Source linked", "detail": source_label, "tone": "neutral"},
        ],
        "nodes": [
            _node("csv-upload", "CSV upload", "User upload", "csv", "neutral", "/ingestion-workbench", 1, 1),
            _node("csv-validation", "CSV validation", "Schema & rules", "validation", "success", "/ingestion-workbench", 1, 2),
            _node("s3-landing", "S3 landing", "Parquet files", "s3", "neutral", "/data-sources?source_id=workspace.s3", 2, 1),
            _node("postgres-import", "PostgreSQL import", "Raw tables", "postgres", "neutral", "/data-sources?source_id=pg_oltp", 2, 2),
            _node("loader", "Loader / normalization", "CSV import & normalization", "loader", "neutral", "/loader-workbench", 3, 1, span=2),
            _node("sql-stages", "SQL transformation stages", "No linked pipeline yet", "transform", "neutral", "/query-workbench", 4, 1, span=2),
            _node("materialized-output", "Published source", source_label, "output", "success", _product_documentation_path(product), 5, 1, span=2),
            _node("published-catalog", "Published catalog", "Read-only endpoint", "catalog", "success", _product_documentation_path(product), 6, 1),
            _node("individuals", "Individuals", "Self-service access", "individual", "success", _product_public_path(product), 6, 2),
            _node("business-innovator", "Microsoft Business Innovator", "Analytics consumption", "analytics", "success", _product_public_path(product), 6, 3),
        ],
        "links": _default_links(),
        "actions": _product_actions(product),
    }


def _flow_from_pipeline(summary: _GraphSummary) -> dict[str, object]:
    output_label = "Shared Workspace Parquet"
    output_nodes = [
        node
        for node in _graph_nodes(summary.graph)
        if isinstance(node.get("outputSource"), dict) and node.get("outputSource")
    ]
    if output_nodes:
        output_label = (
            str(output_nodes[-1].get("title") or "").strip()
            or str((output_nodes[-1].get("outputSource") or {}).get("sourceDisplayName") or "").strip()
            or output_label
        )
    title = _pipeline_display_title(summary.notebook.title, output_label)
    return _base_flow(
        flow_id=f"pipeline:{summary.notebook.notebook_id}",
        kind="pipeline",
        title=title,
        subtitle="Pipeline output lineage",
        summary=summary,
        product=None,
        output_label=output_label,
        published=False,
    )


def _base_flow(
    *,
    flow_id: str,
    kind: str,
    title: str,
    subtitle: str,
    summary: _GraphSummary,
    product: dict[str, object] | None,
    output_label: str,
    published: bool,
) -> dict[str, object]:
    status = _flow_status(summary=summary, published=published)
    loader_label = _loader_label(summary.notebook.linked_generator_id, summary.notebook.title)
    stage_detail = (
        f"{summary.stage_count} SQL stages"
        if summary.stage_count != 1
        else "1 SQL stage"
    )
    output_tone = "success" if summary.output_count else "neutral"
    publication_tone = "success" if published else "warning"
    consumer_detail = "Self-service access" if published else "Pending access"
    innovator_detail = "Analytics consumption" if published else "Pending publication"
    consumer_href = _product_public_path(product or {}) if published else "/data-products"
    product_href = _product_documentation_path(product or {}) if published else "/data-products"
    warning_tone = "warning" if summary.warning_count else "neutral"
    return {
        "flowId": flow_id,
        "kind": kind,
        "title": title,
        "subtitle": subtitle,
        "notebookId": summary.notebook.notebook_id,
        "status": status,
        "badges": _badges(summary=summary, published=published),
        "nodes": [
            _node("csv-upload", "CSV upload", "User upload", "csv", "neutral", "/ingestion-workbench", 1, 1),
            _node("csv-validation", "CSV validation", "Schema & rules", "validation", warning_tone, "/ingestion-workbench", 1, 2),
            _node("s3-landing", "S3 landing", "Parquet files", "s3", "neutral", "/data-sources?source_id=workspace.s3", 2, 1),
            _node("postgres-import", "PostgreSQL import", "Raw tables", "postgres", "neutral", "/data-sources?source_id=pg_oltp", 2, 2),
            _node("loader", loader_label, "CSV import & normalization", "loader", "neutral", "/loader-workbench", 3, 1, span=2),
            _node("sql-stages", "SQL transformation stages", stage_detail, "transform", status["tone"], _notebook_path(summary.notebook.notebook_id), 4, 1, span=2),
            _node("materialized-output", "Shared Workspace Parquet", output_label, "output", output_tone, "/data-sources?source_id=workspace.s3", 5, 1, span=2),
            _node("published-catalog", "Published catalog", "Read-only endpoint" if published else "Not published yet", "catalog", publication_tone, product_href, 6, 1),
            _node("individuals", "Individuals", consumer_detail, "individual", publication_tone, consumer_href, 6, 2),
            _node("business-innovator", "Microsoft Business Innovator", innovator_detail, "analytics", publication_tone, consumer_href, 6, 3),
        ],
        "links": _default_links(),
        "actions": _product_actions(product or {}, notebook_id=summary.notebook.notebook_id, published=published),
    }


def _flow_status(
    *,
    summary: _GraphSummary,
    published: bool,
) -> dict[str, str]:
    if summary.active:
        return {
            "label": "Running",
            "tone": "running",
            "message": "Pipeline activity is currently in progress.",
        }
    if summary.error_count:
        return {
            "label": "Needs attention",
            "tone": "error",
            "message": "At least one pipeline stage has an error.",
        }
    if summary.warning_count:
        return {
            "label": "Warnings",
            "tone": "warning",
            "message": "Lineage is available with warnings.",
        }
    if summary.completed and published:
        return {
            "label": "Published and fresh",
            "tone": "success",
            "message": "Latest materialized output is published.",
        }
    if summary.completed:
        return {
            "label": "Materialized",
            "tone": "success",
            "message": "Pipeline has a materialized output.",
        }
    return {
        "label": "Planned",
        "tone": "neutral",
        "message": "Pipeline is available but has no completed materialization yet.",
    }


def _badges(
    *,
    summary: _GraphSummary,
    published: bool,
) -> list[dict[str, str]]:
    badges = [
        {
            "label": summary.latest_run_label,
            "detail": "Pipeline status",
            "tone": "success" if summary.completed else ("running" if summary.active else "neutral"),
        },
        {
            "label": "Fresh" if summary.completed else "Freshness pending",
            "detail": "Updated recently" if summary.completed else "Awaiting materialization",
            "tone": "success" if summary.completed else "neutral",
        },
        {"label": "2 landing targets", "detail": "S3, PostgreSQL", "tone": "running"},
        {
            "label": f"{summary.stage_count} stages" if summary.stage_count != 1 else "1 stage",
            "detail": f"{summary.warning_count} warning" if summary.warning_count == 1 else (f"{summary.warning_count} warnings" if summary.warning_count else "SQL pipeline"),
            "tone": "warning" if summary.warning_count else "running",
        },
    ]
    if published:
        badges.append({"label": "1 published endpoint", "detail": "Published catalog", "tone": "success"})
    else:
        badges.append({"label": "Publication pending", "detail": "Publish to catalog", "tone": "warning"})
    if summary.warning_count:
        badges.append({"label": f"{summary.warning_count} warning(s)", "detail": "Review stages", "tone": "warning"})
    if summary.error_count:
        badges.append({"label": f"{summary.error_count} error(s)", "detail": "Needs attention", "tone": "error"})
    return badges


def _node(
    node_id: str,
    title: str,
    detail: str,
    icon: str,
    tone: str,
    href: str,
    column: int,
    row: int,
    *,
    span: int = 1,
) -> dict[str, object]:
    return {
        "nodeId": node_id,
        "title": title,
        "detail": detail,
        "icon": icon,
        "tone": tone,
        "href": href,
        "column": column,
        "row": row,
        "span": span,
    }


def _default_links() -> list[dict[str, str]]:
    return [
        {"from": "csv-upload", "to": "s3-landing"},
        {"from": "csv-validation", "to": "s3-landing"},
        {"from": "csv-validation", "to": "postgres-import"},
        {"from": "s3-landing", "to": "loader"},
        {"from": "postgres-import", "to": "loader"},
        {"from": "loader", "to": "sql-stages"},
        {"from": "sql-stages", "to": "materialized-output"},
        {"from": "materialized-output", "to": "published-catalog"},
        {"from": "materialized-output", "to": "individuals"},
        {"from": "materialized-output", "to": "business-innovator"},
    ]


def _product_actions(
    product: dict[str, object],
    *,
    notebook_id: str = "",
    published: bool = True,
) -> list[dict[str, str]]:
    actions: list[dict[str, str]] = []
    if notebook_id:
        actions.append({"label": "Open pipeline", "href": _notebook_path(notebook_id), "kind": "pipeline"})
    if published:
        actions.append({"label": "Open data product", "href": _product_documentation_path(product), "kind": "product"})
        actions.append({"label": "Open endpoint", "href": _product_public_path(product), "kind": "endpoint"})
    else:
        actions.append({"label": "Publish product", "href": "/data-products", "kind": "product"})
    return actions


def _latest_run_label(
    nodes: list[dict[str, object]],
    *,
    active: bool,
    completed: bool,
    error_count: int,
) -> str:
    if active:
        return "Latest run active"
    if error_count:
        return "Latest run failed"
    if completed:
        return "Latest run completed"
    if any(node.get("latestRun") for node in nodes):
        return "Latest run incomplete"
    return "No run yet"


def _graph_nodes(graph: dict[str, object]) -> list[dict[str, object]]:
    return [node for node in graph.get("nodes", []) or [] if isinstance(node, dict)]


def _node_products(node: dict[str, object]) -> list[dict[str, object]]:
    return [
        product
        for product in node.get("publishedDataProducts", []) or []
        if isinstance(product, dict)
    ]


def _normalized_product(product: dict[str, object]) -> dict[str, object]:
    return dict(product or {})


def _notebook_path(notebook_id: str) -> str:
    return f"/notebooks/{str(notebook_id or '').strip()}"


def _product_documentation_path(product: dict[str, object]) -> str:
    path = str(product.get("documentationPath") or "").strip()
    if path:
        return path
    slug = str(product.get("slug") or "").strip()
    return f"/dataproducts/{slug}" if slug else "/data-products"


def _product_public_path(product: dict[str, object]) -> str:
    path = str(product.get("publicPath") or "").strip()
    if path:
        return path
    slug = str(product.get("slug") or "").strip()
    return f"/api/public/data-products/{slug}" if slug else "/data-products"


def _loader_label(generator_id: str, notebook_title: str) -> str:
    normalized = str(generator_id or "").strip()
    known = {
        "kostenbelege_3_1_multi_source_loader": "Kostenbelege loader",
        "mwa_abrechnung_multi_format_loader": "MWA Abrechnung loader",
        "parquet_performance_options_loader": "Parquet options loader",
    }
    if normalized in known:
        return known[normalized]
    if normalized:
        label = re.sub(r"[_-]+", " ", normalized).strip()
        return label[:1].upper() + label[1:]
    title = str(notebook_title or "").strip()
    return title or "Loader"


def _pipeline_display_title(notebook_title: str, output_label: str) -> str:
    normalized_title = str(notebook_title or "").strip()
    normalized_output = str(output_label or "").strip()
    combined = f"{normalized_title} {normalized_output}".lower()
    if "kostenbelege" in combined and "settlement" in combined:
        return "Kostenbelege Settlement Audit"
    if "mwa" in combined and "abrechnung" in combined:
        return "MWA Abrechnung Lineage"
    return normalized_title or normalized_output or "Data product lineage"


def _empty_flow() -> dict[str, object]:
    return {
        "flowId": "empty",
        "kind": "empty",
        "title": "No data product lineage yet",
        "subtitle": "Publish a data product or materialize a pipeline to populate this view.",
        "status": {
            "label": "Empty",
            "tone": "neutral",
            "message": "No published data product or pipeline lineage is available yet.",
        },
        "badges": [{"label": "Waiting for lineage", "tone": "neutral"}],
        "nodes": [
            _node("csv-upload", "CSV upload", "User upload", "csv", "neutral", "/ingestion-workbench", 1, 1),
            _node("csv-validation", "CSV validation", "Schema & rules", "validation", "neutral", "/ingestion-workbench", 1, 2),
            _node("s3-landing", "S3 landing", "Parquet files", "s3", "neutral", "/data-sources?source_id=workspace.s3", 2, 1),
            _node("postgres-import", "PostgreSQL import", "Raw tables", "postgres", "neutral", "/data-sources?source_id=pg_oltp", 2, 2),
            _node("loader", "Loader / normalization", "Run a loader", "loader", "neutral", "/loader-workbench", 3, 1, span=2),
            _node("sql-stages", "SQL transformation stages", "Build a pipeline", "transform", "neutral", "/query-workbench", 4, 1, span=2),
            _node("materialized-output", "Shared Workspace Parquet", "Materialize output", "output", "neutral", "/data-sources?source_id=workspace.s3", 5, 1, span=2),
            _node("published-catalog", "Published catalog", "Publish product", "catalog", "neutral", "/data-products", 6, 1),
            _node("individuals", "Individuals", "Self-service access", "individual", "neutral", "/dataproducts/", 6, 2),
            _node("business-innovator", "Microsoft Business Innovator", "Analytics consumption", "analytics", "neutral", "/dataproducts/", 6, 3),
        ],
        "links": _default_links(),
        "actions": [{"label": "Open Data Products", "href": "/data-products", "kind": "product"}],
    }
