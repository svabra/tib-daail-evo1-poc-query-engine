from __future__ import annotations

import json
import re
import time
from collections import Counter
from typing import Any

import duckdb


PLAN_LABELS = {
    "logical_plan": "Logical Plan",
    "logical_opt": "Optimized Plan",
    "physical_plan": "Physical Plan",
}

PLAN_ORDER = ("logical_plan", "logical_opt", "physical_plan")


def generate_duckdb_explain(
    *,
    connection: duckdb.DuckDBPyConnection,
    execution_sql: str,
    display_sql: str,
    notebook_id: str,
    notebook_title: str,
    cell_id: str,
    data_sources: list[str] | None,
    touched_relations: list[str] | None,
    touched_buckets: list[str] | None,
) -> dict[str, object]:
    started = time.perf_counter()
    normalized_sql = str(execution_sql or "").strip()
    if not normalized_sql:
        raise ValueError("The SQL cell is empty.")

    try:
        json_rows = _execute_explain(connection, normalized_sql, "json")
        text_rows = _execute_explain(connection, normalized_sql, "text")
    except duckdb.Error as exc:
        raise ValueError(f"Query plan could not be generated: {exc}") from exc

    json_plans = _json_plan_map(json_rows)
    text_plans = _text_plan_map(text_rows)
    summary = summarize_explain_plans(
        json_plans=json_plans,
        text_plans=text_plans,
        data_sources=data_sources or [],
        touched_relations=touched_relations or [],
        touched_buckets=touched_buckets or [],
    )

    plans = {
        key: {
            "key": key,
            "label": PLAN_LABELS[key],
            "text": text_plans.get(key, ""),
            "json": json_plans.get(key, []),
        }
        for key in PLAN_ORDER
    }

    return {
        "status": "completed",
        "message": "Query plan generated.",
        "durationMs": (time.perf_counter() - started) * 1000,
        "duckdbVersion": duckdb.__version__,
        "notebookId": notebook_id,
        "notebookTitle": notebook_title,
        "cellId": cell_id,
        "sql": display_sql or execution_sql,
        "dataSources": list(data_sources or []),
        "touchedRelations": list(touched_relations or []),
        "touchedBuckets": list(touched_buckets or []),
        "plans": plans,
        "summary": summary,
    }


def summarize_explain_plans(
    *,
    json_plans: dict[str, list[dict[str, Any]]],
    text_plans: dict[str, str],
    data_sources: list[str],
    touched_relations: list[str],
    touched_buckets: list[str],
) -> dict[str, object]:
    physical_nodes = list(_walk_plan_nodes(json_plans.get("physical_plan", [])))
    logical_nodes = list(_walk_plan_nodes(json_plans.get("logical_plan", [])))
    optimized_nodes = list(_walk_plan_nodes(json_plans.get("logical_opt", [])))
    all_text = "\n".join(text_plans.values()).lower()

    operator_counts = Counter(_operator_name(node) for node in physical_nodes)
    operator_counts.pop("", None)
    operator_payload = [
        {"name": name, "count": count}
        for name, count in sorted(operator_counts.items(), key=lambda item: (-item[1], item[0]))
    ]

    estimated_rows = sorted(
        (
            {
                "operator": _operator_name(node),
                "estimatedRows": rows,
            }
            for node in physical_nodes
            for rows in [_estimated_cardinality(node)]
            if rows is not None
        ),
        key=lambda item: int(item["estimatedRows"]),
        reverse=True,
    )

    categories = _operator_categories(operator_counts)
    warnings: list[str] = []
    hints: list[str] = []
    notes: list[str] = []

    max_estimated_rows = int(estimated_rows[0]["estimatedRows"]) if estimated_rows else None
    if max_estimated_rows is not None and max_estimated_rows >= 1_000_000:
        warnings.append(
            f"The plan contains an estimated operator cardinality of {max_estimated_rows:,} rows."
        )

    if any("CROSS_PRODUCT" in name or "CROSS PRODUCT" in name for name in operator_counts):
        warnings.append("The plan indicates a cross product, which is likely expensive on large data.")
    if any("NESTED_LOOP" in name or "NESTED LOOP" in name for name in operator_counts):
        warnings.append("The plan indicates a nested-loop join; verify that the join predicate is selective.")
    if categories["scan"] and not categories["filter"]:
        warnings.append("The physical plan scans source data without an explicit filter operator.")
    if categories["sort"]:
        warnings.append("The plan includes a sort/order operator; large sorts may need memory or temporary space.")

    if categories["join"]:
        hints.append("Join operators are present; check that the build side is smaller when reading large sources.")
    if any("HASH_JOIN" in name or "HASH JOIN" in name for name in operator_counts):
        hints.append("Hash joins are visible in the plan, so memory pressure can grow with join input size.")
    if categories["aggregate"]:
        hints.append("Aggregate operators are present; high group cardinality can dominate runtime.")
    if categories["window"]:
        hints.append("Window operators are present; partition and order columns can strongly affect runtime.")

    relation_text = " ".join([*touched_relations, all_text]).lower()
    if "csv" in relation_text or "read_csv" in all_text:
        warnings.append("The plan appears to read CSV data; parsing text is usually costlier than Parquet.")
    if "json" in relation_text or "read_json" in all_text:
        warnings.append("The plan appears to read JSON/JSONL data; row-oriented parsing can be expensive.")
    if "parquet" in relation_text or "read_parquet" in all_text:
        hints.append("The plan reads Parquet data; column pruning and predicate pushdown may reduce scanned data.")
    if touched_buckets:
        hints.append("S3-backed sources are involved; network and object count can influence runtime.")

    if len(optimized_nodes) != len(logical_nodes):
        notes.append(
            f"DuckDB changed the operator tree from {len(logical_nodes)} logical node(s) "
            f"to {len(optimized_nodes)} optimized node(s)."
        )
    elif optimized_nodes:
        notes.append("The optimized and logical plans have the same major operator count.")
    if categories["projection"]:
        notes.append("Projection operators indicate DuckDB can avoid carrying some unused columns.")
    if categories["filter"]:
        notes.append("Filter operators are visible; review the physical plan to see how close they are to scans.")

    if not warnings:
        warnings.append("No obvious high-risk plan pattern was detected from the non-executing EXPLAIN output.")
    if not hints:
        hints.append("Review the physical plan for the exact operator order before running on large data.")

    return {
        "operatorCounts": operator_payload,
        "operatorCategories": categories,
        "estimatedRows": estimated_rows[:8],
        "maxEstimatedRows": max_estimated_rows,
        "warnings": _unique(warnings),
        "hints": _unique(hints),
        "optimizationNotes": _unique(notes),
        "sources": {
            "dataSources": list(data_sources),
            "relations": list(touched_relations),
            "buckets": list(touched_buckets),
        },
    }


def _execute_explain(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
    output_format: str,
) -> list[tuple[str, str]]:
    connection.execute("PRAGMA explain_output='all'")
    rows = connection.execute(f"EXPLAIN (FORMAT {output_format}) {sql}").fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _json_plan_map(rows: list[tuple[str, str]]) -> dict[str, list[dict[str, Any]]]:
    plans: dict[str, list[dict[str, Any]]] = {}
    for key, value in rows:
        if key not in PLAN_LABELS:
            continue
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = []
        plans[key] = parsed if isinstance(parsed, list) else []
    return plans


def _text_plan_map(rows: list[tuple[str, str]]) -> dict[str, str]:
    return {key: value for key, value in rows if key in PLAN_LABELS}


def _walk_plan_nodes(plan: Any):
    if isinstance(plan, list):
        for item in plan:
            yield from _walk_plan_nodes(item)
        return
    if not isinstance(plan, dict):
        return
    yield plan
    children = plan.get("children")
    if isinstance(children, list):
        for child in children:
            yield from _walk_plan_nodes(child)


def _operator_name(node: dict[str, Any]) -> str:
    return str(node.get("name") or "").strip().upper()


def _operator_extra(node: dict[str, Any]) -> dict[str, Any]:
    extra = node.get("extra_info")
    if isinstance(extra, dict):
        return extra
    return {
        str(key): value
        for key, value in node.items()
        if key not in {"name", "children"} and value is not None
    }


def _estimated_cardinality(node: dict[str, Any]) -> int | None:
    extra = _operator_extra(node)
    value = extra.get("Estimated Cardinality")
    if value is None:
        return None
    match = re.search(r"\d[\d,]*", str(value))
    if not match:
        return None
    return int(match.group(0).replace(",", ""))


def _operator_categories(operator_counts: Counter[str]) -> dict[str, int]:
    names = list(operator_counts)
    return {
        "scan": sum(count for name, count in operator_counts.items() if "SCAN" in name or "READ_" in name),
        "filter": sum(count for name, count in operator_counts.items() if "FILTER" in name),
        "join": sum(count for name, count in operator_counts.items() if "JOIN" in name or "CROSS_PRODUCT" in name),
        "aggregate": sum(count for name, count in operator_counts.items() if "AGGREGATE" in name or "GROUP_BY" in name),
        "sort": sum(count for name, count in operator_counts.items() if "ORDER" in name or "SORT" in name or "TOP_N" in name),
        "window": sum(count for name, count in operator_counts.items() if "WINDOW" in name),
        "limit": sum(count for name, count in operator_counts.items() if "LIMIT" in name or "TOP_N" in name),
        "projection": sum(count for name, count in operator_counts.items() if "PROJECTION" in name),
        "cte": sum(count for name, count in operator_counts.items() if "CTE" in name),
        "total": sum(operator_counts.get(name, 0) for name in names),
    }


def _unique(values: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result
