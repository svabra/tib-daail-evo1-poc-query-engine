from __future__ import annotations

import argparse
import json
import math
import shutil
import sys
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
BDW_ROOT = REPO_ROOT / "bdw"
SCRIPTS_ROOT = REPO_ROOT / "scripts"
for path in (BDW_ROOT, SCRIPTS_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from bit_data_workbench.backend.pure_duckdb import (  # noqa: E402
    FACT_BUPO_TARGET,
    KBHP_FULL_PATH,
    KBKP_FULL_PATH,
    KBKP_TODAY_TARGET,
    KALENDER_PATH,
    KBPO_PATHS,
)
from bit_data_workbench.backend.pure_duckdb_benchmark_variants import (  # noqa: E402
    PureDuckDBBenchmarkVariant,
    fact_scan_sql,
    pure_duckdb_q1_q2_benchmark_variants,
    pure_duckdb_q1_q2_comparison_columns,
)
from bit_data_workbench.backend.sql_utils import sql_literal  # noqa: E402
from pure_duckdb_big_data_benchmark import (  # noqa: E402
    benchmark_s3_url,
    generate_files,
    local_path_for_s3,
    s3_client,
    s3_parts,
    upload_files,
)


TERMINAL_WIDTH = 120
NUMERIC_CONSISTENCY_TOLERANCE = 0.0001
EXPLANATORY_COMPARISON_COLUMNS = (
    "change_summary",
    "sql_strategy",
    "output_layout",
    "duckdb_settings",
    "expected_effect",
    "consistency_details",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark Pure DuckDB Query 1 and Query 2 optimization variants and "
            "validate every candidate against the current baseline."
        )
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd() / ".tmp" / "pure-duckdb-q1-q2-benchmark",
        help="Local working directory for generated Parquet files and benchmark outputs.",
    )
    parser.add_argument(
        "--target-compressed-mib",
        type=float,
        default=20.0,
        help="Minimum compressed size for each generated KBPO input file.",
    )
    parser.add_argument("--rows-per-kbpo-file", type=int, default=0)
    parser.add_argument("--dimension-rows", type=int, default=50000)
    parser.add_argument("--seed", type=int, default=31031)
    parser.add_argument("--max-tuning-passes", type=int, default=8)
    parser.add_argument("--skip-generate", action="store_true")
    parser.add_argument("--use-s3", action="store_true", help="Upload inputs and run variants against S3 URLs.")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--local-compatible-s3-names", action="store_true")
    parser.add_argument("--s3-endpoint-url", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key-id", default="minioadmin")
    parser.add_argument("--s3-secret-access-key", default="minioadmin")
    parser.add_argument("--s3-region", default="us-east-1")
    parser.add_argument("--s3-url-style", default="path")
    parser.add_argument("--s3-use-ssl", action="store_true")
    parser.add_argument(
        "--variant",
        action="append",
        default=[],
        help="Run only this variant id. May be passed multiple times.",
    )
    parser.add_argument(
        "--query",
        action="append",
        choices=["1", "2"],
        default=[],
        help="Run only Q1 or Q2 variants. May be passed multiple times.",
    )
    parser.add_argument(
        "--rerun-fastest",
        type=int,
        default=2,
        help="Rerun each top passing non-baseline candidate this many extra times.",
    )
    parser.add_argument(
        "--rerun-top-candidates",
        type=int,
        default=2,
        help="Number of fastest passing non-baseline candidates per query to rerun.",
    )
    parser.add_argument(
        "--markdown-output",
        type=Path,
        default=None,
        help="Optional path for the Markdown comparison table.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path for the full JSON benchmark summary.",
    )
    parser.add_argument("--no-json", action="store_true", help="Do not print the JSON summary to stdout.")
    parser.add_argument("--quiet", action="store_true")
    return parser.parse_args()


def _s3_urls_for_variants(variants: list[PureDuckDBBenchmarkVariant]) -> list[str]:
    urls = [KBKP_FULL_PATH, KBHP_FULL_PATH, KALENDER_PATH, FACT_BUPO_TARGET, KBKP_TODAY_TARGET]
    urls.extend(KBPO_PATHS)
    urls.extend(variant.output_s3_url for variant in variants if variant.output_s3_url)
    return sorted({url for url in urls if url})


def _local_sql(args: argparse.Namespace, sql: str, variants: list[PureDuckDBBenchmarkVariant]) -> str:
    rewritten = sql
    for s3_url in _s3_urls_for_variants(variants):
        local_path = local_path_for_s3(Path(args.output_dir), s3_url).as_posix()
        if s3_url.endswith("/"):
            local_path = f"{local_path}/"
        rewritten = rewritten.replace(s3_url, local_path)
    return rewritten


def _s3_sql(args: argparse.Namespace, sql: str, variants: list[PureDuckDBBenchmarkVariant]) -> str:
    rewritten = sql
    for s3_url in _s3_urls_for_variants(variants):
        rewritten = rewritten.replace(s3_url, benchmark_s3_url(args, s3_url))
    return rewritten


def runtime_sql(args: argparse.Namespace, sql: str, variants: list[PureDuckDBBenchmarkVariant]) -> str:
    return _s3_sql(args, sql, variants) if args.use_s3 else _local_sql(args, sql, variants)


def _configure_s3_connection(connection: duckdb.DuckDBPyConnection, args: argparse.Namespace) -> None:
    endpoint = str(args.s3_endpoint_url or "").strip()
    if endpoint.startswith("http://"):
        endpoint = endpoint.removeprefix("http://")
        use_ssl = False
    elif endpoint.startswith("https://"):
        endpoint = endpoint.removeprefix("https://")
        use_ssl = True
    else:
        use_ssl = bool(args.s3_use_ssl)
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    connection.execute(f"SET s3_endpoint = {sql_literal(endpoint)}")
    connection.execute(f"SET s3_access_key_id = {sql_literal(str(args.s3_access_key_id))}")
    connection.execute(f"SET s3_secret_access_key = {sql_literal(str(args.s3_secret_access_key))}")
    connection.execute(f"SET s3_region = {sql_literal(str(args.s3_region))}")
    connection.execute(f"SET s3_url_style = {sql_literal(str(args.s3_url_style))}")
    connection.execute(f"SET s3_use_ssl = {'true' if use_ssl else 'false'}")


def _remove_local_output(args: argparse.Namespace, s3_url: str) -> None:
    path = local_path_for_s3(Path(args.output_dir), s3_url)
    if s3_url.rstrip().endswith("/"):
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink()
    path.parent.mkdir(parents=True, exist_ok=True)


def _delete_s3_output(args: argparse.Namespace, s3_url: str) -> None:
    bucket, key = s3_parts(benchmark_s3_url(args, s3_url))
    client = s3_client(args)
    if s3_url.rstrip().endswith("/"):
        paginator = client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            objects = [{"Key": item["Key"]} for item in page.get("Contents", [])]
            if objects:
                client.delete_objects(Bucket=bucket, Delete={"Objects": objects})
    else:
        try:
            client.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass


def _cleanup_variant_output(args: argparse.Namespace, variant: PureDuckDBBenchmarkVariant) -> None:
    if not variant.output_s3_url:
        return
    if args.use_s3:
        _delete_s3_output(args, variant.output_s3_url)
    else:
        _remove_local_output(args, variant.output_s3_url)


def _fetch_one(connection: duckdb.DuckDBPyConnection, sql: str) -> tuple[Any, ...]:
    row = connection.execute(sql).fetchone()
    return tuple(row or ())


def _decimal_close(left: Any, right: Any) -> bool:
    if left is None or right is None:
        return left is None and right is None
    if isinstance(left, Decimal) or isinstance(right, Decimal):
        return abs(Decimal(str(left)) - Decimal(str(right))) <= Decimal(
            str(NUMERIC_CONSISTENCY_TOLERANCE)
        )
    return math.isclose(
        float(left),
        float(right),
        rel_tol=1e-9,
        abs_tol=NUMERIC_CONSISTENCY_TOLERANCE,
    )


def _fingerprint_equal(left: Any, right: Any) -> bool:
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(
            _fingerprint_equal(left_value, right_value)
            for left_value, right_value in zip(left, right, strict=True)
        )
    if isinstance(left, (int, float, Decimal)) or isinstance(right, (int, float, Decimal)):
        return _decimal_close(left, right)
    return left == right


def _q1_consistency(result: tuple[Any, ...], baseline: tuple[Any, ...] | None) -> tuple[str, str]:
    if baseline is None:
        return "baseline", f"baseline cnt={result[0] if result else None}, total={result[1] if len(result) > 1 else None}"
    if len(result) < 2 or len(baseline) < 2:
        return "fail", f"expected two aggregate columns, got result={result}, baseline={baseline}"
    if result[0] != baseline[0]:
        return "fail", f"count differs: result={result[0]}, baseline={baseline[0]}"
    if not _decimal_close(result[1], baseline[1]):
        return "fail", f"sum differs: result={result[1]}, baseline={baseline[1]}"
    return "pass", f"count and sum match baseline: cnt={result[0]}, total={result[1]}"


def _q2_profile(connection: duckdb.DuckDBPyConnection, scan_sql: str) -> dict[str, Any]:
    describe_rows = connection.execute(f"DESCRIBE SELECT * FROM ({scan_sql}) q").fetchall()
    schema = [(row[0], row[1]) for row in describe_rows]
    totals = connection.execute(
        f"""
        SELECT
              COUNT(*) AS row_count
            , ROUND(SUM(BetragHauswaehrung), 6) AS sum_betrag_hauswaehrung
            , ROUND(SUM(BetragHauptbuch), 6) AS sum_betrag_hauptbuch
            , ROUND(SUM(BetragTransaktionswaehrung), 6) AS sum_betrag_transaktionswaehrung
        FROM ({scan_sql}) q
        """
    ).fetchone()
    grouped = connection.execute(
        f"""
        SELECT
              COALESCE(Positionsart, '') AS Positionsart
            , COALESCE(BelegartID, '') AS BelegartID
            , COALESCE(WaehrungHauptbuchID, '') AS WaehrungHauptbuchID
            , COUNT(*) AS row_count
            , ROUND(SUM(BetragHauswaehrung), 6) AS sum_betrag_hauswaehrung
            , ROUND(SUM(BetragHauptbuch), 6) AS sum_betrag_hauptbuch
            , ROUND(SUM(BetragTransaktionswaehrung), 6) AS sum_betrag_transaktionswaehrung
        FROM ({scan_sql}) q
        GROUP BY
              COALESCE(Positionsart, '')
            , COALESCE(BelegartID, '')
            , COALESCE(WaehrungHauptbuchID, '')
        ORDER BY 1, 2, 3
        """
    ).fetchall()
    return {
        "schema": schema,
        "totals": tuple(totals or ()),
        "grouped": [tuple(row) for row in grouped],
    }


def _q2_consistency(profile: dict[str, Any], baseline: dict[str, Any] | None) -> tuple[str, str]:
    row_count = profile.get("totals", (None,))[0]
    if baseline is None:
        return "baseline", f"baseline rows={row_count}, groups={len(profile.get('grouped', []))}"
    if profile.get("schema") != baseline.get("schema"):
        return "fail", "schema differs from baseline"
    if not _fingerprint_equal(profile.get("totals"), baseline.get("totals")):
        return "fail", f"totals differ: result={profile.get('totals')}, baseline={baseline.get('totals')}"
    if not _fingerprint_equal(profile.get("grouped"), baseline.get("grouped")):
        return "fail", "grouped fingerprint differs from baseline"
    return "pass", f"schema, totals, and grouped fingerprints match baseline; rows={row_count}"


def _execute_variant(
    args: argparse.Namespace,
    variant: PureDuckDBBenchmarkVariant,
    variants: list[PureDuckDBBenchmarkVariant],
) -> dict[str, Any]:
    _cleanup_variant_output(args, variant)
    started = time.perf_counter()
    connection = duckdb.connect(":memory:")
    try:
        if args.use_s3:
            _configure_s3_connection(connection, args)
        for setting in variant.duckdb_settings:
            connection.execute(setting)
        final_result: tuple[Any, ...] | None = None
        for index, statement in enumerate(variant.statements):
            cursor = connection.execute(runtime_sql(args, statement, variants))
            if variant.query_number == 1 and index == len(variant.statements) - 1:
                final_result = tuple(cursor.fetchone() or ())
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        if variant.query_number == 1:
            validation_payload: Any = final_result or _fetch_one(
                connection,
                runtime_sql(args, variant.validation_sql, variants),
            )
        else:
            validation_payload = _q2_profile(
                connection,
                runtime_sql(args, variant.validation_sql, variants),
            )
        return {
            "variantId": variant.variant_id,
            "query": variant.query_label,
            "status": "completed",
            "elapsedMs": elapsed_ms,
            "validationPayload": validation_payload,
            "error": "",
        }
    except Exception as exc:
        return {
            "variantId": variant.variant_id,
            "query": variant.query_label,
            "status": "failed",
            "elapsedMs": round((time.perf_counter() - started) * 1000, 3),
            "validationPayload": None,
            "error": str(exc),
        }
    finally:
        connection.close()


def _duration_text(ms: float | int | None) -> str:
    if ms is None:
        return "-"
    value = float(ms)
    if value < 1000:
        return f"{value:.0f} ms"
    seconds = value / 1000
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    return f"{minutes}m {seconds - minutes * 60:.1f}s"


def _comparison_row(
    *,
    args: argparse.Namespace,
    variant: PureDuckDBBenchmarkVariant,
    result: dict[str, Any],
    consistency_status: str,
    consistency_details: str,
) -> dict[str, Any]:
    metadata = variant.comparison_metadata
    return {
        **metadata,
        "dataset_size": f"{args.target_compressed_mib:g}MiB/file",
        "elapsed": _duration_text(result.get("elapsedMs")),
        "elapsed_ms": result.get("elapsedMs"),
        "status": result.get("status"),
        "consistency_status": consistency_status,
        "consistency_details": consistency_details,
        "error": result.get("error") or "",
    }


def markdown_table(rows: list[dict[str, Any]]) -> str:
    columns = pure_duckdb_q1_q2_comparison_columns()
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, separator]
    for row in rows:
        values = []
        for column in columns:
            text = str(row.get(column, "")).replace("\n", " ").replace("|", "\\|")
            if len(text) > TERMINAL_WIDTH:
                text = f"{text[: TERMINAL_WIDTH - 3]}..."
            values.append(text)
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def _selected_variants(args: argparse.Namespace) -> list[PureDuckDBBenchmarkVariant]:
    all_variants = list(pure_duckdb_q1_q2_benchmark_variants())
    variants = list(all_variants)
    selected_ids = {item.strip() for item in args.variant if item.strip()}
    selected_queries = {int(item) for item in args.query if item}
    if selected_ids:
        variants = [variant for variant in variants if variant.variant_id in selected_ids]
    if selected_queries:
        variants = [variant for variant in variants if variant.query_number in selected_queries]
    selected_query_numbers = {variant.query_number for variant in variants}
    baseline_ids = {
        1: "q1_baseline_current",
        2: "q2_baseline_current",
    }
    existing_ids = {variant.variant_id for variant in variants}
    baselines = [
        variant
        for query_number, baseline_id in baseline_ids.items()
        for variant in all_variants
        if query_number in selected_query_numbers
        and baseline_id not in existing_ids
        and variant.variant_id == baseline_id
    ]
    variants = [*baselines, *variants]
    return variants


def _rerun_fastest(
    args: argparse.Namespace,
    variants: list[PureDuckDBBenchmarkVariant],
    rows: list[dict[str, Any]],
    raw_results: list[dict[str, Any]],
) -> None:
    extra_runs = max(0, int(args.rerun_fastest))
    if extra_runs <= 0:
        return
    top_candidate_count = max(1, int(getattr(args, "rerun_top_candidates", 2)))
    by_query: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if row["consistency_status"] != "pass" or "baseline" in row["variant_id"]:
            continue
        query_number = int(str(row["query"]).removeprefix("Q"))
        by_query.setdefault(query_number, []).append(row)
    variant_by_id = {variant.variant_id: variant for variant in variants}
    for query_number, candidates in by_query.items():
        for rank, fastest in enumerate(
            sorted(candidates, key=lambda row: float(row.get("elapsed_ms") or 0))[
                :top_candidate_count
            ],
            start=1,
        ):
            variant = variant_by_id[fastest["variant_id"]]
            for run_index in range(1, extra_runs + 1):
                result = _execute_variant(args, variant, variants)
                raw_results.append(
                    {
                        **result,
                        "rerunOf": variant.variant_id,
                        "rerunIndex": run_index,
                        "rerunRank": rank,
                    }
                )
                rows.append(
                    {
                        **fastest,
                        "variant_id": f"{variant.variant_id}#rerun{run_index}",
                        "elapsed": _duration_text(result.get("elapsedMs")),
                        "elapsed_ms": result.get("elapsedMs"),
                        "status": result.get("status"),
                        "consistency_status": "rerun",
                        "consistency_details": (
                            f"Top passing {variant.query_label} candidate rank {rank} "
                            f"rerun {run_index}; original consistency was "
                            f"{fastest['consistency_status']}."
                        ),
                        "error": result.get("error") or "",
                    }
                )


def main() -> int:
    args = parse_args()
    variants = _selected_variants(args)
    if not variants:
        raise SystemExit("No benchmark variants selected.")

    summary: dict[str, Any] = {
        "datasetSize": f"{args.target_compressed_mib:g}MiB/file",
        "generated": [],
        "uploaded": [],
        "results": [],
        "comparisonRows": [],
    }

    if not args.skip_generate:
        summary["generated"] = generate_files(args)
        if not args.quiet:
            for item in summary["generated"]:
                print(f"{item['s3Url']}: {item['sizeMiB']} MiB", flush=True)
    if args.use_s3 and not args.skip_upload:
        summary["uploaded"] = upload_files(args)
        if not args.quiet:
            for item in summary["uploaded"]:
                print(f"uploaded {item['s3Url']} in {item['uploadMs']} ms", flush=True)

    q1_baseline: tuple[Any, ...] | None = None
    q2_baseline: dict[str, Any] | None = None
    comparison_rows: list[dict[str, Any]] = []
    raw_results: list[dict[str, Any]] = []

    for variant in variants:
        if not args.quiet:
            print(f"Running {variant.variant_id} ({variant.query_label})...", flush=True)
        result = _execute_variant(args, variant, variants)
        raw_results.append(result)
        if result["status"] != "completed":
            consistency_status = "fail"
            consistency_details = result["error"]
        elif variant.query_number == 1:
            consistency_status, consistency_details = _q1_consistency(
                result["validationPayload"],
                q1_baseline,
            )
            if variant.variant_id == "q1_baseline_current":
                q1_baseline = result["validationPayload"]
        else:
            consistency_status, consistency_details = _q2_consistency(
                result["validationPayload"],
                q2_baseline,
            )
            if variant.variant_id == "q2_baseline_current":
                q2_baseline = result["validationPayload"]
        comparison_rows.append(
            _comparison_row(
                args=args,
                variant=variant,
                result=result,
                consistency_status=consistency_status,
                consistency_details=consistency_details,
            )
        )

    _rerun_fastest(args, variants, comparison_rows, raw_results)
    summary["results"] = raw_results
    summary["comparisonRows"] = comparison_rows

    table = markdown_table(comparison_rows)
    print(table, flush=True)
    if not args.no_json:
        print(json.dumps(summary, indent=2, default=str, sort_keys=True), flush=True)
    if args.markdown_output is not None:
        args.markdown_output.parent.mkdir(parents=True, exist_ok=True)
        args.markdown_output.write_text(table + "\n", encoding="utf-8")
    if args.json_output is not None:
        args.json_output.parent.mkdir(parents=True, exist_ok=True)
        args.json_output.write_text(
            json.dumps(summary, indent=2, default=str, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    failed = any(row["consistency_status"] == "fail" for row in comparison_rows)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
