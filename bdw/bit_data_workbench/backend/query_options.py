from __future__ import annotations

from typing import Any


PARQUET_HIVE_PARTITIONING_OPTIONS = {"auto", "on", "off"}
CACHE_HYDRATION_MODES = {"off", "on"}
CACHE_HYDRATION_SCOPES = {"referencedS3Parquet"}
CACHE_HYDRATION_INDEX_POLICIES = {"autoPredicates"}
RESULT_STORAGE_MODES = {"off", "on"}
SOURCE_EXISTENCE_VALIDATION_OPTIONS = {"off", "on"}
DEFAULT_QUERY_OPTIONS = {
    "duckdb": {
        "parquetHivePartitioning": "auto",
        "cacheHydration": {
            "mode": "off",
            "scope": "referencedS3Parquet",
            "indexPolicy": "autoPredicates",
        },
        "resultStorage": {
            "mode": "off",
            "path": "",
        },
    },
    "validation": {
        "sourceExistence": "off",
    },
}


def default_query_options() -> dict[str, dict[str, object]]:
    return {
        "duckdb": {
            "parquetHivePartitioning": "auto",
            "cacheHydration": {
                "mode": "off",
                "scope": "referencedS3Parquet",
                "indexPolicy": "autoPredicates",
            },
            "resultStorage": {
                "mode": "off",
                "path": "",
            },
        },
        "validation": {
            "sourceExistence": "off",
        },
    }


def normalize_query_options(value: Any) -> dict[str, dict[str, object]]:
    if value is None or value == "":
        return default_query_options()
    if hasattr(value, "model_dump"):
        value = value.model_dump(by_alias=True)
    if not isinstance(value, dict):
        raise ValueError("queryOptions must be an object.")

    duckdb_options = value.get("duckdb")
    if duckdb_options is None:
        duckdb_options = {}
    if not isinstance(duckdb_options, dict):
        raise ValueError("queryOptions.duckdb must be an object.")

    validation_options = value.get("validation")
    if validation_options is None:
        validation_options = {}
    if not isinstance(validation_options, dict):
        raise ValueError("queryOptions.validation must be an object.")

    parquet_hive_partitioning = str(
        duckdb_options.get("parquetHivePartitioning") or "auto"
    ).strip().lower()
    if parquet_hive_partitioning not in PARQUET_HIVE_PARTITIONING_OPTIONS:
        raise ValueError(
            "queryOptions.duckdb.parquetHivePartitioning must be one of: auto, on, off."
        )

    cache_hydration = duckdb_options.get("cacheHydration")
    if cache_hydration is None:
        cache_hydration = {}
    if not isinstance(cache_hydration, dict):
        raise ValueError("queryOptions.duckdb.cacheHydration must be an object.")

    mode = str(cache_hydration.get("mode") or "off").strip().lower()
    if mode not in CACHE_HYDRATION_MODES:
        raise ValueError("queryOptions.duckdb.cacheHydration.mode must be one of: off, on.")
    scope = str(cache_hydration.get("scope") or "referencedS3Parquet").strip()
    if scope not in CACHE_HYDRATION_SCOPES:
        raise ValueError(
            "queryOptions.duckdb.cacheHydration.scope must be referencedS3Parquet."
        )
    index_policy = str(cache_hydration.get("indexPolicy") or "autoPredicates").strip()
    if index_policy not in CACHE_HYDRATION_INDEX_POLICIES:
        raise ValueError(
            "queryOptions.duckdb.cacheHydration.indexPolicy must be autoPredicates."
        )

    result_storage = duckdb_options.get("resultStorage")
    if result_storage is None:
        result_storage = {}
    if not isinstance(result_storage, dict):
        raise ValueError("queryOptions.duckdb.resultStorage must be an object.")

    result_storage_mode = str(result_storage.get("mode") or "off").strip().lower()
    if result_storage_mode not in RESULT_STORAGE_MODES:
        raise ValueError("queryOptions.duckdb.resultStorage.mode must be one of: off, on.")
    result_storage_path = str(result_storage.get("path") or "").strip()

    source_existence = (
        str(validation_options.get("sourceExistence") or "off").strip().lower()
    )
    if source_existence not in SOURCE_EXISTENCE_VALIDATION_OPTIONS:
        raise ValueError(
            "queryOptions.validation.sourceExistence must be one of: off, on."
        )

    return {
        "duckdb": {
            "parquetHivePartitioning": parquet_hive_partitioning,
            "cacheHydration": {
                "mode": mode,
                "scope": scope,
                "indexPolicy": index_policy,
            },
            "resultStorage": {
                "mode": result_storage_mode,
                "path": result_storage_path,
            },
        },
        "validation": {
            "sourceExistence": source_existence,
        },
    }


def parquet_hive_partitioning_option(query_options: Any) -> str:
    return str(normalize_query_options(query_options)["duckdb"]["parquetHivePartitioning"])


def cache_hydration_options(query_options: Any) -> dict[str, str]:
    options = normalize_query_options(query_options)["duckdb"]["cacheHydration"]
    return {
        "mode": str(options.get("mode") or "off"),
        "scope": str(options.get("scope") or "referencedS3Parquet"),
        "indexPolicy": str(options.get("indexPolicy") or "autoPredicates"),
    }


def cache_hydration_enabled(query_options: Any) -> bool:
    return cache_hydration_options(query_options)["mode"] == "on"


def result_storage_options(query_options: Any) -> dict[str, str]:
    options = normalize_query_options(query_options)["duckdb"]["resultStorage"]
    return {
        "mode": str(options.get("mode") or "off"),
        "path": str(options.get("path") or "").strip(),
    }


def result_storage_enabled(query_options: Any) -> bool:
    return result_storage_options(query_options)["mode"] == "on"


def source_existence_validation_enabled(query_options: Any) -> bool:
    return normalize_query_options(query_options)["validation"]["sourceExistence"] == "on"
