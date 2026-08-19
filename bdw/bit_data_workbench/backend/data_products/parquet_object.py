from __future__ import annotations

from collections.abc import Callable
from threading import RLock
from typing import Any

import duckdb

from ...models import SourceField
from ..sql_utils import sql_literal


class S3ParquetObjectReader:
    """Inspect and read one concrete Parquet object without catalog discovery."""

    def __init__(
        self,
        *,
        connection_factory: Callable[[], duckdb.DuckDBPyConnection],
        object_head_provider: Callable[[str, str], object],
    ) -> None:
        self._connection_factory = connection_factory
        self._object_head_provider = object_head_provider
        self._field_cache: dict[tuple[str, str, str], tuple[SourceField, ...]] = {}
        self._cache_lock = RLock()

    def fields(self, bucket: str, key: str) -> list[SourceField]:
        normalized_bucket, normalized_key = self._normalize_location(bucket, key)
        revision = self._object_revision(normalized_bucket, normalized_key)
        cache_key = (normalized_bucket, normalized_key, revision)
        with self._cache_lock:
            cached = self._field_cache.get(cache_key)
        if cached is not None:
            return [SourceField(name=field.name, data_type=field.data_type) for field in cached]

        object_path = self._object_path(normalized_bucket, normalized_key)
        connection = self._connection_factory()
        try:
            rows = connection.execute(
                "DESCRIBE SELECT * FROM "
                f"read_parquet({sql_literal(object_path)})"
            ).fetchall()
        except Exception as exc:
            raise ValueError(
                f"The stored result at {object_path} could not be read as Parquet: {exc}"
            ) from exc
        finally:
            connection.close()

        fields = tuple(
            SourceField(name=str(row[0]).strip(), data_type=str(row[1]).strip() or "VARCHAR")
            for row in rows
            if row and str(row[0]).strip()
        )
        if not fields:
            raise ValueError(
                f"The stored Parquet result at {object_path} does not expose any schema fields."
            )

        with self._cache_lock:
            stale_keys = [
                item
                for item in self._field_cache
                if item[:2] == (normalized_bucket, normalized_key) and item != cache_key
            ]
            for stale_key in stale_keys:
                self._field_cache.pop(stale_key, None)
            self._field_cache[cache_key] = fields
        return [SourceField(name=field.name, data_type=field.data_type) for field in fields]

    def page(
        self,
        bucket: str,
        key: str,
        limit: int,
        offset: int,
    ) -> tuple[list[SourceField], list[tuple[Any, ...]], bool]:
        normalized_bucket, normalized_key = self._normalize_location(bucket, key)
        fields = self.fields(normalized_bucket, normalized_key)
        object_path = self._object_path(normalized_bucket, normalized_key)
        connection = self._connection_factory()
        try:
            rows = connection.execute(
                "SELECT * FROM "
                f"read_parquet({sql_literal(object_path)}) "
                f"LIMIT {int(limit) + 1} OFFSET {int(offset)}"
            ).fetchall()
        except Exception as exc:
            raise ValueError(
                f"The stored result at {object_path} could not be read as Parquet: {exc}"
            ) from exc
        finally:
            connection.close()

        has_more = len(rows) > int(limit)
        return fields, list(rows[: int(limit)]), has_more

    def _object_revision(self, bucket: str, key: str) -> str:
        object_path = self._object_path(bucket, key)
        try:
            raw_head = self._object_head_provider(bucket, key)
        except Exception as exc:
            raise ValueError(
                f"The stored result at {object_path} is not available in Shared Workspace."
            ) from exc

        head = raw_head if isinstance(raw_head, dict) else {}
        version_id = str(head.get("VersionId") or "").strip()
        etag = str(head.get("ETag") or "").strip().strip('"')
        content_length = str(head.get("ContentLength") or "").strip()
        last_modified = str(head.get("LastModified") or "").strip()
        return "|".join((version_id, etag, content_length, last_modified))

    @staticmethod
    def _normalize_location(bucket: str, key: str) -> tuple[str, str]:
        normalized_bucket = str(bucket or "").strip()
        normalized_key = str(key or "").strip().lstrip("/")
        if not normalized_bucket or not normalized_key:
            raise ValueError(
                "A stored Parquet data product requires both a bucket and an object key."
            )
        if not normalized_key.lower().endswith(".parquet"):
            raise ValueError(
                "Typed S3 data products currently require one concrete Parquet object."
            )
        return normalized_bucket, normalized_key

    @staticmethod
    def _object_path(bucket: str, key: str) -> str:
        return f"s3://{bucket}/{key}"
