from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence


def relation_backed_daca_source(source: dict[str, object]) -> dict[str, object]:
    """Treat a tabular S3 object as a relation when DaCa metadata is requested.

    Local-only object publications keep their raw byte-stream behavior. DaCa,
    however, requires a typed schema and catalogs the DAAIF endpoint as JSON, so
    managed S3 objects must go through discovery and relation resolution first.
    """

    resolved = dict(source)
    source_kind = str(
        resolved.get("sourceKind") or resolved.get("source_kind") or ""
    ).strip()
    source_id = str(
        resolved.get("sourceId") or resolved.get("source_id") or ""
    ).strip()
    if source_kind != "object" or source_id != "s3":
        return resolved

    resolved["sourceKind"] = "relation"
    resolved["relation"] = ""
    return resolved


class S3RelationSourceResolver:
    """Resolve a stored S3 result to the relation created by source discovery."""

    def __init__(
        self,
        *,
        sync_s3_buckets: Callable[..., object],
        relation_specs_provider: Callable[[], Mapping[str, object]],
        relation_fields_provider: Callable[[str], Sequence[object]],
        object_head_provider: Callable[[str, str], object],
    ) -> None:
        self._sync_s3_buckets = sync_s3_buckets
        self._relation_specs_provider = relation_specs_provider
        self._relation_fields_provider = relation_fields_provider
        self._object_head_provider = object_head_provider

    def resolve(self, source: dict[str, object]) -> dict[str, object]:
        resolved = dict(source)
        source_kind = str(
            resolved.get("sourceKind") or resolved.get("source_kind") or ""
        ).strip()
        source_id = str(
            resolved.get("sourceId") or resolved.get("source_id") or ""
        ).strip()
        if source_kind != "relation" or source_id != "s3":
            return resolved

        bucket = str(resolved.get("bucket") or "").strip()
        key = str(resolved.get("key") or "").strip().lstrip("/")
        if not bucket and not key:
            return resolved
        if not bucket or not key:
            raise ValueError(
                "A stored S3 relation source requires both a bucket and an object key."
            )

        object_path = f"s3://{bucket}/{key}"
        try:
            self._object_head_provider(bucket, key)
        except Exception as exc:
            raise ValueError(
                f"The stored result at {object_path} is not available in Shared Workspace."
            ) from exc

        relation = self._queryable_relation_for_path(object_path)
        if not relation:
            try:
                self._sync_s3_buckets([bucket], emit_event=True)
            except Exception as exc:
                raise ValueError(self._unavailable_message(object_path)) from exc
            relation = self._queryable_relation_for_path(object_path)

        if not relation:
            raise ValueError(self._unavailable_message(object_path))

        resolved.update(
            {
                "sourceKind": "relation",
                "sourceId": "s3",
                "relation": relation,
                "bucket": bucket,
                "key": key,
            }
        )
        return resolved

    def _queryable_relation_for_path(self, object_path: str) -> str:
        try:
            specs = self._relation_specs_provider()
        except Exception:
            return ""

        for relation_id, spec in specs.items():
            if str(getattr(spec, "object_path", "") or "").strip() != object_path:
                continue
            relation = str(relation_id or "").strip()
            if not relation:
                continue
            try:
                fields = self._relation_fields_provider(relation)
            except Exception:
                continue
            if fields:
                return relation
        return ""

    @staticmethod
    def _unavailable_message(object_path: str) -> str:
        return (
            f"The stored result at {object_path} could not be registered as a "
            "queryable relation. Check Shared Workspace discovery and S3 "
            "permissions, then retry."
        )
