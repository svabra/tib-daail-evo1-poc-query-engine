from __future__ import annotations


def relation_backed_daca_source(source: dict[str, object]) -> dict[str, object]:
    """Expose a concrete S3 object through the tabular public JSON endpoint.

    Local-only object publications keep their raw byte-stream behavior. DaCa,
    however, requires a typed schema and catalogs the DAAIF endpoint as JSON, so
    managed S3 objects are represented as relations. The backend resolves the
    concrete bucket/key directly and does not require catalog discovery.
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
