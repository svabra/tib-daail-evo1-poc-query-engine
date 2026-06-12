from __future__ import annotations

from typing import Any

from ..backend.data_sources.s3_paths import source_object_s3_location
from ..models import SourceCatalog, SourceObject


def build_source_tree_s3_hierarchy(
    catalogs: list[SourceCatalog] | tuple[SourceCatalog, ...],
) -> dict[str, list[dict[str, Any]]]:
    hierarchy: dict[str, list[dict[str, Any]]] = {}

    for catalog in catalogs:
        catalog_source_id = str(catalog.connection_source_id or catalog.name or "").strip()
        if catalog_source_id != "workspace.s3":
            continue
        for schema in catalog.schemas:
            schema_key = f"{catalog.name}::{schema.name}"
            root = _new_folder(label="", bucket=str(schema.label or schema.name or ""), prefix="")
            for source_object in schema.objects:
                _add_source_object(root, source_object)
            children = _finalize_children(root)
            if children:
                hierarchy[schema_key] = children

    return hierarchy


def _new_folder(*, label: str, bucket: str, prefix: str) -> dict[str, Any]:
    normalized_prefix = prefix.strip("/")
    prefix_with_slash = f"{normalized_prefix}/" if normalized_prefix else ""
    path = f"s3://{bucket}/{prefix_with_slash}" if bucket else prefix_with_slash
    return {
        "kind": "folder",
        "label": label,
        "bucket": bucket,
        "prefix": prefix_with_slash,
        "path": path,
        "searchable": " ".join(
            part
            for part in (label, bucket, prefix_with_slash, path)
            if str(part or "").strip()
        ),
        "_folders": {},
        "_objects": [],
    }


def _add_source_object(root: dict[str, Any], source_object: SourceObject) -> None:
    location = source_object_s3_location(source_object)
    if location is None:
        display_name = str(
            getattr(source_object, "display_name", "")
            or getattr(source_object, "name", "")
            or ""
        ).strip()
        root["_objects"].append(
            {
                "kind": "object",
                "label": display_name,
                "source_object": source_object,
            }
        )
        return

    segments = [segment for segment in location.logical_key.split("/") if segment]
    if not segments:
        return

    folder = root
    for index, segment in enumerate(segments[:-1]):
        prefix = "/".join(segments[: index + 1])
        folders = folder["_folders"]
        if segment not in folders:
            folders[segment] = _new_folder(
                label=segment,
                bucket=location.bucket,
                prefix=prefix,
            )
        folder = folders[segment]

    display_name = str(
        getattr(source_object, "display_name", "")
        or getattr(source_object, "name", "")
        or ""
    ).strip()
    query_reference = str(
        getattr(source_object, "query_reference", "")
        or getattr(source_object, "query_alias", "")
        or getattr(source_object, "relation", "")
        or ""
    ).strip()
    folder["_objects"].append(
        {
            "kind": "object",
            "label": location.leaf_label or display_name,
            "searchable": " ".join(
                part
                for part in (
                    location.leaf_label,
                    display_name,
                    getattr(source_object, "name", ""),
                    getattr(source_object, "relation", ""),
                    query_reference,
                    location.key,
                    getattr(source_object, "s3_path", ""),
                )
                if str(part or "").strip()
            ),
            "source_object": source_object,
        }
    )


def _finalize_children(folder: dict[str, Any]) -> list[dict[str, Any]]:
    child_folders = [
        _finalize_folder(child)
        for child in folder.get("_folders", {}).values()
    ]
    child_objects = list(folder.get("_objects", []))
    child_folders.sort(key=lambda node: str(node.get("label") or "").lower())
    child_objects.sort(key=lambda node: str(node.get("label") or "").lower())
    return child_folders + child_objects


def _finalize_folder(folder: dict[str, Any]) -> dict[str, Any]:
    finalized = {
        key: value
        for key, value in folder.items()
        if key not in {"_folders", "_objects"}
    }
    children = _finalize_children(folder)
    finalized["children"] = children
    finalized["childCount"] = len(children)
    return finalized
