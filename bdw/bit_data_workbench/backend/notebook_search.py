from __future__ import annotations

from hashlib import sha256
import json
from urllib.parse import quote

from ..models import NotebookDefinition


def notebook_search_items(
    notebooks: list[NotebookDefinition],
) -> list[dict[str, object]]:
    """Return the intentionally small notebook document used by the home search."""

    items = [
        {
            "id": notebook.notebook_id,
            "title": notebook.title,
            "summary": notebook.summary,
            "tags": list(notebook.tags),
            "path": " / ".join(notebook.tree_path),
            "type": "shared" if notebook.shared else "built-in",
            "targetUrl": f"/notebooks/{quote(notebook.notebook_id, safe='')}",
        }
        for notebook in notebooks
    ]
    return sorted(
        items,
        key=lambda item: (
            str(item["title"]).casefold(),
            str(item["id"]),
        ),
    )


def versioned_notebook_search_document(
    notebooks: list[NotebookDefinition],
) -> tuple[dict[str, object], str]:
    items = notebook_search_items(notebooks)
    canonical = json.dumps(
        items,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    version = sha256(canonical).hexdigest()[:16]
    return {"version": version, "items": items}, version
