from __future__ import annotations

from ..models import DataGeneratorFolder


def build_runbook_tree(generators: list[dict[str, object]]) -> list[DataGeneratorFolder]:
    roots: list[DataGeneratorFolder] = []
    folder_index: dict[tuple[str, ...], DataGeneratorFolder] = {}

    for generator in generators:
        raw_path = generator.get("treePath") if isinstance(generator, dict) else None
        tree_path = tuple(
            segment
            for segment in (str(item).strip() for item in (raw_path or []))
            if segment
        )
        if not tree_path:
            continue

        path_key: tuple[str, ...] = ()
        parent_folder: DataGeneratorFolder | None = None

        for segment in tree_path:
            path_key = (*path_key, segment)
            folder = folder_index.get(path_key)
            if folder is None:
                folder = DataGeneratorFolder(
                    folder_id="-".join(part.lower().replace(" ", "-") for part in path_key),
                    name=segment,
                )
                folder_index[path_key] = folder
                if parent_folder is None:
                    roots.append(folder)
                else:
                    parent_folder.folders.append(folder)
            parent_folder = folder

        parent_folder.generators.append(generator)

    return roots
