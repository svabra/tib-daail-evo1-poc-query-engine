from __future__ import annotations

import os
from pathlib import Path


DEFAULT_VERSION = "dev"
VERSION_ENV_VAR = "IMAGE_VERSION"
VERSION_FILE_NAME = "VERSION"


def _normalize_anchor(anchor: Path) -> Path:
    expanded = anchor.expanduser()
    try:
        resolved = expanded.resolve()
    except OSError:
        resolved = expanded.absolute()
    return resolved if resolved.is_dir() else resolved.parent


def _anchor_directories(*anchors: Path) -> list[Path]:
    effective_anchors = anchors or (Path(__file__).resolve(), Path.cwd())
    directories: list[Path] = []
    seen: set[str] = set()
    for anchor in effective_anchors:
        directory = _normalize_anchor(anchor)
        key = directory.as_posix().lower()
        if key in seen:
            continue
        seen.add(key)
        directories.append(directory)
    return directories


def discover_version_file(*anchors: Path) -> Path | None:
    seen: set[str] = set()
    for directory in _anchor_directories(*anchors):
        for candidate_dir in (directory, *directory.parents):
            candidate = candidate_dir / VERSION_FILE_NAME
            key = candidate.as_posix().lower()
            if key in seen:
                continue
            seen.add(key)
            try:
                if candidate.is_file():
                    return candidate
            except OSError:
                continue
    return None


def read_version_file(version_file: Path | None) -> str | None:
    if version_file is None:
        return None
    try:
        raw_value = version_file.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return raw_value or None


def current_repo_version(*anchors: Path, fallback: str = DEFAULT_VERSION) -> str:
    raw_value = read_version_file(discover_version_file(*anchors))
    return raw_value or fallback


def runtime_image_version(
    *anchors: Path,
    env_var: str = VERSION_ENV_VAR,
    fallback: str = DEFAULT_VERSION,
) -> str:
    raw_value = os.getenv(env_var)
    if raw_value is not None:
        normalized = raw_value.strip()
        if normalized:
            return normalized
    return current_repo_version(*anchors, fallback=fallback)
