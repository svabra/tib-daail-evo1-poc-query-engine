from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from urllib.parse import urlparse

from .source_references import normalize_qualified_name, normalize_reference_parts


ALIAS_ROOT_MIN_PARTS = {
    "local": 3,
    "pg": 3,
    "s3": 3,
    "workspace": 4,
}


@dataclass(frozen=True, slots=True)
class QueryAliasReference:
    alias: str
    start: int
    end: int
    parts: tuple[str, ...] = ()
    part_end_offsets: tuple[int, ...] = ()


def normalize_query_alias_segment(value: str, *, fallback: str = "item") -> str:
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value or "").strip().lower())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    if not normalized:
        normalized = fallback
    if normalized[0].isdigit():
        normalized = f"n_{normalized}"
    return normalized


def file_alias_segments(file_name: str, *, fallback: str = "file") -> list[str]:
    name = PurePosixPath(str(file_name or "").strip()).name
    if not name:
        return [normalize_query_alias_segment(fallback, fallback=fallback)]
    suffix = PurePosixPath(name).suffix.lower().lstrip(".")
    stem = name[: -(len(suffix) + 1)] if suffix else name
    segments = [normalize_query_alias_segment(stem, fallback=fallback)]
    if suffix:
        segments.append(normalize_query_alias_segment(suffix, fallback="file"))
    return segments


def local_query_alias(*, folder_path: str = "", file_name: str, root: str = "local") -> str:
    segments = [normalize_query_alias_segment(root, fallback="local")]
    segments.extend(
        normalize_query_alias_segment(part, fallback="folder")
        for part in str(folder_path or "").replace("\\", "/").split("/")
        if part.strip()
    )
    segments.extend(file_alias_segments(file_name, fallback="file"))
    return ".".join(segments)


def _contains_glob_token(value: str) -> bool:
    return any(token in str(value or "") for token in "*?[")


def _s3_alias_from_parts(
    *,
    bucket: str,
    folder_parts: list[str],
    file_name: str,
    root: str = "s3",
    drop_duplicate_leaf: bool = False,
) -> str:
    segments = [normalize_query_alias_segment(root, fallback="s3")]
    segments.append(normalize_query_alias_segment(bucket, fallback="bucket"))

    normalized_folders = [part for part in folder_parts if str(part or "").strip()]
    file_segments = file_alias_segments(file_name, fallback="s3_object")
    if normalized_folders and drop_duplicate_leaf and file_segments:
        folder_leaf = normalize_query_alias_segment(normalized_folders[-1], fallback="folder")
        if folder_leaf == file_segments[0]:
            normalized_folders = normalized_folders[:-1]

    segments.extend(
        normalize_query_alias_segment(part, fallback="folder")
        for part in normalized_folders
    )
    segments.extend(file_segments)
    return ".".join(segments)


def s3_collection_query_alias(
    *,
    bucket: str,
    prefix: str = "",
    display_name: str,
    root: str = "s3",
) -> str:
    folder_parts = [
        part
        for part in str(prefix or "").strip().strip("/").split("/")
        if part.strip()
    ]
    return _s3_alias_from_parts(
        bucket=bucket,
        folder_parts=folder_parts,
        file_name=display_name,
        root=root,
        drop_duplicate_leaf=True,
    )


def s3_query_alias(
    *,
    bucket: str,
    key: str = "",
    display_name: str = "",
    root: str = "s3",
) -> str:
    segments = [normalize_query_alias_segment(root, fallback="s3")]
    segments.append(normalize_query_alias_segment(bucket, fallback="bucket"))

    normalized_key = str(key or "").strip().strip("/")
    key_parts = [part for part in normalized_key.split("/") if part.strip()]
    has_glob = any(_contains_glob_token(part) for part in key_parts)
    normalized_display_name = str(display_name or "").strip()
    if has_glob and normalized_display_name:
        return s3_collection_query_alias(
            bucket=bucket,
            prefix="/".join(part for part in key_parts if not _contains_glob_token(part)),
            display_name=normalized_display_name,
            root=root,
        )

    if key_parts and not has_glob:
        folder_parts = key_parts[:-1]
        file_name = key_parts[-1]
    else:
        folder_parts = [part for part in key_parts if part and not _contains_glob_token(part)]
        file_name = normalized_display_name or (folder_parts.pop() if folder_parts else "s3_object")

    segments.extend(
        normalize_query_alias_segment(part, fallback="folder")
        for part in folder_parts
        if part.strip()
    )
    segments.extend(file_alias_segments(file_name, fallback="s3_object"))
    return ".".join(segments)


def s3_query_alias_from_path(path: str, *, display_name: str = "") -> str:
    parsed = urlparse(str(path or "").strip())
    if parsed.scheme != "s3" or not parsed.netloc:
        return ""
    return s3_query_alias(
        bucket=parsed.netloc,
        key=parsed.path.lstrip("/"),
        display_name=display_name,
    )


def add_stable_alias_suffix(alias: str, seed: str) -> str:
    parts = [part for part in str(alias or "").split(".") if part]
    if not parts:
        return alias
    suffix = hashlib.sha1(str(seed or alias).encode("utf-8")).hexdigest()[:8]
    target_index = len(parts) - 2 if len(parts) >= 3 else len(parts) - 1
    parts[target_index] = f"{parts[target_index]}_{suffix}"
    return ".".join(parts)


def unique_query_aliases(candidates: list[tuple[str, str]]) -> dict[str, str]:
    counts: dict[str, int] = {}
    for _key, alias in candidates:
        normalized = normalize_relation_key(alias)
        if normalized:
            counts[normalized] = counts.get(normalized, 0) + 1

    aliases: dict[str, str] = {}
    for key, alias in candidates:
        normalized = normalize_relation_key(alias)
        aliases[key] = add_stable_alias_suffix(alias, key) if counts.get(normalized, 0) > 1 else alias
    return aliases


def normalize_relation_key(value: str) -> str:
    return normalize_qualified_name(value)


def _is_identifier_start(char: str) -> bool:
    return char.isalpha() or char in {"_", "$"}


def _is_identifier_continue(char: str) -> bool:
    return char.isalnum() or char in {"_", "$", "-"}


def _read_identifier(text: str, index: int) -> tuple[str, int]:
    current = index + 1
    while current < len(text) and _is_identifier_continue(text[current]):
        current += 1
    return text[index:current], current


def _skip_quoted(text: str, index: int, quote: str, *, doubled_escape: bool) -> int:
    current = index + 1
    while current < len(text):
        if text[current] == quote:
            if doubled_escape and current + 1 < len(text) and text[current + 1] == quote:
                current += 2
                continue
            return current + 1
        current += 1
    return current


def _skip_bracketed_identifier(text: str, index: int) -> int:
    end = text.find("]", index + 1)
    return len(text) if end < 0 else end + 1


def _read_quoted_identifier_part(text: str, index: int, quote: str) -> tuple[str, int]:
    current = index + 1
    fragments: list[str] = []
    while current < len(text):
        char = text[current]
        if char == quote:
            if quote == '"' and current + 1 < len(text) and text[current + 1] == quote:
                fragments.append(quote)
                current += 2
                continue
            return "".join(fragments), current + 1
        fragments.append(char)
        current += 1
    return "".join(fragments), current


def _read_bracketed_identifier_part(text: str, index: int) -> tuple[str, int]:
    end = text.find("]", index + 1)
    if end < 0:
        return text[index + 1 :], len(text)
    return text[index + 1 : end], end + 1


def _read_alias_part_after_dot(text: str, index: int) -> tuple[str, int] | None:
    if index >= len(text):
        return None
    char = text[index]
    if char in {'"', "`"}:
        return _read_quoted_identifier_part(text, index, char)
    if char == "[":
        return _read_bracketed_identifier_part(text, index)
    if _is_identifier_start(char):
        return _read_identifier(text, index)
    return None


def _root_min_parts(root: str, configured_roots: set[str]) -> int:
    if root in ALIAS_ROOT_MIN_PARTS:
        return ALIAS_ROOT_MIN_PARTS[root]
    return 2 if root in configured_roots else 3


def find_query_alias_references(
    sql: str,
    *,
    roots: set[str] | None = None,
) -> list[QueryAliasReference]:
    configured_roots = {str(root).lower() for root in (roots or {"local", "s3"}) if str(root).strip()}
    text = str(sql or "")
    references: list[QueryAliasReference] = []
    index = 0

    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""
        if char == "-" and next_char == "-":
            newline = text.find("\n", index + 2)
            index = len(text) if newline < 0 else newline + 1
            continue
        if char == "/" and next_char == "*":
            end = text.find("*/", index + 2)
            index = len(text) if end < 0 else end + 2
            continue
        if char == "'":
            index = _skip_quoted(text, index, "'", doubled_escape=True)
            continue
        if char == '"':
            index = _skip_quoted(text, index, '"', doubled_escape=True)
            continue
        if char == "`":
            index = _skip_quoted(text, index, "`", doubled_escape=False)
            continue
        if char == "[":
            index = _skip_bracketed_identifier(text, index)
            continue
        if not _is_identifier_start(char):
            index += 1
            continue
        if index > 0 and (text[index - 1] == "." or _is_identifier_continue(text[index - 1])):
            index += 1
            continue

        root, current = _read_identifier(text, index)
        normalized_root = root.lower()
        if normalized_root not in configured_roots:
            index = current
            continue

        parts = [root]
        part_ends = [current]
        while current < len(text) and text[current] == ".":
            next_index = current + 1
            parsed_part = _read_alias_part_after_dot(text, next_index)
            if parsed_part is None:
                break
            part, current = parsed_part
            parts.append(part)
            part_ends.append(current)

        min_parts = _root_min_parts(normalized_root, configured_roots)
        if len(parts) >= min_parts:
            references.append(
                QueryAliasReference(
                    alias=text[index : part_ends[-1]],
                    start=index,
                    end=part_ends[-1],
                    parts=tuple(parts),
                    part_end_offsets=tuple(part_ends),
                )
            )
            index = part_ends[-1]
            continue
        index += 1

    return references


def rewrite_query_aliases(sql: str, alias_map: dict[str, str]) -> str:
    normalized_alias_map = {
        normalize_relation_key(alias): relation
        for alias, relation in alias_map.items()
        if normalize_relation_key(alias) and str(relation or "").strip()
    }
    if not normalized_alias_map:
        return str(sql or "")

    roots = {
        key.split(".", 1)[0]
        for key in normalized_alias_map
        if "." in key
    }
    text = str(sql or "")
    pieces: list[str] = []
    last_index = 0
    for reference in find_query_alias_references(text, roots=roots):
        parts = list(reference.parts)
        part_end_offsets = list(reference.part_end_offsets)
        if not parts or not part_end_offsets:
            parts = [part for part in reference.alias.split(".") if part]
            part_end_offsets = []
            cursor = reference.start
            for part in parts:
                cursor += len(part)
                part_end_offsets.append(cursor)
                if cursor < reference.end and text[cursor] == ".":
                    cursor += 1

        replacement: str | None = None
        replacement_end = reference.end
        for count in range(len(parts), 1, -1):
            candidate = normalize_reference_parts(parts[:count])
            if candidate in normalized_alias_map:
                replacement = normalized_alias_map[candidate]
                replacement_end = part_end_offsets[count - 1]
                break
        if replacement is None:
            continue
        pieces.append(text[last_index:reference.start])
        pieces.append(replacement)
        last_index = replacement_end
    pieces.append(text[last_index:])
    return "".join(pieces)
