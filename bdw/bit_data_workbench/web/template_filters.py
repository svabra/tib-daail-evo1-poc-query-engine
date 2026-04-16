from __future__ import annotations

from fastapi.templating import Jinja2Templates


TRUNCATION_MARKER = "[..]"
MAX_SOURCE_NAVIGATION_STEM_CHARS = 25


def truncate_source_navigation_label(
    value: object,
    max_stem_chars: int = MAX_SOURCE_NAVIGATION_STEM_CHARS,
) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    last_dot = text.rfind(".")
    has_file_suffix = 0 < last_dot < len(text) - 1
    if has_file_suffix:
        stem = text[:last_dot]
        suffix = text[last_dot:]
        if len(stem) <= max_stem_chars:
            return text
        return f"{stem[:max_stem_chars]}{TRUNCATION_MARKER}{suffix}"

    if len(text) <= max_stem_chars:
        return text
    return f"{text[:max_stem_chars]}{TRUNCATION_MARKER}"


def format_byte_count(value: object) -> str:
    normalized_size = int(value or 0)
    if normalized_size < 1024:
        return f"{normalized_size} B"
    if normalized_size < 1024 * 1024:
        return f"{normalized_size / 1024:.1f} KB"
    return f"{normalized_size / (1024 * 1024):.1f} MB"


def register_template_filters(templates: Jinja2Templates) -> None:
    templates.env.filters["truncate_source_navigation_label"] = (
        truncate_source_navigation_label
    )
    templates.env.filters["format_byte_count"] = format_byte_count
