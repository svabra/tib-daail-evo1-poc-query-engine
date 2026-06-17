from __future__ import annotations

import re

from .sql_utils import sql_literal


_PARQUET_SELECT_UNION_RE = re.compile(
    r"""
    \bSELECT\s+\*\s+FROM\s+read_parquet\(\s*
    (?P<first>'(?:[^']|'')*')
    \s*\)
    (?:
        \s+UNION\s+ALL\s+
        SELECT\s+\*\s+FROM\s+read_parquet\(\s*
        '(?:[^']|'')*'
        \s*\)
    )+
    """,
    re.IGNORECASE | re.VERBOSE,
)
_PARQUET_PATH_RE = re.compile(
    r"read_parquet\(\s*(?P<path>'(?:[^']|'')*')\s*\)",
    re.IGNORECASE,
)


def rewrite_parquet_select_star_unions(sql: str) -> str:
    text = str(sql or "")
    if "union" not in text.lower() or "read_parquet" not in text.lower():
        return text

    protected_spans = _protected_sql_spans(text)
    replacements: list[tuple[int, int, str]] = []
    for match in _PARQUET_SELECT_UNION_RE.finditer(text):
        if _span_contains_offset(protected_spans, match.start()):
            continue
        if _is_followed_by_union(text, match.end()):
            continue

        path_literals = [
            item.group("path")
            for item in _PARQUET_PATH_RE.finditer(match.group(0))
        ]
        if len(path_literals) < 2:
            continue
        paths = [_sql_string_literal_value(path) for path in path_literals]
        if not all(_is_s3_parquet_path(path) for path in paths):
            continue

        literal_list = ",\n    ".join(sql_literal(path) for path in paths)
        replacement = (
            "SELECT * FROM read_parquet([\n"
            f"    {literal_list}\n"
            "  ], union_by_name = true)"
        )
        replacements.append((match.start(), match.end(), replacement))

    if not replacements:
        return text

    output: list[str] = []
    current = 0
    for start, end, replacement in replacements:
        if start < current:
            continue
        output.append(text[current:start])
        output.append(replacement)
        current = end
    output.append(text[current:])
    return "".join(output)


def _is_s3_parquet_path(path: str) -> bool:
    normalized = str(path or "").strip().lower()
    return normalized.startswith("s3://") and normalized.endswith(".parquet")


def _is_followed_by_union(sql: str, offset: int) -> bool:
    tail = str(sql or "")[offset:].lstrip()
    return tail[:5].lower() == "union"


def _sql_string_literal_value(literal: str) -> str:
    text = str(literal or "").strip()
    if len(text) < 2 or not (text.startswith("'") and text.endswith("'")):
        return text
    return text[1:-1].replace("''", "'")


def _span_contains_offset(spans: list[tuple[int, int]], offset: int) -> bool:
    return any(start <= offset < end for start, end in spans)


def _protected_sql_spans(sql: str) -> list[tuple[int, int]]:
    spans: list[tuple[int, int]] = []
    text = str(sql or "")
    quote: str | None = None
    quote_start = 0
    index = 0
    while index < len(text):
        char = text[index]
        next_char = text[index + 1] if index + 1 < len(text) else ""

        if quote is not None:
            if char == quote:
                if quote in {"'", '"'} and next_char == quote:
                    index += 2
                    continue
                spans.append((quote_start, index + 1))
                quote = None
            index += 1
            continue

        if char == "-" and next_char == "-":
            start = index
            while index < len(text) and text[index] not in "\r\n":
                index += 1
            spans.append((start, index))
            continue

        if char == "/" and next_char == "*":
            start = index
            index += 2
            while index + 1 < len(text) and not (
                text[index] == "*" and text[index + 1] == "/"
            ):
                index += 1
            index = index + 2 if index + 1 < len(text) else len(text)
            spans.append((start, index))
            continue

        if char in {"'", '"', "`"}:
            quote = char
            quote_start = index
            index += 1
            continue

        index += 1

    if quote is not None:
        spans.append((quote_start, len(text)))
    return spans
