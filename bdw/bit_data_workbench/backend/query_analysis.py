from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable
from urllib.parse import urlparse

from ..models import SourceCatalog


TABLE_FUNCTION_NAMES = {
    "read_csv",
    "read_csv_auto",
    "read_json",
    "read_json_auto",
    "read_ndjson",
    "read_parquet",
}
JOIN_KEYWORDS = {"join", "left", "right", "full", "inner", "cross", "outer", "semi", "anti", "asof"}
SOURCE_CLAUSE_TERMINATORS = {
    "connect",
    "except",
    "fetch",
    "group",
    "having",
    "intersect",
    "limit",
    "offset",
    "on",
    "order",
    "pivot",
    "qualify",
    "sample",
    "union",
    "unpivot",
    "using",
    "where",
    "window",
}
SOURCE_ENTRY_MODIFIERS = {"lateral", "only"}


@dataclass(frozen=True, slots=True)
class SqlToken:
    kind: str
    value: str
    normalized: str


@dataclass(frozen=True, slots=True)
class KnownRelationReference:
    relation: str
    bucket: str = ""


@dataclass(slots=True)
class QueryTouchSummary:
    touched_relations: list[str] = field(default_factory=list)
    touched_buckets: list[str] = field(default_factory=list)


def normalize_relation_key(value: str) -> str:
    parts = [
        segment.strip().strip('"').strip("`").strip("[]").lower()
        for segment in str(value or "").split(".")
        if segment.strip()
    ]
    return ".".join(parts)


def build_relation_index(catalogs: Iterable[SourceCatalog]) -> dict[str, KnownRelationReference]:
    relation_entries: list[tuple[set[str], KnownRelationReference]] = []
    alias_counts: dict[str, int] = {}

    for catalog in catalogs:
        catalog_prefixes = {
            normalize_relation_key(catalog.name),
            normalize_relation_key(catalog.connection_source_id or catalog.name),
        }
        for schema in catalog.schemas:
            for source_object in schema.objects:
                canonical_relation = str(source_object.relation or "").strip()
                if not canonical_relation:
                    continue
                entry = KnownRelationReference(
                    relation=canonical_relation,
                    bucket=str(source_object.s3_bucket or "").strip(),
                )
                aliases = {
                    normalize_relation_key(canonical_relation),
                    normalize_relation_key(source_object.name),
                }
                if schema.name:
                    aliases.add(normalize_relation_key(f"{schema.name}.{source_object.name}"))
                for prefix in catalog_prefixes:
                    if prefix:
                        aliases.add(normalize_relation_key(f"{prefix}.{canonical_relation}"))
                        aliases.add(normalize_relation_key(f"{prefix}.{source_object.name}"))
                        if schema.name:
                            aliases.add(normalize_relation_key(f"{prefix}.{schema.name}.{source_object.name}"))

                for alias in aliases:
                    if alias:
                        alias_counts[alias] = alias_counts.get(alias, 0) + 1
                relation_entries.append((aliases, entry))

    index: dict[str, KnownRelationReference] = {}
    for aliases, entry in relation_entries:
        canonical_alias = normalize_relation_key(entry.relation)
        for alias in aliases:
            if not alias:
                continue
            if alias != canonical_alias and alias_counts.get(alias, 0) > 1:
                continue
            index.setdefault(alias, entry)
    return index


def analyze_query_touches(
    sql: str,
    relation_index: dict[str, KnownRelationReference] | None = None,
) -> QueryTouchSummary:
    tokens = tokenize_sql(sql)
    if not tokens:
        return QueryTouchSummary()

    relation_index = relation_index or {}
    cte_names = extract_cte_names(tokens)
    extracted_relations = extract_relation_references(tokens, cte_names)
    direct_s3_paths = extract_direct_s3_paths(tokens)

    touched_relations: list[str] = []
    touched_buckets: list[str] = []
    seen_relations: set[str] = set()
    seen_buckets: set[str] = set()

    def add_relation(value: str) -> None:
        normalized = normalize_relation_key(value)
        if not normalized or normalized in seen_relations:
            return
        seen_relations.add(normalized)
        touched_relations.append(value)

    def add_bucket(value: str) -> None:
        normalized = str(value or "").strip().lower()
        if not normalized or normalized in seen_buckets:
            return
        seen_buckets.add(normalized)
        touched_buckets.append(str(value).strip())

    for relation_name in extracted_relations:
        lookup_key = normalize_relation_key(relation_name)
        known_relation = relation_index.get(lookup_key)
        if known_relation is not None:
            add_relation(known_relation.relation)
            if known_relation.bucket:
                add_bucket(known_relation.bucket)
            continue
        add_relation(relation_name)

    for s3_path in direct_s3_paths:
        add_relation(s3_path)
        bucket_name = s3_bucket_from_path(s3_path)
        if bucket_name:
            add_bucket(bucket_name)

    return QueryTouchSummary(
        touched_relations=touched_relations,
        touched_buckets=touched_buckets,
    )


def tokenize_sql(sql: str) -> list[SqlToken]:
    tokens: list[SqlToken] = []
    text = str(sql or "")
    length = len(text)
    index = 0

    while index < length:
        current = text[index]
        if current.isspace():
            index += 1
            continue
        if current == "-" and index + 1 < length and text[index + 1] == "-":
            index += 2
            while index < length and text[index] not in "\r\n":
                index += 1
            continue
        if current == "/" and index + 1 < length and text[index + 1] == "*":
            index += 2
            while index + 1 < length and not (text[index] == "*" and text[index + 1] == "/"):
                index += 1
            index = min(length, index + 2)
            continue
        if current == "'":
            value, index = read_quoted_token(text, index, quote_char="'", doubled_escape=True)
            tokens.append(SqlToken(kind="string", value=value, normalized=value.lower()))
            continue
        if current == '"':
            value, index = read_quoted_token(text, index, quote_char='"', doubled_escape=True)
            tokens.append(SqlToken(kind="identifier", value=value, normalized=value.lower()))
            continue
        if current == "`":
            value, index = read_quoted_token(text, index, quote_char="`", doubled_escape=False)
            tokens.append(SqlToken(kind="identifier", value=value, normalized=value.lower()))
            continue
        if current == "[":
            end_index = text.find("]", index + 1)
            if end_index == -1:
                value = text[index + 1 :]
                index = length
            else:
                value = text[index + 1 : end_index]
                index = end_index + 1
            tokens.append(SqlToken(kind="identifier", value=value, normalized=value.lower()))
            continue
        if current.isalpha() or current == "_" or current == "$":
            start_index = index
            index += 1
            while index < length and (text[index].isalnum() or text[index] in {"_", "$"}):
                index += 1
            value = text[start_index:index]
            tokens.append(SqlToken(kind="word", value=value, normalized=value.lower()))
            continue
        if current.isdigit():
            start_index = index
            index += 1
            while index < length and (text[index].isdigit() or text[index] == "."):
                index += 1
            value = text[start_index:index]
            tokens.append(SqlToken(kind="number", value=value, normalized=value))
            continue
        if current in "(),.;":
            tokens.append(SqlToken(kind="symbol", value=current, normalized=current))
            index += 1
            continue
        if current == ".":
            tokens.append(SqlToken(kind="symbol", value=current, normalized=current))
            index += 1
            continue
        index += 1

    return tokens


def read_quoted_token(
    text: str,
    index: int,
    *,
    quote_char: str,
    doubled_escape: bool,
) -> tuple[str, int]:
    index += 1
    length = len(text)
    fragments: list[str] = []
    while index < length:
        current = text[index]
        if current == quote_char:
            if doubled_escape and index + 1 < length and text[index + 1] == quote_char:
                fragments.append(quote_char)
                index += 2
                continue
            return "".join(fragments), index + 1
        fragments.append(current)
        index += 1
    return "".join(fragments), length


def extract_cte_names(tokens: list[SqlToken]) -> set[str]:
    cte_names: set[str] = set()
    if not tokens:
        return cte_names

    index = 0
    while index < len(tokens):
        token = tokens[index]
        if token.kind == "word" and token.normalized == "with":
            index += 1
            if index < len(tokens) and tokens[index].kind == "word" and tokens[index].normalized == "recursive":
                index += 1
            while index < len(tokens):
                if not is_identifier_token(tokens[index]):
                    return cte_names
                cte_names.add(tokens[index].normalized)
                index += 1
                if index < len(tokens) and tokens[index].value == "(":
                    index = skip_parenthesized(tokens, index)
                if index >= len(tokens) or tokens[index].kind != "word" or tokens[index].normalized != "as":
                    return cte_names
                index += 1
                if index >= len(tokens) or tokens[index].value != "(":
                    return cte_names
                index = skip_parenthesized(tokens, index)
                if index < len(tokens) and tokens[index].value == ",":
                    index += 1
                    continue
                return cte_names
        index += 1
    return cte_names


def extract_relation_references(tokens: list[SqlToken], cte_names: set[str]) -> list[str]:
    relations: list[str] = []
    seen: set[str] = set()
    index = 0

    def add_relation(value: str) -> None:
        normalized = normalize_relation_key(value)
        if not normalized or normalized in cte_names or normalized in seen:
            return
        seen.add(normalized)
        relations.append(value)

    while index < len(tokens):
        token = tokens[index]
        if token.kind != "word":
            index += 1
            continue

        normalized = token.normalized
        if normalized == "delete" and index + 1 < len(tokens) and tokens[index + 1].normalized == "from":
            index = consume_single_relation(tokens, index + 2, add_relation)
            continue
        if normalized == "merge" and index + 1 < len(tokens) and tokens[index + 1].normalized == "into":
            index = consume_single_relation(tokens, index + 2, add_relation)
            continue
        if normalized == "from":
            index = consume_relation_list(tokens, index + 1, add_relation)
            continue
        if normalized in {"join", "update", "into", "table"}:
            index = consume_single_relation(tokens, index + 1, add_relation)
            continue
        index += 1

    return relations


def consume_relation_list(
    tokens: list[SqlToken],
    index: int,
    add_relation,
) -> int:
    current = index
    while current < len(tokens):
        current = consume_single_relation(tokens, current, add_relation)
        if current < len(tokens) and tokens[current].value == ",":
            current += 1
            continue
        return current
    return current


def consume_single_relation(
    tokens: list[SqlToken],
    index: int,
    add_relation,
) -> int:
    current = skip_source_modifiers(tokens, index)
    if current >= len(tokens):
        return current

    token = tokens[current]
    if token.value == "(":
        current = skip_parenthesized(tokens, current)
        return skip_source_alias(tokens, current)

    if not is_identifier_token(token):
        return current + 1

    if current + 1 < len(tokens) and tokens[current + 1].value == "(":
        current = skip_parenthesized(tokens, current + 1)
        return skip_source_alias(tokens, current)

    relation_name, current = read_qualified_identifier(tokens, current)
    if relation_name:
        add_relation(relation_name)
    return skip_source_alias(tokens, current)


def skip_source_modifiers(tokens: list[SqlToken], index: int) -> int:
    current = index
    while current < len(tokens):
        token = tokens[current]
        if token.kind == "word" and token.normalized in SOURCE_ENTRY_MODIFIERS:
            current += 1
            continue
        return current
    return current


def skip_source_alias(tokens: list[SqlToken], index: int) -> int:
    current = index
    while current < len(tokens):
        token = tokens[current]
        if token.value == ",":
            return current
        if token.kind == "word" and (
            token.normalized in SOURCE_CLAUSE_TERMINATORS or token.normalized in JOIN_KEYWORDS
        ):
            return current
        if token.kind == "word" and token.normalized == "as":
            current += 1
            if current < len(tokens) and is_identifier_token(tokens[current]):
                current += 1
                if current < len(tokens) and tokens[current].value == "(":
                    current = skip_parenthesized(tokens, current)
            continue
        if is_identifier_token(token):
            current += 1
            if current < len(tokens) and tokens[current].value == "(":
                current = skip_parenthesized(tokens, current)
            continue
        return current
    return current


def read_qualified_identifier(tokens: list[SqlToken], index: int) -> tuple[str, int]:
    parts: list[str] = []
    current = index
    while current < len(tokens):
        token = tokens[current]
        if not is_identifier_token(token):
            break
        parts.append(token.value)
        current += 1
        if current < len(tokens) and tokens[current].value == ".":
            current += 1
            continue
        break
    return ".".join(part.strip() for part in parts if part.strip()), current


def skip_parenthesized(tokens: list[SqlToken], index: int) -> int:
    if index >= len(tokens) or tokens[index].value != "(":
        return index
    depth = 0
    current = index
    while current < len(tokens):
        token = tokens[current]
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth -= 1
            if depth == 0:
                return current + 1
        current += 1
    return current


def extract_direct_s3_paths(tokens: list[SqlToken]) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    index = 0

    while index + 1 < len(tokens):
        token = tokens[index]
        if (
            token.kind == "word"
            and token.normalized in TABLE_FUNCTION_NAMES
            and tokens[index + 1].value == "("
        ):
            end_index = skip_parenthesized(tokens, index + 1)
            for argument in tokens[index + 2 : max(index + 2, end_index - 1)]:
                if argument.kind != "string":
                    continue
                value = argument.value.strip()
                if not value.lower().startswith("s3://"):
                    continue
                normalized = value.lower()
                if normalized in seen:
                    continue
                seen.add(normalized)
                paths.append(value)
            index = end_index
            continue
        index += 1

    return paths


def s3_bucket_from_path(path: str) -> str:
    parsed = urlparse(str(path or "").strip())
    if parsed.scheme.lower() != "s3" or not parsed.netloc:
        return ""
    return parsed.netloc


def is_identifier_token(token: SqlToken) -> bool:
    return token.kind in {"word", "identifier"}
