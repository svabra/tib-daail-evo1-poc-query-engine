from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from .query_analysis import (
    KnownRelationReference,
    consume_single_relation,
    extract_cte_names,
    is_identifier_token,
    normalize_relation_key,
    read_qualified_identifier,
    skip_parenthesized,
    skip_source_alias,
    skip_source_modifiers,
    tokenize_sql,
)


QUERY_SOURCE_VALID = "valid"
QUERY_SOURCE_INVALID = "invalid"
QUERY_SOURCE_UNCHECKED = "unchecked"
LOCAL_WORKSPACE_RELATION_PREFIX = "workspace.local.saved_results."
LOCAL_WORKSPACE_PHYSICAL_SCHEMA_PREFIX = "workspace_local_"


@dataclass(frozen=True, slots=True)
class QuerySourceValidationReference:
    reference: str
    matched_relation: str = ""

    @property
    def payload(self) -> dict[str, str]:
        return {
            "reference": self.reference,
            "matchedRelation": self.matched_relation,
        }


@dataclass(frozen=True, slots=True)
class QuerySourceValidationResult:
    status: str
    references: list[str] = field(default_factory=list)
    matched_references: list[QuerySourceValidationReference] = field(default_factory=list)
    missing_references: list[str] = field(default_factory=list)
    message: str = ""

    @property
    def payload(self) -> dict[str, object]:
        return {
            "status": self.status,
            "canRun": self.status != QUERY_SOURCE_INVALID,
            "references": list(self.references),
            "matchedReferences": [reference.payload for reference in self.matched_references],
            "missingReferences": list(self.missing_references),
            "message": self.message,
        }


def extract_select_source_references(sql: str) -> list[str]:
    tokens = tokenize_sql(sql)
    if not tokens:
        return []

    cte_names = extract_cte_names(tokens)
    relations: list[str] = []
    seen: set[str] = set()
    index = 0

    def add_relation(value: str) -> None:
        normalized = normalize_relation_key(value)
        if (
            not normalized
            or normalized in cte_names
            or normalized in seen
            or normalized.startswith(LOCAL_WORKSPACE_RELATION_PREFIX)
            or normalized.startswith(LOCAL_WORKSPACE_PHYSICAL_SCHEMA_PREFIX)
        ):
            return
        seen.add(normalized)
        relations.append(value)

    while index < len(tokens):
        token = tokens[index]
        if token.kind != "word":
            index += 1
            continue

        normalized = token.normalized
        if normalized == "from":
            index = consume_select_relation_list(tokens, index + 1, add_relation)
            continue
        if normalized == "join":
            index = consume_select_relation(tokens, index + 1, add_relation)
            continue
        if normalized == "table":
            next_index = consume_table_expression_relation(tokens, index + 1, add_relation)
            if next_index != index + 1:
                index = next_index
                continue
        index += 1

    return relations


def consume_select_relation_list(tokens, index: int, add_relation) -> int:
    current = index
    while current < len(tokens):
        current = consume_select_relation(tokens, current, add_relation)
        if current < len(tokens) and tokens[current].value == ",":
            current += 1
            continue
        return current
    return current


def consume_select_relation(tokens, index: int, add_relation) -> int:
    current = skip_source_modifiers(tokens, index)
    if (
        current + 1 < len(tokens)
        and tokens[current].kind == "word"
        and tokens[current].normalized == "table"
        and tokens[current + 1].value == "("
    ):
        return consume_table_expression_relation(tokens, current + 1, add_relation)
    return consume_single_relation(tokens, index, add_relation)


def consume_table_expression_relation(tokens, index: int, add_relation) -> int:
    if index >= len(tokens) or tokens[index].value != "(":
        return index

    current = index + 1
    if current >= len(tokens):
        return skip_parenthesized(tokens, index)

    token = tokens[current]
    if not is_identifier_token(token):
        return skip_source_alias(tokens, skip_parenthesized(tokens, index))

    if current + 1 < len(tokens) and tokens[current + 1].value == "(":
        return skip_source_alias(tokens, skip_parenthesized(tokens, index))

    relation_name, current = read_qualified_identifier(tokens, current)
    if relation_name:
        add_relation(relation_name)
    return skip_source_alias(tokens, skip_parenthesized(tokens, index))


def validate_query_sources(
    sql: str,
    *,
    relation_index: dict[str, KnownRelationReference],
    data_sources: Iterable[str] | None = None,
) -> QuerySourceValidationResult:
    del data_sources

    references = extract_select_source_references(sql)
    if not references:
        return QuerySourceValidationResult(
            status=QUERY_SOURCE_UNCHECKED,
            references=[],
            message="No source references found.",
        )

    matched: list[QuerySourceValidationReference] = []
    missing: list[str] = []
    for reference in references:
        known_relation = relation_index.get(normalize_relation_key(reference))
        if known_relation is None:
            missing.append(reference)
            continue
        matched.append(
            QuerySourceValidationReference(
                reference=reference,
                matched_relation=known_relation.relation,
            )
        )

    if missing:
        return QuerySourceValidationResult(
            status=QUERY_SOURCE_INVALID,
            references=references,
            matched_references=matched,
            missing_references=missing,
            message=f"Referenced source(s) were not found: {', '.join(missing)}.",
        )

    return QuerySourceValidationResult(
        status=QUERY_SOURCE_VALID,
        references=references,
        matched_references=matched,
        message="All referenced sources exist.",
    )
