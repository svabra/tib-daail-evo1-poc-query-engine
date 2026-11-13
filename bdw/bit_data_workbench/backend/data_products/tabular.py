"""Trusted tabular product sources and optional, owner-published product scope."""

from __future__ import annotations

import uuid
from datetime import date

from ..sql_utils import sql_literal


def product_scope(product) -> dict:
    properties = product.custom_properties
    scope = {}
    window_type = properties.get("basisWindowType", "fixed")
    rolling_size = properties.get("rollingWindowSize")
    rolling_unit = properties.get("rollingWindowUnit")
    if window_type not in {"fixed", "rolling"}:
        raise ValueError("Unknown data-basis window type.")
    if window_type == "rolling":
        if any(properties.get(key) for key in ("basisFrom", "basisUntil", "cantonCode", "taxYear")):
            raise ValueError("A rolling window cannot also contain a fixed or legacy scope.")
        try:
            size = int(str(rolling_size))
        except (TypeError, ValueError) as exc:
            raise ValueError("A rolling window needs a positive integer size.") from exc
        if not 1 <= size <= 10000 or rolling_unit not in {"days", "weeks", "months", "years"}:
            raise ValueError("Invalid rolling window size or unit.")
        scope.update(basisWindowType="rolling", rollingWindowSize=size, rollingWindowUnit=rolling_unit)
    elif rolling_size is not None or rolling_unit is not None:
        raise ValueError("A fixed window cannot contain rolling parameters.")
    if properties.get("cantonCode") or properties.get("taxYear"):
        canton = str(properties.get("cantonCode", ""))
        try:
            year = int(properties.get("taxYear", ""))
        except (TypeError, ValueError) as exc:
            raise ValueError("A scoped product needs its canton and tax year.") from exc
        if (
            canton
            not in "ZH BE LU UR SZ OW NW GL ZG FR SO BS BL SH AR AI SG GR AG TG TI VD VS NE GE JU".split()
            or not 1900 <= year <= 2100
        ):
            raise ValueError("Invalid product canton or tax year.")
        scope.update(cantonCode=canton, taxYear=year)
    if properties.get("basisFrom") or properties.get("basisUntil"):
        start = date.fromisoformat(properties.get("basisFrom", ""))
        end = date.fromisoformat(properties.get("basisUntil", ""))
        if start > end:
            raise ValueError("The data-basis end date precedes its start date.")
        scope.update(basisFrom=start.isoformat(), basisUntil=end.isoformat())
        if "basisWindowType" in properties:
            scope["basisWindowType"] = "fixed"
    if properties.get("creationRequestId"):
        scope["creationRequestId"] = str(uuid.UUID(properties["creationRequestId"]))
    return scope


def source_sql(product) -> str:
    source = product.source
    if source.source_kind != "relation":
        raise ValueError("Downloads require a tabular data product.")
    if source.bucket and source.key and source.key.lower().endswith(".parquet"):
        return f"read_parquet({sql_literal(f's3://{source.bucket}/{source.key}')})"
    if source.relation:
        from .manager import relation_identifier

        return relation_identifier(source.relation)
    raise ValueError("No tabular product source is configured.")


def select_sql(product, *, scoped=True) -> str:
    query = f"SELECT * FROM {source_sql(product)}"
    scope = product_scope(product) if scoped else {}
    if "cantonCode" in scope:
        query += f" WHERE canton_code = {sql_literal(scope['cantonCode'])} AND tax_year = {scope['taxYear']}"
    return query


def validate_scope(connection, product) -> None:
    scope = product_scope(product)
    if "cantonCode" not in scope:
        return
    rows, invalid = connection.execute(
        f"SELECT COUNT(*), COUNT(*) FILTER (WHERE canton_code IS NULL OR tax_year IS NULL "
        f"OR canton_code <> {sql_literal(scope['cantonCode'])} OR tax_year <> {scope['taxYear']}) "
        f"FROM {source_sql(product)}"
    ).fetchone()
    if not rows or invalid:
        raise ValueError(
            "Der Ergebnisbestand muss ausschliesslich Firmenzeilen des beantragten Kantons und Jahres enthalten."
        )
