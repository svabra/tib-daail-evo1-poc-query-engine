from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

from .formats import normalize_export_format


_CSV_DELIMITER_TOKENS = {
    ",": ",",
    "comma": ",",
    ";": ";",
    "semicolon": ";",
    "\t": "\t",
    "tab": "\t",
    "|": "|",
    "pipe": "|",
}
_XML_NAME_SANITIZE_PATTERN = re.compile(r"[^a-zA-Z0-9_.-]+")
_XML_NAME_INVALID_START_PATTERN = re.compile(r"^[^a-zA-Z_]+")
_EXCEL_SHEET_INVALID_PATTERN = re.compile(r"[:\\/?*\[\]]+")


@dataclass(slots=True)
class QueryResultExportOptions:
    csv_delimiter: str = ","
    csv_include_header: bool = True
    excel_sheet_name: str = "Results"
    excel_include_header: bool = True
    excel_freeze_header: bool = True
    xml_root_name: str = "results"
    xml_row_name: str = "row"
    xml_pretty_print: bool = True

    def payload_for_format(self, export_format: str) -> dict[str, Any]:
        normalized_format = normalize_export_format(export_format)
        if normalized_format == "csv":
            return {
                "delimiter": self.csv_delimiter,
                "includeHeader": self.csv_include_header,
            }
        if normalized_format == "xlsx":
            return {
                "sheetName": self.excel_sheet_name,
                "includeHeader": self.excel_include_header,
                "freezeHeader": self.excel_freeze_header,
            }
        if normalized_format == "xml":
            return {
                "rootName": self.xml_root_name,
                "rowName": self.xml_row_name,
                "prettyPrint": self.xml_pretty_print,
            }
        return {}


def _coerce_bool(value: object, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"true", "1", "yes", "on"}:
        return True
    if normalized in {"false", "0", "no", "off"}:
        return False
    return default


def normalize_csv_delimiter(value: object, default: str = ",") -> str:
    normalized = _CSV_DELIMITER_TOKENS.get(str(value or "").strip().lower(), "")
    return normalized or default


def normalize_excel_sheet_name(value: object, default: str = "Results") -> str:
    normalized = _EXCEL_SHEET_INVALID_PATTERN.sub(" ", str(value or "").strip())
    normalized = re.sub(r"\s{2,}", " ", normalized).strip().strip("'")
    normalized = normalized[:31].strip()
    return normalized or default


def normalize_xml_name(value: object, default: str) -> str:
    normalized = _XML_NAME_SANITIZE_PATTERN.sub("_", str(value or "").strip())
    normalized = normalized.strip("._-")
    if not normalized:
        return default
    if _XML_NAME_INVALID_START_PATTERN.match(normalized):
        normalized = f"n_{normalized}"
    return normalized


def normalize_export_settings(
    export_format: str,
    raw_settings: Mapping[str, object] | None = None,
) -> QueryResultExportOptions:
    normalized_format = normalize_export_format(export_format)
    settings = raw_settings if isinstance(raw_settings, Mapping) else {}
    options = QueryResultExportOptions()

    if normalized_format == "csv":
        options.csv_delimiter = normalize_csv_delimiter(settings.get("delimiter"), ",")
        options.csv_include_header = _coerce_bool(settings.get("includeHeader"), True)
        return options

    if normalized_format == "xlsx":
        options.excel_sheet_name = normalize_excel_sheet_name(settings.get("sheetName"), "Results")
        options.excel_include_header = _coerce_bool(settings.get("includeHeader"), True)
        options.excel_freeze_header = _coerce_bool(settings.get("freezeHeader"), True)
        return options

    if normalized_format == "xml":
        options.xml_root_name = normalize_xml_name(settings.get("rootName"), "results")
        options.xml_row_name = normalize_xml_name(settings.get("rowName"), "row")
        options.xml_pretty_print = _coerce_bool(settings.get("prettyPrint"), True)
        return options

    return options
