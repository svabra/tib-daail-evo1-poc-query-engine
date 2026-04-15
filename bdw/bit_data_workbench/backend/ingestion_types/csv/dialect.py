from __future__ import annotations

from collections.abc import Mapping


CSV_S3_METADATA_HAS_HEADER_KEY = "bdw_csv_has_header"
CSV_S3_METADATA_DELIMITER_KEY = "bdw_csv_delimiter"

_DELIMITER_TO_TOKEN = {
    ",": "comma",
    ";": "semicolon",
    "\t": "tab",
    "|": "pipe",
}
_TOKEN_TO_DELIMITER = {value: key for key, value in _DELIMITER_TO_TOKEN.items()}


def normalize_csv_delimiter(value: str) -> str:
    normalized_value = str(value or "")
    if normalized_value in _DELIMITER_TO_TOKEN:
        return normalized_value
    return ""


def csv_s3_metadata(
    *,
    delimiter: str = "",
    has_header: bool = True,
) -> dict[str, str]:
    normalized_delimiter = normalize_csv_delimiter(delimiter)
    metadata = {
        CSV_S3_METADATA_HAS_HEADER_KEY: "true" if has_header else "false",
    }
    if normalized_delimiter:
        metadata[CSV_S3_METADATA_DELIMITER_KEY] = _DELIMITER_TO_TOKEN[normalized_delimiter]
    return metadata


def csv_read_settings_from_s3_metadata(
    metadata: Mapping[str, object] | None,
) -> tuple[str, bool | None]:
    if not metadata:
        return "", None

    normalized_metadata = {
        str(key or "").strip().lower(): str(value or "").strip().lower()
        for key, value in metadata.items()
        if str(key or "").strip()
    }

    delimiter = _TOKEN_TO_DELIMITER.get(
        normalized_metadata.get(CSV_S3_METADATA_DELIMITER_KEY, ""),
        "",
    )

    has_header_text = normalized_metadata.get(CSV_S3_METADATA_HAS_HEADER_KEY, "")
    if has_header_text == "true":
        return delimiter, True
    if has_header_text == "false":
        return delimiter, False
    return delimiter, None
