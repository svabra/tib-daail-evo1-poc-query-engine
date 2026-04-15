from __future__ import annotations

import csv
from pathlib import Path

from .dialect import normalize_csv_delimiter


SUPPORTED_AUTO_DELIMITERS = (",", ";", "\t", "|")


def _count_delimiter_outside_quotes(line: str, delimiter: str) -> int:
    count = 0
    in_quotes = False
    index = 0
    while index < len(line):
        character = line[index]
        if character == '"':
            if in_quotes and index + 1 < len(line) and line[index + 1] == '"':
                index += 2
                continue
            in_quotes = not in_quotes
        elif not in_quotes and character == delimiter:
            count += 1
        index += 1

    return count


def detect_csv_delimiter(local_path: Path) -> str:
    try:
        with local_path.open("r", encoding="utf-8-sig", newline="") as handle:
            sample_lines = [
                line.strip()
                for line in handle
                if line.strip()
            ][:8]
    except UnicodeDecodeError as exc:
        raise ValueError("The CSV file must be UTF-8 encoded.") from exc

    if not sample_lines:
        return ","

    best_delimiter = ","
    best_score = float("-inf")
    for index, candidate in enumerate(SUPPORTED_AUTO_DELIMITERS):
        counts = [_count_delimiter_outside_quotes(line, candidate) for line in sample_lines]
        non_zero_counts = [count for count in counts if count > 0]
        if not non_zero_counts:
            continue
        average_count = sum(non_zero_counts) / len(non_zero_counts)
        spread = max(non_zero_counts) - min(non_zero_counts)
        score = average_count * 10 - spread - index * 0.01
        if score > best_score:
            best_score = score
            best_delimiter = candidate
    return best_delimiter


def resolve_csv_delimiter(local_path: Path, delimiter: str = "") -> str:
    normalized_delimiter = normalize_csv_delimiter(delimiter)
    if normalized_delimiter:
        return normalized_delimiter
    return detect_csv_delimiter(local_path)


def validate_csv_file(local_path: Path, *, delimiter: str = "", has_header: bool = True) -> str:
    resolved_delimiter = resolve_csv_delimiter(local_path, delimiter)

    try:
        with local_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.reader(handle, delimiter=resolved_delimiter, quotechar='"', doublequote=True)
            expected_width: int | None = None
            saw_any_rows = False
            for line_number, row in enumerate(reader, start=1):
                if not row or not any(str(value or "").strip() for value in row):
                    continue

                saw_any_rows = True
                row_width = len(row)
                if expected_width is None:
                    expected_width = row_width
                    if has_header:
                        continue
                elif row_width != expected_width:
                    raise ValueError(
                        "CSV row width mismatch at line "
                        f"{line_number}: expected {expected_width} fields but found {row_width}. "
                        "This usually means the delimiter is wrong or a value contains an unquoted delimiter."
                    )
    except UnicodeDecodeError as exc:
        raise ValueError("The CSV file must be UTF-8 encoded.") from exc

    if not saw_any_rows:
        raise ValueError("The CSV file does not contain any rows.")

    return resolved_delimiter
