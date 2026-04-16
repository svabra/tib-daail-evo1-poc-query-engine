from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from .tabular_rows import row_payload


StreamRows = Callable[[Callable[[list[str], list[tuple[Any, ...]]], None]], None]


def write_json(local_path: Path, stream_rows: StreamRows) -> None:
    with local_path.open("w", encoding="utf-8", newline="") as handle:
        handle.write("[\n")
        first_row = True

        def consume(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
            nonlocal first_row
            for row in rows:
                payload = row_payload(columns, row)
                if not first_row:
                    handle.write(",\n")
                handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
                first_row = False

        stream_rows(consume)
        handle.write("\n]")


def write_jsonl(local_path: Path, stream_rows: StreamRows) -> None:
    with local_path.open("w", encoding="utf-8", newline="") as handle:

        def consume(columns: list[str], rows: list[tuple[Any, ...]]) -> None:
            for row in rows:
                payload = row_payload(columns, row)
                handle.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
                handle.write("\n")

        stream_rows(consume)
