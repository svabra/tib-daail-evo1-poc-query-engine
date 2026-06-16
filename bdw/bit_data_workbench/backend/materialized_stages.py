from __future__ import annotations

import hashlib
import json
import re
import shutil
import tempfile
import threading
import uuid
from collections import deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError

from ..config import Settings
from .query_aliases import normalize_query_alias_segment
from .query_options import normalize_query_options
from .source_references import s3_source_reference, s3_table_function_sql
from .s3_storage import s3_client
from .sql_utils import qualified_name, sql_literal


STAGE_ROOT_PREFIX = "_bdw_stages"
STAGE_SCHEMA_NAME = "stage"
STAGE_OUTPUT_FILE_EXTENSION = ".parquet"
TERMINAL_STAGE_STATUSES = {"completed", "failed", "cancelled", "skipped"}
VALID_MATERIALIZED_STATUS = "valid"
STAGE_REF_RE = re.compile(r"(?<![A-Za-z0-9_$])stage\.([A-Za-z_][A-Za-z0-9_$]*)", re.IGNORECASE)
QUERY_START_RE = re.compile(r"^(?:select|with|from|values)\b", re.IGNORECASE)
DUCKDB_TABLE_FUNCTION_RE = re.compile(
    r"^(?:parquet_scan|read_(?:parquet|csv|csv_auto|json|json_auto|ndjson|ndjson_auto|xlsx))\s*\(",
    re.IGNORECASE,
)


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _sha256_text(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _clean_stage_kind(value: object) -> str:
    normalized = str(value or "").strip().lower()
    return "final" if normalized == "final" else "intermediate"


def _clean_stage_id(value: object, *, cell_id: str, index: int) -> str:
    normalized = str(value or "").strip()
    if normalized:
        return normalized
    seed = normalize_query_alias_segment(cell_id or f"cell-{index + 1}", fallback="cell")
    return f"stage-{seed}"


def _clean_stage_alias(value: object, *, title: str, cell_id: str, index: int) -> str:
    fallback = title or cell_id or f"stage-{index + 1}"
    return normalize_query_alias_segment(str(value or "").strip() or fallback, fallback="stage")


def _clean_stage_title(value: object, *, alias: str, index: int) -> str:
    title = str(value or "").strip()
    if title:
        return title
    return alias.replace("_", " ").title() or f"Stage {index + 1}"


def _unique_alias(base_alias: str, used_aliases: set[str]) -> str:
    candidate = base_alias
    suffix = 2
    while candidate.lower() in used_aliases:
        candidate = f"{base_alias}_{suffix}"
        suffix += 1
    used_aliases.add(candidate.lower())
    return candidate


def sql_stage_alias_references(sql: str) -> list[str]:
    seen: set[str] = set()
    aliases: list[str] = []
    for match in STAGE_REF_RE.finditer(str(sql or "")):
        alias = normalize_query_alias_segment(match.group(1), fallback="stage")
        normalized = alias.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        aliases.append(alias)
    return aliases


def notebook_slug(notebook_id: str, notebook_title: str = "") -> str:
    source = str(notebook_id or "").strip() or str(notebook_title or "").strip()
    return normalize_query_alias_segment(source, fallback="notebook")


def recommended_stage_output_file_name(alias: object) -> str:
    stem = normalize_query_alias_segment(str(alias or "").strip(), fallback="stage")
    return f"{stem}{STAGE_OUTPUT_FILE_EXTENSION}"


def normalize_stage_output_file_name(value: object, *, alias: object) -> str:
    text = str(value or "").strip().replace("\\", "/")
    name = text.rsplit("/", 1)[-1].strip()
    if not name:
        return recommended_stage_output_file_name(alias)
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    name = re.sub(r"_+", "_", name).strip("._-")
    if not name or name in {".", ".."}:
        return recommended_stage_output_file_name(alias)
    if not name.lower().endswith(STAGE_OUTPUT_FILE_EXTENSION):
        name = f"{name}{STAGE_OUTPUT_FILE_EXTENSION}"
    return name


def materialized_stage_query_sql(sql: object) -> str:
    normalized = str(sql or "").strip()
    while normalized.endswith(";"):
        normalized = normalized[:-1].rstrip()
    if not normalized or QUERY_START_RE.match(normalized):
        return normalized
    if DUCKDB_TABLE_FUNCTION_RE.match(normalized):
        return f"SELECT * FROM {normalized}"
    return normalized


@dataclass(slots=True)
class StageRecord:
    run_id: str
    notebook_id: str
    stage_id: str
    cell_id: str
    stage_alias: str
    stage_title: str
    status: str
    revision_id: str = ""
    sql_hash: str = ""
    predecessor_revision_ids: list[str] = field(default_factory=list)
    schema_fingerprint: str = ""
    row_count: int = 0
    size_bytes: int = 0
    result_fingerprint: str = ""
    output_bucket: str = ""
    output_key: str = ""
    metadata_key: str = ""
    output_path: str = ""
    output_file_name: str = ""
    query_path: str = ""
    query_reference: str = ""
    query_sql: str = ""
    query_job_id: str = ""
    started_at: str = ""
    completed_at: str = ""
    updated_at: str = ""
    message: str = ""
    error: str = ""
    changed_result: bool = False
    can_cancel: bool = False

    @property
    def duration_ms(self) -> int | None:
        started = _parse_iso_datetime(self.started_at)
        if started is None:
            return None
        completed = _parse_iso_datetime(self.completed_at)
        if completed is None and self.status in {"running", "queued"}:
            completed = datetime.now(UTC)
        if completed is None:
            return None
        return max(0, round((completed - started).total_seconds() * 1000))

    @property
    def payload(self) -> dict[str, object]:
        return {
            "runId": self.run_id,
            "notebookId": self.notebook_id,
            "stageId": self.stage_id,
            "cellId": self.cell_id,
            "stageAlias": self.stage_alias,
            "stageTitle": self.stage_title,
            "status": self.status,
            "revisionId": self.revision_id,
            "sqlHash": self.sql_hash,
            "predecessorRevisionIds": list(self.predecessor_revision_ids),
            "schemaFingerprint": self.schema_fingerprint,
            "rowCount": self.row_count,
            "sizeBytes": self.size_bytes,
            "resultFingerprint": self.result_fingerprint,
            "outputBucket": self.output_bucket,
            "outputKey": self.output_key,
            "metadataKey": self.metadata_key,
            "outputPath": self.output_path,
            "outputFileName": self.output_file_name,
            "queryPath": self.query_path,
            "queryReference": self.query_reference or self.query_path,
            "querySql": self.query_sql,
            "queryJobId": self.query_job_id,
            "startedAt": self.started_at,
            "completedAt": self.completed_at,
            "updatedAt": self.updated_at,
            "message": self.message,
            "error": self.error,
            "changedResult": self.changed_result,
            "canCancel": self.can_cancel,
            "durationMs": self.duration_ms,
        }

    @classmethod
    def from_payload(cls, payload: object) -> "StageRecord | None":
        if not isinstance(payload, dict):
            return None
        run_id = str(payload.get("runId") or "").strip()
        notebook_id = str(payload.get("notebookId") or "").strip()
        stage_id = str(payload.get("stageId") or "").strip()
        cell_id = str(payload.get("cellId") or "").strip()
        if not run_id or not notebook_id or not stage_id:
            return None
        output_bucket = str(payload.get("outputBucket") or "").strip()
        output_key = str(payload.get("outputKey") or "").strip()
        output_file_name = str(payload.get("outputFileName") or "").strip()
        if not output_file_name and output_key:
            output_file_name = output_key.rsplit("/", 1)[-1].strip()
        query_reference = str(payload.get("queryReference") or "").strip()
        if not query_reference and output_bucket and output_key:
            query_reference = s3_source_reference(bucket=output_bucket, key=output_key)
        query_path = str(payload.get("queryPath") or "").strip() or query_reference
        query_sql = str(payload.get("querySql") or "").strip()
        if not query_sql and output_bucket and output_key:
            query_sql = s3_table_function_sql(
                bucket=output_bucket,
                key=output_key,
                file_format="parquet",
            )
        return cls(
            run_id=run_id,
            notebook_id=notebook_id,
            stage_id=stage_id,
            cell_id=cell_id,
            stage_alias=str(payload.get("stageAlias") or "").strip(),
            stage_title=str(payload.get("stageTitle") or "").strip(),
            status=str(payload.get("status") or "").strip() or "planned",
            revision_id=str(payload.get("revisionId") or "").strip(),
            sql_hash=str(payload.get("sqlHash") or "").strip(),
            predecessor_revision_ids=[
                str(item).strip()
                for item in payload.get("predecessorRevisionIds", []) or []
                if str(item).strip()
            ],
            schema_fingerprint=str(payload.get("schemaFingerprint") or "").strip(),
            row_count=max(0, int(payload.get("rowCount") or 0)),
            size_bytes=max(0, int(payload.get("sizeBytes") or 0)),
            result_fingerprint=str(payload.get("resultFingerprint") or "").strip(),
            output_bucket=output_bucket,
            output_key=output_key,
            metadata_key=str(payload.get("metadataKey") or "").strip(),
            output_path=str(payload.get("outputPath") or "").strip(),
            output_file_name=output_file_name,
            query_path=query_path,
            query_reference=query_reference,
            query_sql=query_sql,
            query_job_id=str(payload.get("queryJobId") or "").strip(),
            started_at=str(payload.get("startedAt") or "").strip(),
            completed_at=str(payload.get("completedAt") or "").strip(),
            updated_at=str(payload.get("updatedAt") or "").strip(),
            message=str(payload.get("message") or "").strip(),
            error=str(payload.get("error") or "").strip(),
            changed_result=bool(payload.get("changedResult")),
            can_cancel=bool(payload.get("canCancel")),
        )


class MaterializedStageStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.RLock()

    def read_state(self) -> dict[str, object]:
        with self._lock:
            if not self._path.exists():
                return {"version": 0, "records": [], "stageStates": {}}
            try:
                raw = json.loads(self._path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return {"version": 0, "records": [], "stageStates": {}}
            if not isinstance(raw, dict):
                return {"version": 0, "records": [], "stageStates": {}}
            records = raw.get("records")
            stage_states = raw.get("stageStates")
            return {
                "version": max(0, int(raw.get("version") or 0)),
                "records": records if isinstance(records, list) else [],
                "stageStates": stage_states if isinstance(stage_states, dict) else {},
            }

    def write_state(self, state: dict[str, object]) -> dict[str, object]:
        with self._lock:
            version = int(state.get("version") or 0) + 1
            next_state = {
                "version": version,
                "records": list(state.get("records", []) or []),
                "stageStates": dict(state.get("stageStates", {}) or {}),
                "updatedAt": utc_now_iso(),
            }
            self._path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
            temp_path.write_text(json.dumps(next_state, indent=2), encoding="utf-8")
            temp_path.replace(self._path)
            return next_state


def normalize_stage_cells(cells: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    stages: list[dict[str, object]] = []
    used_aliases: set[str] = set()
    for index, raw_cell in enumerate(cells or []):
        if not isinstance(raw_cell, dict):
            continue
        language = str(raw_cell.get("language") or "sql").strip().lower()
        if language == "python":
            continue
        cell_id = str(raw_cell.get("cellId") or raw_cell.get("cell_id") or "").strip()
        if not cell_id:
            cell_id = f"cell-{index + 1}"
        raw_stage = raw_cell.get("stage")
        stage_meta = raw_stage if isinstance(raw_stage, dict) else {}
        if stage_meta.get("enabled") is False:
            continue
        stage_id = _clean_stage_id(stage_meta.get("stageId"), cell_id=cell_id, index=index)
        base_alias = _clean_stage_alias(
            stage_meta.get("alias"),
            title=str(stage_meta.get("title") or "").strip(),
            cell_id=cell_id,
            index=index,
        )
        alias = _unique_alias(base_alias, used_aliases)
        title = _clean_stage_title(stage_meta.get("title"), alias=alias, index=index)
        raw_output_file_name = (
            stage_meta.get("outputFileName")
            if "outputFileName" in stage_meta
            else stage_meta.get("output_file_name")
        )
        output_file_name = (
            normalize_stage_output_file_name(raw_output_file_name, alias=alias)
            if str(raw_output_file_name or "").strip()
            else ""
        )
        recommended_output_file_name = recommended_stage_output_file_name(alias)
        predecessors = [
            str(item).strip()
            for item in stage_meta.get("predecessorStageIds", []) or []
            if str(item).strip()
        ]
        stages.append(
            {
                "stageId": stage_id,
                "cellId": cell_id,
                "cellIndex": index,
                "alias": alias,
                "title": title,
                "description": str(stage_meta.get("description") or "").strip(),
                "kind": _clean_stage_kind(stage_meta.get("kind")),
                "materialize": stage_meta.get("materialize") is not False,
                "outputFileName": output_file_name,
                "recommendedOutputFileName": recommended_output_file_name,
                "resolvedOutputFileName": output_file_name or recommended_output_file_name,
                "predecessorStageIds": predecessors,
                "sql": str(raw_cell.get("sql") or ""),
                "dataSources": [
                    str(item).strip()
                    for item in raw_cell.get("dataSources", []) or []
                    if str(item).strip()
                ],
                "queryOptions": (
                    normalize_query_options(raw_cell.get("queryOptions") or {})
                    if isinstance(raw_cell.get("queryOptions"), dict)
                    else normalize_query_options({})
                ),
            }
        )
    return stages


def _records_from_state(state: dict[str, object]) -> list[StageRecord]:
    records: list[StageRecord] = []
    for item in state.get("records", []) or []:
        record = StageRecord.from_payload(item)
        if record is not None:
            records.append(record)
    return records


def _latest_completed_by_stage(records: Iterable[StageRecord]) -> dict[tuple[str, str], StageRecord]:
    latest: dict[tuple[str, str], StageRecord] = {}
    for record in records:
        if record.status != "completed":
            continue
        key = (record.notebook_id, record.stage_id)
        current = latest.get(key)
        if current is None or (record.completed_at or record.updated_at) > (current.completed_at or current.updated_at):
            latest[key] = record
    return latest


def _latest_by_stage(records: Iterable[StageRecord]) -> dict[tuple[str, str], StageRecord]:
    latest: dict[tuple[str, str], StageRecord] = {}
    for record in records:
        key = (record.notebook_id, record.stage_id)
        current = latest.get(key)
        if current is None or (record.updated_at or record.started_at) > (current.updated_at or current.started_at):
            latest[key] = record
    return latest


def _notebook_stage_states(state: dict[str, object], notebook_id: str) -> dict[str, dict[str, object]]:
    raw_stage_states = state.get("stageStates")
    if not isinstance(raw_stage_states, dict):
        return {}
    raw_notebook = raw_stage_states.get(notebook_id)
    if not isinstance(raw_notebook, dict):
        return {}
    return {
        str(stage_id): dict(stage_state)
        for stage_id, stage_state in raw_notebook.items()
        if isinstance(stage_state, dict)
    }


def _stage_cell_index(stage: dict[str, object]) -> int:
    try:
        return int(stage.get("cellIndex", 0))
    except (TypeError, ValueError):
        return 0


def _pipeline_path_id(terminal_stage_id: str) -> str:
    return f"path-{terminal_stage_id}"


def normalize_pipeline_paths(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, object]] = []
    seen_keys: set[str] = set()
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            continue
        terminal_stage_id = str(
            item.get("terminalStageId")
            or item.get("terminal_stage_id")
            or ""
        ).strip()
        path_id = str(item.get("pathId") or item.get("path_id") or "").strip()
        if not terminal_stage_id and path_id.startswith("path-"):
            terminal_stage_id = path_id[5:]
        if not path_id and terminal_stage_id:
            path_id = _pipeline_path_id(terminal_stage_id)
        if not terminal_stage_id and not path_id:
            continue
        key = terminal_stage_id or path_id
        if key in seen_keys:
            continue
        seen_keys.add(key)
        label = str(item.get("label") or item.get("name") or "").strip()
        try:
            priority = int(item.get("priority") or item.get("rank") or index + 1)
        except (TypeError, ValueError):
            priority = index + 1
        normalized.append(
            {
                "pathId": path_id,
                "terminalStageId": terminal_stage_id,
                "label": label,
                "priority": max(1, priority),
                "_index": index,
            }
        )
    normalized.sort(key=lambda item: (int(item.get("priority") or 0), int(item.get("_index") or 0)))
    for item in normalized:
        item.pop("_index", None)
    return normalized


def _topological_stage_order(
    *,
    indegree: dict[str, int],
    successors: dict[str, list[str]],
    by_id: dict[str, dict[str, object]],
    stage_priority_rank: dict[str, int] | None = None,
) -> tuple[list[str], list[str]]:
    remaining = dict(indegree)
    priority_rank = stage_priority_rank or {}

    def sort_key(stage_id: str) -> tuple[int, int, str]:
        return (
            int(priority_rank.get(stage_id, 1_000_000)),
            _stage_cell_index(by_id.get(stage_id, {})),
            stage_id,
        )

    ready = sorted(
        (stage_id for stage_id, degree in remaining.items() if degree == 0),
        key=sort_key,
    )
    ordered_stage_ids: list[str] = []
    while ready:
        stage_id = ready.pop(0)
        ordered_stage_ids.append(stage_id)
        for successor_id in sorted(successors.get(stage_id, []), key=sort_key):
            remaining[successor_id] -= 1
            if remaining[successor_id] == 0:
                ready.append(successor_id)
        ready.sort(key=sort_key)

    cycle_stage_ids = [stage_id for stage_id, degree in remaining.items() if degree > 0]
    if cycle_stage_ids:
        ordered_stage_ids.extend(
            stage_id
            for stage_id in sorted(cycle_stage_ids, key=sort_key)
            if stage_id not in ordered_stage_ids
        )
    return ordered_stage_ids, cycle_stage_ids


def _ancestor_stage_ids(
    terminal_stage_id: str,
    *,
    predecessor_map: dict[str, list[str]],
    fallback_ordered_stage_ids: list[str],
) -> list[str]:
    required = {terminal_stage_id}
    queue = deque(predecessor_map.get(terminal_stage_id, []))
    while queue:
        stage_id = queue.popleft()
        if not stage_id or stage_id in required:
            continue
        required.add(stage_id)
        queue.extend(predecessor_map.get(stage_id, []))
    ordered = [stage_id for stage_id in fallback_ordered_stage_ids if stage_id in required]
    return ordered or [terminal_stage_id]


def _computed_pipeline_paths(
    *,
    by_id: dict[str, dict[str, object]],
    successors: dict[str, list[str]],
    predecessor_map: dict[str, list[str]],
    fallback_ordered_stage_ids: list[str],
    pipeline_paths: Iterable[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    fallback_index = {stage_id: index for index, stage_id in enumerate(fallback_ordered_stage_ids)}
    terminal_stage_ids = sorted(
        [
            stage_id
            for stage_id in by_id
            if not [successor for successor in successors.get(stage_id, []) if successor in by_id]
        ],
        key=lambda stage_id: fallback_index.get(stage_id, 1_000_000),
    )
    if not terminal_stage_ids:
        return []

    known_terminals = set(terminal_stage_ids)
    metadata_by_terminal: dict[str, dict[str, object]] = {}
    priority_terminal_ids: list[str] = []
    for path in normalize_pipeline_paths(list(pipeline_paths or [])):
        terminal_stage_id = str(path.get("terminalStageId") or "").strip()
        if terminal_stage_id not in known_terminals or terminal_stage_id in metadata_by_terminal:
            continue
        metadata_by_terminal[terminal_stage_id] = path
        priority_terminal_ids.append(terminal_stage_id)

    ordered_terminal_ids = [
        *priority_terminal_ids,
        *[stage_id for stage_id in terminal_stage_ids if stage_id not in metadata_by_terminal],
    ]
    paths: list[dict[str, object]] = []
    for index, terminal_stage_id in enumerate(ordered_terminal_ids):
        stage = by_id[terminal_stage_id]
        metadata = metadata_by_terminal.get(terminal_stage_id, {})
        label = str(metadata.get("label") or stage.get("title") or stage.get("alias") or terminal_stage_id).strip()
        path_id = str(metadata.get("pathId") or _pipeline_path_id(terminal_stage_id)).strip()
        paths.append(
            {
                "pathId": path_id,
                "label": label,
                "terminalStageId": terminal_stage_id,
                "terminalStageTitle": str(stage.get("title") or stage.get("alias") or terminal_stage_id),
                "stageIds": _ancestor_stage_ids(
                    terminal_stage_id,
                    predecessor_map=predecessor_map,
                    fallback_ordered_stage_ids=fallback_ordered_stage_ids,
                ),
                "priority": index + 1,
            }
        )
    return paths


def _stage_priority_ranks(paths: Iterable[dict[str, object]]) -> dict[str, int]:
    ranks: dict[str, int] = {}
    for path in sorted(paths, key=lambda item: int(item.get("priority") or 1_000_000)):
        rank = int(path.get("priority") or 1_000_000)
        for stage_id in path.get("stageIds", []) or []:
            normalized_stage_id = str(stage_id or "").strip()
            if normalized_stage_id and normalized_stage_id not in ranks:
                ranks[normalized_stage_id] = rank
    return ranks


def build_notebook_stage_graph(
    *,
    notebook_id: str,
    notebook_title: str = "",
    cells: Iterable[dict[str, object]],
    state: dict[str, object] | None = None,
    published_products_for_source: Callable[[dict[str, object]], list[dict[str, object]]] | None = None,
    pipeline_paths: Iterable[dict[str, object]] | None = None,
) -> dict[str, object]:
    stage_cells = normalize_stage_cells(cells)
    state = state or {"version": 0, "records": [], "stageStates": {}}
    records = _records_from_state(state)
    latest = _latest_by_stage(records)
    latest_completed = _latest_completed_by_stage(records)
    stage_states = _notebook_stage_states(state, notebook_id)
    by_id = {stage["stageId"]: stage for stage in stage_cells}
    by_alias = {str(stage["alias"]).lower(): stage for stage in stage_cells}
    predecessor_map: dict[str, list[str]] = {}
    diagnostics: list[dict[str, object]] = []

    for stage in stage_cells:
        stage_id = str(stage["stageId"])
        predecessors: list[str] = []
        for predecessor_id in stage.get("predecessorStageIds", []) or []:
            normalized_id = str(predecessor_id).strip()
            if normalized_id and normalized_id not in predecessors:
                predecessors.append(normalized_id)
        for alias in sql_stage_alias_references(str(stage.get("sql") or "")):
            predecessor = by_alias.get(alias.lower())
            if predecessor is None:
                diagnostics.append(
                    {
                        "severity": "error",
                        "code": "missing-stage-reference",
                        "stageId": stage_id,
                        "message": f"SQL references missing stage alias stage.{alias}.",
                    }
                )
                continue
            predecessor_id = str(predecessor["stageId"])
            if predecessor_id != stage_id and predecessor_id not in predecessors:
                predecessors.append(predecessor_id)
        for predecessor_id in predecessors:
            if predecessor_id not in by_id:
                diagnostics.append(
                    {
                        "severity": "error",
                        "code": "missing-predecessor",
                        "stageId": stage_id,
                        "predecessorStageId": predecessor_id,
                        "message": "Configured predecessor stage is no longer present.",
                    }
                )
        predecessor_map[stage_id] = predecessors

    indegree = {stage["stageId"]: 0 for stage in stage_cells}
    successors: dict[str, list[str]] = {stage["stageId"]: [] for stage in stage_cells}
    for stage_id, predecessors in predecessor_map.items():
        for predecessor_id in predecessors:
            if predecessor_id not in indegree:
                continue
            indegree[stage_id] += 1
            successors[predecessor_id].append(stage_id)

    fallback_ordered_stage_ids, _fallback_cycle_stage_ids = _topological_stage_order(
        indegree=indegree,
        successors=successors,
        by_id=by_id,
    )
    paths = _computed_pipeline_paths(
        by_id=by_id,
        successors=successors,
        predecessor_map=predecessor_map,
        fallback_ordered_stage_ids=fallback_ordered_stage_ids,
        pipeline_paths=pipeline_paths,
    )
    ordered_stage_ids, cycle_stage_ids = _topological_stage_order(
        indegree=indegree,
        successors=successors,
        by_id=by_id,
        stage_priority_rank=_stage_priority_ranks(paths),
    )
    if cycle_stage_ids:
        diagnostics.append(
            {
                "severity": "error",
                "code": "cycle",
                "stageIds": cycle_stage_ids,
                "message": "Pipeline dependencies contain a cycle.",
            }
        )

    layer_by_stage: dict[str, int] = {}
    for stage_id in ordered_stage_ids:
        layer_by_stage[stage_id] = 0
        for predecessor_id in predecessor_map.get(stage_id, []):
            if predecessor_id in layer_by_stage:
                layer_by_stage[stage_id] = max(layer_by_stage[stage_id], layer_by_stage[predecessor_id] + 1)

    nodes: list[dict[str, object]] = []
    for stage_id in ordered_stage_ids:
        stage = by_id[stage_id]
        record = latest.get((notebook_id, stage_id))
        completed = latest_completed.get((notebook_id, stage_id))
        stage_state = stage_states.get(stage_id, {})
        has_stage_error = any(
            item.get("stageId") == stage_id and item.get("severity") == "error"
            for item in diagnostics
        )
        status = "planned"
        run_warning = ""
        if has_stage_error:
            status = "invalid"
        elif record and record.status in {"running", "queued", "planned"}:
            status = record.status
        elif stage_state.get("status") == "obsolete" and completed is not None:
            status = "obsolete"
        elif completed is not None:
            status = VALID_MATERIALIZED_STATUS
            if record and record.status in {"failed", "cancelled"}:
                run_warning = (
                    "Last run did not complete. The pipeline can still use the latest "
                    "saved materialized revision."
                )
        elif record and record.status in {"failed", "cancelled"}:
            status = record.status

        output_source = {}
        published_products: list[dict[str, object]] = []
        if completed and completed.output_bucket and completed.output_key:
            output_source = {
                "sourceKind": "object",
                "sourceId": "s3",
                "bucket": completed.output_bucket,
                "key": completed.output_key,
                "sourceDisplayName": f"{stage['title']} materialized output",
                "sourcePlatform": "s3",
            }
            if published_products_for_source is not None:
                published_products = published_products_for_source(output_source)

        nodes.append(
            {
                **stage,
                "predecessorStageIds": predecessor_map.get(stage_id, []),
                "status": status,
                "order": len(nodes),
                "layer": layer_by_stage.get(stage_id, 0),
                "successorStageIds": successors.get(stage_id, []),
                "latestRevision": completed.payload if completed else None,
                "latestRun": record.payload if record else None,
                "outputSource": output_source,
                "published": bool(published_products),
                "publishedDataProducts": published_products,
                "obsoleteReason": str(stage_state.get("reason") or "").strip(),
                "runWarning": run_warning,
            }
        )

    source_nodes: list[dict[str, object]] = []
    seen_sources: set[str] = set()
    for stage in stage_cells:
        for source_id in stage.get("dataSources", []) or []:
            source_key = str(source_id or "").strip()
            if not source_key or source_key in seen_sources:
                continue
            seen_sources.add(source_key)
            source_nodes.append(
                {
                    "sourceId": f"source:{source_key}",
                    "label": source_key,
                    "targetStageIds": [
                        str(item["stageId"])
                        for item in stage_cells
                        if source_key in (item.get("dataSources") or [])
                    ],
                }
            )

    edges: list[dict[str, object]] = []
    for stage_id, predecessors in predecessor_map.items():
        for predecessor_id in predecessors:
            if predecessor_id in by_id:
                edges.append({"fromStageId": predecessor_id, "toStageId": stage_id})
    for source in source_nodes:
        for target_stage_id in source.get("targetStageIds", []) or []:
            edges.append({"fromSourceId": source["sourceId"], "toStageId": target_stage_id})

    default_selected = ""
    for node in nodes:
        if node["status"] in {"invalid", "obsolete", "failed"}:
            default_selected = str(node["stageId"])
            break
    if not default_selected and nodes:
        default_selected = str(nodes[0]["stageId"])

    return {
        "notebookId": notebook_id,
        "notebookTitle": notebook_title,
        "version": int(state.get("version") or 0),
        "nodes": nodes,
        "sourceNodes": source_nodes,
        "edges": edges,
        "diagnostics": diagnostics,
        "order": ordered_stage_ids,
        "paths": paths,
        "defaultSelectedStageId": default_selected,
    }


class MaterializedStageManager:
    def __init__(
        self,
        *,
        settings: Settings,
        store: MaterializedStageStore,
        connection_factory: Callable[[], Any],
        source_summaries_provider: Callable[[str, list[str], dict[str, object]], list[dict[str, object]]],
        bootstrap_source_views: Callable[[Any, list[dict[str, object]]], None],
        sql_rewriter: Callable[[str, list[str], dict[str, object]], str] | None = None,
        metadata_refresher: Callable[[], None] | None = None,
        state_change_callback: Callable[[dict[str, object]], None] | None = None,
        published_products_for_source: Callable[[dict[str, object]], list[dict[str, object]]] | None = None,
        object_writer: Callable[[str, str, Path, str, dict[str, object]], dict[str, object]] | None = None,
        query_job_runner: Callable[..., dict[str, object]] | None = None,
    ) -> None:
        self._settings = settings
        self._store = store
        self._connection_factory = connection_factory
        self._source_summaries_provider = source_summaries_provider
        self._bootstrap_source_views = bootstrap_source_views
        self._sql_rewriter = sql_rewriter or (lambda sql, _sources, _options: sql)
        self._metadata_refresher = metadata_refresher
        self._state_change_callback = state_change_callback
        self._published_products_for_source = published_products_for_source
        self._object_writer = object_writer or self._write_object_to_s3
        self._query_job_runner = query_job_runner
        self._lock = threading.RLock()
        self._active_runs: dict[str, dict[str, object]] = {}
        self._threads: list[threading.Thread] = []

    def state_payload(self) -> dict[str, object]:
        state = self._store.read_state()
        records = [record.payload for record in _records_from_state(state)]
        with self._lock:
            active_runs = [dict(item) for item in self._active_runs.values()]
        return {
            "version": int(state.get("version") or 0),
            "records": records,
            "stageStates": dict(state.get("stageStates", {}) or {}),
            "activeRuns": active_runs,
        }

    def graph_payload(
        self,
        *,
        notebook_id: str,
        notebook_title: str = "",
        cells: Iterable[dict[str, object]],
        pipeline_paths: Iterable[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        graph = build_notebook_stage_graph(
            notebook_id=notebook_id,
            notebook_title=notebook_title,
            cells=cells,
            state=self._store.read_state(),
            published_products_for_source=self._published_products_for_source,
            pipeline_paths=pipeline_paths,
        )
        with self._lock:
            active_runs = [
                dict(item)
                for item in self._active_runs.values()
                if item.get("notebookId") == notebook_id
            ]
        if active_runs:
            self._apply_active_runs_to_graph(graph, active_runs)
        graph["activeRuns"] = active_runs
        return graph

    def run_pipeline(
        self,
        *,
        notebook_id: str,
        notebook_title: str = "",
        start_stage_id: str = "",
        cells: Iterable[dict[str, object]],
        pipeline_paths: Iterable[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        graph = self.graph_payload(
            notebook_id=notebook_id,
            notebook_title=notebook_title,
            cells=cells,
            pipeline_paths=pipeline_paths,
        )
        ordered_stage_ids = [str(item) for item in graph.get("order", [])]
        diagnostic_stage_ids = None
        normalized_start_stage_id = str(start_stage_id or "").strip()
        if normalized_start_stage_id:
            if normalized_start_stage_id not in set(ordered_stage_ids):
                return self._record_submission_failure(
                    graph=graph,
                    notebook_id=notebook_id,
                    notebook_title=notebook_title,
                    stage_ids=[normalized_start_stage_id],
                    error=f"Unknown stage: {normalized_start_stage_id}",
                )
            diagnostic_stage_ids = self._stage_and_successor_ids(graph, normalized_start_stage_id)
            ordered_stage_ids = [stage_id for stage_id in ordered_stage_ids if stage_id in diagnostic_stage_ids]
        return self._start_run(
            graph=graph,
            notebook_id=notebook_id,
            notebook_title=notebook_title,
            stage_ids=ordered_stage_ids,
            force=True,
            diagnostic_stage_ids=diagnostic_stage_ids,
        )

    def run_stage(
        self,
        *,
        notebook_id: str,
        stage_id: str,
        notebook_title: str = "",
        cells: Iterable[dict[str, object]],
        pipeline_paths: Iterable[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        graph = self.graph_payload(
            notebook_id=notebook_id,
            notebook_title=notebook_title,
            cells=cells,
            pipeline_paths=pipeline_paths,
        )
        normalized_stage_id = str(stage_id or "").strip()
        if normalized_stage_id not in {str(node.get("stageId")) for node in graph.get("nodes", [])}:
            return self._record_submission_failure(
                graph=graph,
                notebook_id=notebook_id,
                notebook_title=notebook_title,
                stage_ids=[normalized_stage_id],
                error=f"Unknown stage: {normalized_stage_id}",
            )
        diagnostic_stage_ids = self._stage_and_predecessor_ids(graph, normalized_stage_id)
        return self._start_run(
            graph=graph,
            notebook_id=notebook_id,
            notebook_title=notebook_title,
            stage_ids=[normalized_stage_id],
            force=True,
            diagnostic_stage_ids=diagnostic_stage_ids,
        )

    def stop_stage(self, *, notebook_id: str, stage_id: str) -> dict[str, object]:
        normalized_notebook_id = str(notebook_id or "").strip()
        normalized_stage_id = str(stage_id or "").strip()
        cancelled = False
        with self._lock:
            for run in self._active_runs.values():
                if run.get("notebookId") != normalized_notebook_id:
                    continue
                if normalized_stage_id not in set(run.get("stageIds", []) or []):
                    continue
                run["cancelRequested"] = True
                cancelled = True
        if cancelled:
            self._append_record(
                StageRecord(
                    run_id=f"stop-{uuid.uuid4().hex}",
                    notebook_id=normalized_notebook_id,
                    stage_id=normalized_stage_id,
                    cell_id="",
                    stage_alias="",
                    stage_title="",
                    status="cancelled",
                    updated_at=utc_now_iso(),
                    completed_at=utc_now_iso(),
                    message="Stage cancellation requested.",
                    can_cancel=False,
                )
            )
        return self.state_payload()

    def cancel_pipeline(self, *, notebook_id: str) -> dict[str, object]:
        normalized_notebook_id = str(notebook_id or "").strip()
        with self._lock:
            for run in self._active_runs.values():
                if run.get("notebookId") == normalized_notebook_id:
                    run["cancelRequested"] = True
                    run["status"] = "cancelling"
        self._publish_state()
        return self.state_payload()

    def wait_for_idle(self, timeout: float = 10.0) -> None:
        deadline = datetime.now(UTC).timestamp() + timeout
        while datetime.now(UTC).timestamp() < deadline:
            with self._lock:
                alive = [thread for thread in self._threads if thread.is_alive()]
                self._threads = alive
            if not alive:
                return
            for thread in alive:
                thread.join(timeout=0.05)

    def _start_run(
        self,
        *,
        graph: dict[str, object],
        notebook_id: str,
        notebook_title: str,
        stage_ids: list[str],
        force: bool,
        diagnostic_stage_ids: set[str] | None,
    ) -> dict[str, object]:
        diagnostics = [
            item
            for item in graph.get("diagnostics", []) or []
            if (
                isinstance(item, dict)
                and item.get("severity") == "error"
                and self._diagnostic_applies_to_stage_ids(item, diagnostic_stage_ids)
            )
        ]
        if diagnostics:
            diagnostic = diagnostics[0]
            target_stage_ids = self._stage_ids_for_diagnostic(diagnostic) or list(stage_ids)
            return self._record_submission_failure(
                graph=graph,
                notebook_id=notebook_id,
                notebook_title=notebook_title,
                stage_ids=target_stage_ids,
                error=str(diagnostic.get("message") or "Pipeline graph is invalid."),
            )
        if not stage_ids:
            return self._record_submission_failure(
                graph=graph,
                notebook_id=notebook_id,
                notebook_title=notebook_title,
                stage_ids=[],
                error="The notebook does not contain SQL stages to run.",
            )
        run_id = f"stage-run-{uuid.uuid4().hex}"
        with self._lock:
            self._active_runs[run_id] = {
                "runId": run_id,
                "notebookId": notebook_id,
                "notebookTitle": notebook_title,
                "stageIds": list(stage_ids),
                "force": force,
                "status": "running",
                "cancelRequested": False,
                "startedAt": utc_now_iso(),
            }
        thread = threading.Thread(
            target=self._run_stage_sequence,
            args=(run_id, graph, stage_ids, force),
            daemon=True,
            name=f"bdw-materialized-stages-{run_id}",
        )
        with self._lock:
            self._threads.append(thread)
        thread.start()
        self._publish_state()
        return self.state_payload()

    @staticmethod
    def _stage_ids_for_diagnostic(diagnostic: dict[str, object]) -> list[str]:
        stage_ids = [
            str(item).strip()
            for item in diagnostic.get("stageIds", []) or []
            if str(item).strip()
        ]
        stage_id = str(diagnostic.get("stageId") or "").strip()
        if stage_id and stage_id not in stage_ids:
            stage_ids.insert(0, stage_id)
        return stage_ids

    def _record_submission_failure(
        self,
        *,
        graph: dict[str, object],
        notebook_id: str,
        notebook_title: str,
        stage_ids: list[str],
        error: str,
    ) -> dict[str, object]:
        run_id = f"stage-run-{uuid.uuid4().hex}"
        node_by_id = {
            str(node.get("stageId") or ""): node
            for node in graph.get("nodes", []) or []
            if isinstance(node, dict)
        }
        target_nodes = [
            node_by_id[stage_id]
            for stage_id in stage_ids
            if stage_id in node_by_id
        ]
        if not target_nodes:
            fallback_stage_id = next((str(stage_id).strip() for stage_id in stage_ids if str(stage_id).strip()), "")
            target_nodes = [
                {
                    "notebookId": notebook_id,
                    "stageId": fallback_stage_id,
                    "cellId": "",
                    "alias": "",
                    "title": notebook_title or "Pipeline",
                }
            ]
        for node in target_nodes:
            self._append_record(
                self._record_for_node(
                    run_id,
                    {**node, "notebookId": notebook_id},
                    status="failed",
                    message="Pipeline request failed before execution started.",
                    error=error,
                )
            )
        return self.state_payload()

    @staticmethod
    def _diagnostic_applies_to_stage_ids(
        diagnostic: dict[str, object],
        stage_ids: set[str] | None,
    ) -> bool:
        if stage_ids is None:
            return True
        stage_id = str(diagnostic.get("stageId") or "").strip()
        if stage_id:
            return stage_id in stage_ids
        diagnostic_stage_ids = {
            str(item).strip()
            for item in diagnostic.get("stageIds", []) or []
            if str(item).strip()
        }
        return not diagnostic_stage_ids or bool(diagnostic_stage_ids.intersection(stage_ids))

    @staticmethod
    def _stage_and_predecessor_ids(graph: dict[str, object], stage_id: str) -> set[str]:
        predecessor_map = {
            str(node.get("stageId") or ""): [
                str(item).strip()
                for item in node.get("predecessorStageIds", []) or []
                if str(item).strip()
            ]
            for node in graph.get("nodes", []) or []
            if isinstance(node, dict)
        }
        required = {str(stage_id or "").strip()}
        queue = deque(predecessor_map.get(str(stage_id or "").strip(), []))
        while queue:
            predecessor_id = queue.popleft()
            if not predecessor_id or predecessor_id in required:
                continue
            required.add(predecessor_id)
            queue.extend(predecessor_map.get(predecessor_id, []))
        return required

    @staticmethod
    def _stage_and_successor_ids(graph: dict[str, object], stage_id: str) -> set[str]:
        successor_map: dict[str, list[str]] = {}
        for node in graph.get("nodes", []) or []:
            if not isinstance(node, dict):
                continue
            source_id = str(node.get("stageId") or "").strip()
            if not source_id:
                continue
            for successor_id in node.get("successorStageIds", []) or []:
                normalized_successor_id = str(successor_id or "").strip()
                if normalized_successor_id:
                    successor_map.setdefault(source_id, []).append(normalized_successor_id)
        required = {str(stage_id or "").strip()}
        queue = deque(successor_map.get(str(stage_id or "").strip(), []))
        while queue:
            successor_id = queue.popleft()
            if not successor_id or successor_id in required:
                continue
            required.add(successor_id)
            queue.extend(successor_map.get(successor_id, []))
        return required

    def _run_cancelled(self, run_id: str) -> bool:
        with self._lock:
            return bool(self._active_runs.get(run_id, {}).get("cancelRequested"))

    def _run_stage_sequence(
        self,
        run_id: str,
        graph: dict[str, object],
        stage_ids: list[str],
        force: bool,
    ) -> None:
        node_by_id = {
            str(node.get("stageId")): node
            for node in graph.get("nodes", []) or []
            if isinstance(node, dict)
        }
        predecessor_map = {
            str(node.get("stageId")): [
                str(item)
                for item in node.get("predecessorStageIds", []) or []
                if str(item).strip()
            ]
            for node in node_by_id.values()
        }
        try:
            for stage_id in stage_ids:
                node = node_by_id.get(stage_id)
                if not node:
                    continue
                with self._lock:
                    run = self._active_runs.get(run_id)
                    if run is not None:
                        run["currentStageId"] = stage_id
                if self._run_cancelled(run_id):
                    self._append_record(
                        self._record_for_node(
                            run_id,
                            {**node, "notebookId": graph.get("notebookId") or ""},
                            status="cancelled",
                            message="Run cancelled before this stage started.",
                        )
                    )
                    break
                latest_completed = self._latest_completed_record(str(graph.get("notebookId") or ""), stage_id)
                if not force and latest_completed is not None and str(node.get("status")) == VALID_MATERIALIZED_STATUS:
                    self._append_record(
                        self._record_for_node(
                            run_id,
                            {**node, "notebookId": graph.get("notebookId") or ""},
                            status="skipped",
                            message="Valid materialized revision reused.",
                        )
                    )
                    continue
                missing_predecessor = self._first_missing_predecessor(
                    notebook_id=str(graph.get("notebookId") or ""),
                    predecessor_stage_ids=predecessor_map.get(stage_id, []),
                )
                if missing_predecessor:
                    self._append_record(
                        self._record_for_node(
                            run_id,
                            node,
                            status="failed",
                            error=f"Predecessor stage {missing_predecessor} has no completed materialized revision.",
                        )
                    )
                    break
                try:
                    self._run_one_stage(
                        run_id=run_id,
                        graph=graph,
                        node=node,
                        predecessor_stage_ids=predecessor_map.get(stage_id, []),
                    )
                except Exception:
                    break
        finally:
            with self._lock:
                run = self._active_runs.get(run_id)
                if run is not None:
                    run["status"] = "cancelled" if run.get("cancelRequested") else "completed"
                    run["completedAt"] = utc_now_iso()
                self._active_runs.pop(run_id, None)
            self._publish_state()

    def _record_for_node(
        self,
        run_id: str,
        node: dict[str, object],
        *,
        status: str,
        message: str = "",
        error: str = "",
        started_at: str = "",
    ) -> StageRecord:
        now = utc_now_iso()
        normalized_started_at = started_at or now
        output_file_name = normalize_stage_output_file_name(
            node.get("resolvedOutputFileName") or node.get("outputFileName"),
            alias=node.get("alias"),
        )
        return StageRecord(
            run_id=run_id,
            notebook_id=str(node.get("notebookId") or ""),
            stage_id=str(node.get("stageId") or ""),
            cell_id=str(node.get("cellId") or ""),
            stage_alias=str(node.get("alias") or ""),
            stage_title=str(node.get("title") or ""),
            status=status,
            started_at=normalized_started_at,
            completed_at=now if status in TERMINAL_STAGE_STATUSES else "",
            updated_at=now,
            output_file_name=output_file_name,
            message=message,
            error=error,
            can_cancel=status == "running",
        )

    def _first_missing_predecessor(
        self,
        *,
        notebook_id: str,
        predecessor_stage_ids: list[str],
    ) -> str:
        for predecessor_id in predecessor_stage_ids:
            record = self._latest_completed_record(notebook_id, predecessor_id)
            if record is None:
                return predecessor_id
        return ""

    def _latest_completed_record(self, notebook_id: str, stage_id: str) -> StageRecord | None:
        state = self._store.read_state()
        return _latest_completed_by_stage(_records_from_state(state)).get((notebook_id, stage_id))

    def _run_one_stage(
        self,
        *,
        run_id: str,
        graph: dict[str, object],
        node: dict[str, object],
        predecessor_stage_ids: list[str],
    ) -> None:
        notebook_id = str(graph.get("notebookId") or "")
        started_at = utc_now_iso()
        query_job_id = f"query-pipeline-{uuid.uuid4().hex}"
        running_record = self._record_for_node(run_id, {**node, "notebookId": notebook_id}, status="running")
        running_record.started_at = started_at
        running_record.completed_at = ""
        running_record.can_cancel = True
        running_record.query_job_id = query_job_id
        self._append_record(running_record)

        previous_record = self._latest_completed_record(notebook_id, str(node.get("stageId") or ""))
        predecessor_records = [
            self._latest_completed_record(notebook_id, predecessor_id)
            for predecessor_id in predecessor_stage_ids
        ]
        predecessor_revision_ids = [
            record.revision_id
            for record in predecessor_records
            if record is not None and record.revision_id
        ]
        sql = materialized_stage_query_sql(node.get("sql"))
        sql_hash = _sha256_text(sql)
        data_sources = [
            str(item).strip()
            for item in node.get("dataSources", []) or []
            if str(item).strip()
        ]
        query_options = (
            dict(node.get("queryOptions") or {})
            if isinstance(node.get("queryOptions"), dict)
            else {}
        )
        revision_seed = "|".join([sql_hash, *predecessor_revision_ids, uuid.uuid4().hex])
        revision_id = f"{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}-{_sha256_text(revision_seed)[:10]}"

        try:
            if self._run_cancelled(run_id):
                raise InterruptedError("Stage run was cancelled before execution.")
            execution_sql = materialized_stage_query_sql(
                self._sql_rewriter(sql, data_sources, query_options)
            )
            completed = self._execute_stage(
                run_id=run_id,
                graph=graph,
                node=node,
                sql=sql,
                execution_sql=execution_sql,
                sql_hash=sql_hash,
                revision_id=revision_id,
                predecessor_records=[record for record in predecessor_records if record is not None],
                predecessor_revision_ids=predecessor_revision_ids,
                started_at=started_at,
                query_job_id=query_job_id,
            )
            completed.changed_result = bool(
                previous_record
                and previous_record.result_fingerprint
                and completed.result_fingerprint
                and previous_record.result_fingerprint != completed.result_fingerprint
            )
            self._append_record(completed)
            self._clear_stage_obsolete_state(notebook_id, str(node.get("stageId") or ""))
            self._mark_descendants_obsolete(
                graph,
                changed_stage_id=str(node.get("stageId") or ""),
                changed_revision_id=completed.revision_id,
            )
            if self._metadata_refresher is not None:
                self._metadata_refresher()
        except InterruptedError as exc:
            cancelled_record = self._record_for_node(
                run_id,
                {**node, "notebookId": notebook_id},
                status="cancelled",
                message=str(exc),
                started_at=started_at,
            )
            cancelled_record.query_job_id = query_job_id
            self._append_record(cancelled_record)
        except Exception as exc:
            failed_record = self._record_for_node(
                run_id,
                {**node, "notebookId": notebook_id},
                status="failed",
                error=str(exc),
                started_at=started_at,
            )
            failed_record.query_job_id = query_job_id
            self._append_record(failed_record)
            raise

    def _execute_stage(
        self,
        *,
        run_id: str,
        graph: dict[str, object],
        node: dict[str, object],
        sql: str,
        execution_sql: str,
        sql_hash: str,
        revision_id: str,
        predecessor_records: list[StageRecord],
        predecessor_revision_ids: list[str],
        started_at: str,
        query_job_id: str,
    ) -> StageRecord:
        notebook_id = str(graph.get("notebookId") or "")
        stage_id = str(node.get("stageId") or "")
        stage_alias = str(node.get("alias") or "")
        stage_title = str(node.get("title") or "")
        data_sources = [
            str(item).strip()
            for item in node.get("dataSources", []) or []
            if str(item).strip()
        ]
        query_options = (
            dict(node.get("queryOptions") or {})
            if isinstance(node.get("queryOptions"), dict)
            else {}
        )
        source_summaries = self._source_summaries_provider(sql, data_sources, query_options)
        runtime_source_summaries = [
            *source_summaries,
            *self._predecessor_stage_source_summaries(predecessor_records),
        ]
        output_file_name = normalize_stage_output_file_name(
            node.get("resolvedOutputFileName") or node.get("outputFileName"),
            alias=stage_alias,
        )
        temp_dir = Path(tempfile.mkdtemp(prefix="bdw-stage-"))
        local_output = temp_dir / output_file_name
        connection = None
        try:
            try:
                copy_sql = f"COPY ({execution_sql}) TO {sql_literal(local_output.as_posix())} (FORMAT PARQUET)"
                result_preview_sql = f"SELECT * FROM read_parquet({sql_literal(local_output.as_posix())})"
                if self._query_job_runner is not None:
                    query_payload = self._query_job_runner(
                        requested_job_id=query_job_id,
                        display_sql=sql,
                        execution_sql=copy_sql,
                        result_preview_sql=result_preview_sql,
                        notebook_id=notebook_id,
                        notebook_title=str(graph.get("notebookTitle") or ""),
                        cell_id=str(node.get("cellId") or ""),
                        data_sources=data_sources,
                        source_summaries=runtime_source_summaries,
                        touched_relations=[
                            str(summary.get("relation") or "").strip()
                            for summary in runtime_source_summaries
                            if isinstance(summary, dict) and str(summary.get("relation") or "").strip()
                        ],
                        touched_buckets=[
                            str(summary.get("bucket") or "").strip()
                            for summary in runtime_source_summaries
                            if isinstance(summary, dict) and str(summary.get("bucket") or "").strip()
                        ],
                        query_options=query_options,
                        is_cancelled=lambda: self._run_cancelled(run_id),
                    )
                    query_status = str(query_payload.get("status") or "").strip().lower()
                    if query_status == "cancelled":
                        raise InterruptedError("Stage query job was cancelled.")
                    if query_status != "completed":
                        error = str(
                            query_payload.get("error")
                            or query_payload.get("message")
                            or "Stage query job failed."
                        ).strip()
                        raise RuntimeError(error)
                else:
                    connection = self._connection_factory()
                    self._bootstrap_source_views(connection, source_summaries)
                    self._bootstrap_stage_views(connection, predecessor_records)
                    connection.execute(copy_sql)
                if self._run_cancelled(run_id):
                    raise InterruptedError("Stage run was cancelled after execution.")
                size_bytes = local_output.stat().st_size
                if connection is None:
                    connection = self._connection_factory()
                row_count = int(
                    connection.execute(
                        f"SELECT COUNT(*) FROM read_parquet({sql_literal(local_output.as_posix())})"
                    ).fetchone()[0]
                    or 0
                )
                schema_rows = connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet({sql_literal(local_output.as_posix())})"
                ).fetchall()
                schema_fingerprint = _sha256_text(json.dumps(schema_rows, default=str, sort_keys=True))
            finally:
                if connection is not None:
                    try:
                        connection.close()
                    finally:
                        pass

            file_fingerprint = _sha256_file(local_output)
            result_fingerprint = _sha256_text(
                json.dumps(
                    {
                        "sqlHash": sql_hash,
                        "predecessorRevisionIds": predecessor_revision_ids,
                        "schemaFingerprint": schema_fingerprint,
                        "rowCount": row_count,
                        "fileFingerprint": file_fingerprint,
                    },
                    sort_keys=True,
                )
            )
            bucket = self._stage_bucket()
            key_prefix = "/".join(
                [
                    STAGE_ROOT_PREFIX,
                    notebook_slug(notebook_id, str(graph.get("notebookTitle") or "")),
                    stage_alias,
                    revision_id,
                ]
            )
            output_key = f"{key_prefix}/{output_file_name}"
            metadata_key = f"{key_prefix}/_bdw_stage.json"
            metadata = {
                "notebookId": notebook_id,
                "stageId": stage_id,
                "stageAlias": stage_alias,
                "stageTitle": stage_title,
                "revisionId": revision_id,
                "sqlHash": sql_hash,
                "predecessorRevisionIds": predecessor_revision_ids,
                "schemaFingerprint": schema_fingerprint,
                "rowCount": row_count,
                "sizeBytes": size_bytes,
                "resultFingerprint": result_fingerprint,
                "outputFileName": output_file_name,
                "queryJobId": query_job_id,
                "createdAt": utc_now_iso(),
            }
            self._object_writer(bucket, output_key, local_output, metadata_key, metadata)
            now = utc_now_iso()
            query_reference = s3_source_reference(bucket=bucket, key=output_key)
            query_sql = s3_table_function_sql(
                bucket=bucket,
                key=output_key,
                file_format="parquet",
            )
            return StageRecord(
                run_id=run_id,
                notebook_id=notebook_id,
                stage_id=stage_id,
                cell_id=str(node.get("cellId") or ""),
                stage_alias=stage_alias,
                stage_title=stage_title,
                status="completed",
                revision_id=revision_id,
                sql_hash=sql_hash,
                predecessor_revision_ids=predecessor_revision_ids,
                schema_fingerprint=schema_fingerprint,
                row_count=row_count,
                size_bytes=size_bytes,
                result_fingerprint=result_fingerprint,
                output_bucket=bucket,
                output_key=output_key,
                metadata_key=metadata_key,
                output_path=f"s3://{bucket}/{output_key}",
                output_file_name=output_file_name,
                query_path=query_reference,
                query_reference=query_reference,
                query_sql=query_sql,
                query_job_id=query_job_id,
                started_at=started_at or now,
                completed_at=now,
                updated_at=now,
                message=f"Materialized {row_count} rows.",
                can_cancel=False,
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    def _predecessor_stage_source_summaries(predecessor_records: list[StageRecord]) -> list[dict[str, object]]:
        summaries: list[dict[str, object]] = []
        for record in predecessor_records:
            relation = f"{STAGE_SCHEMA_NAME}.{record.stage_alias}" if record.stage_alias else ""
            query_sql = str(record.query_sql or "").strip()
            if not relation or not query_sql:
                continue
            summaries.append(
                {
                    "relation": relation,
                    "query_alias": "",
                    "query_reference": record.query_reference or record.query_path,
                    "bucket": record.output_bucket,
                    "key": record.output_key,
                    "path": record.output_path,
                    "format": "parquet",
                    "size_bytes": record.size_bytes,
                    "object_revision": record.revision_id,
                    "display_name": record.stage_title or record.stage_alias,
                    "query_sql": query_sql,
                }
            )
        return summaries

    @staticmethod
    def _apply_active_runs_to_graph(graph: dict[str, object], active_runs: list[dict[str, object]]) -> None:
        nodes = graph.get("nodes")
        if not isinstance(nodes, list):
            return
        for node in nodes:
            if not isinstance(node, dict):
                continue
            stage_id = str(node.get("stageId") or "")
            for run in active_runs:
                stage_ids = {str(item) for item in run.get("stageIds", []) or []}
                if stage_id not in stage_ids:
                    continue
                run_id = str(run.get("runId") or "")
                latest_run = node.get("latestRun")
                latest_run_id = str(latest_run.get("runId") or "") if isinstance(latest_run, dict) else ""
                latest_status = str(latest_run.get("status") or "").lower() if isinstance(latest_run, dict) else ""
                if latest_run_id == run_id and latest_status in TERMINAL_STAGE_STATUSES:
                    break
                if latest_run_id == run_id and latest_status == "running":
                    node["status"] = "running"
                else:
                    node["status"] = "queued"
                node["activeRun"] = dict(run)
                break

    def _bootstrap_stage_views(self, connection: Any, predecessor_records: list[StageRecord]) -> None:
        if not predecessor_records:
            return
        connection.execute(f"CREATE SCHEMA IF NOT EXISTS {qualified_name(STAGE_SCHEMA_NAME)}")
        for record in predecessor_records:
            if not record.output_path or not record.stage_alias:
                continue
            connection.execute(
                (
                    f"CREATE OR REPLACE VIEW {qualified_name(STAGE_SCHEMA_NAME, record.stage_alias)} "
                    f"AS SELECT * FROM read_parquet({sql_literal(record.output_path)})"
                )
            )

    def _stage_bucket(self) -> str:
        bucket = str(self._settings.s3_bucket or self._settings.shared_notebooks_bucket or "").strip()
        if not bucket:
            raise ValueError("S3_BUCKET or BDW_SHARED_NOTEBOOKS_BUCKET is required for materialized stages.")
        return bucket

    def _write_object_to_s3(
        self,
        bucket: str,
        output_key: str,
        local_output: Path,
        metadata_key: str,
        metadata: dict[str, object],
    ) -> dict[str, object]:
        try:
            client = s3_client(self._settings)
            client.upload_file(str(local_output), bucket, output_key)
            client.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2).encode("utf-8"),
                ContentType="application/json",
            )
        except (BotoCoreError, ClientError, OSError) as exc:
            raise RuntimeError(f"Failed to write materialized stage to S3: {exc}") from exc
        return {"bucket": bucket, "key": output_key, "metadataKey": metadata_key}

    def _append_record(self, record: StageRecord) -> None:
        state = self._store.read_state()
        records = list(state.get("records", []) or [])
        records.append(record.payload)
        state["records"] = records[-400:]
        self._store.write_state(state)
        self._publish_state()

    def _clear_stage_obsolete_state(self, notebook_id: str, stage_id: str) -> None:
        state = self._store.read_state()
        stage_states = dict(state.get("stageStates", {}) or {})
        notebook_states = dict(stage_states.get(notebook_id, {}) or {})
        if stage_id not in notebook_states:
            return
        notebook_states.pop(stage_id, None)
        stage_states[notebook_id] = notebook_states
        state["stageStates"] = stage_states
        self._store.write_state(state)
        self._publish_state()

    def _mark_descendants_obsolete(
        self,
        graph: dict[str, object],
        *,
        changed_stage_id: str,
        changed_revision_id: str,
    ) -> None:
        notebook_id = str(graph.get("notebookId") or "")
        successors: dict[str, list[str]] = {}
        for edge in graph.get("edges", []) or []:
            if not isinstance(edge, dict):
                continue
            source = str(edge.get("fromStageId") or "").strip()
            target = str(edge.get("toStageId") or "").strip()
            if source and target:
                successors.setdefault(source, []).append(target)
        queue = deque(successors.get(changed_stage_id, []))
        descendants: set[str] = set()
        while queue:
            stage_id = queue.popleft()
            if stage_id in descendants:
                continue
            descendants.add(stage_id)
            queue.extend(successors.get(stage_id, []))
        if not descendants:
            return
        state = self._store.read_state()
        stage_states = dict(state.get("stageStates", {}) or {})
        notebook_states = dict(stage_states.get(notebook_id, {}) or {})
        now = utc_now_iso()
        for descendant_id in descendants:
            notebook_states[descendant_id] = {
                "status": "obsolete",
                "reason": "A predecessor was re-materialized and this stage should be rerun.",
                "changedPredecessorStageId": changed_stage_id,
                "changedPredecessorRevisionId": changed_revision_id,
                "updatedAt": now,
            }
        stage_states[notebook_id] = notebook_states
        state["stageStates"] = stage_states
        self._store.write_state(state)
        self._publish_state()

    def _publish_state(self) -> None:
        if self._state_change_callback is not None:
            self._state_change_callback(self.state_payload())
