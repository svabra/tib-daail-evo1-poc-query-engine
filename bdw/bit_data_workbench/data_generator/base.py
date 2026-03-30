from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

import duckdb

from ..config import Settings
from ..models import DataGenerationJobDefinition, DataGeneratorDefinition


BYTES_PER_GB = 1024**3


class DataGenerationCancelled(Exception):
    pass


@dataclass(slots=True)
class DataGeneratorResult:
    target_name: str
    target_relation: str = ""
    target_path: str = ""
    generated_rows: int = 0
    generated_size_gb: float = 0.0
    message: str = "Data generation completed."


@dataclass(slots=True)
class DataGeneratorContext:
    settings: Settings
    job_id: str
    requested_size_gb: float
    connection_factory: Callable[[], duckdb.DuckDBPyConnection]
    progress_callback: Callable[..., None]
    is_cancelled: Callable[[], bool]

    def connect(self) -> duckdb.DuckDBPyConnection:
        return self.connection_factory()

    def report(
        self,
        *,
        progress: float | None = None,
        progress_label: str | None = None,
        message: str | None = None,
        **changes,
    ) -> None:
        self.progress_callback(
            progress=progress,
            progress_label=progress_label,
            message=message,
            **changes,
        )

    def raise_if_cancelled(self) -> None:
        if self.is_cancelled():
            raise DataGenerationCancelled()


def clamp_size_gb(value: float, minimum: float, maximum: float) -> float:
    if value != value or value <= 0:
        return minimum
    return max(minimum, min(maximum, value))


def estimated_rows_for_size(size_gb: float, approximate_row_bytes: int) -> int:
    row_bytes = max(64, int(approximate_row_bytes))
    total_bytes = max(1, int(size_gb * BYTES_PER_GB))
    return max(1, total_bytes // row_bytes)


def generated_name(prefix: str, job_id: str) -> str:
    return prefix


class DataGenerator(ABC):
    generator_id = ""
    title = ""
    description = ""
    target_kind = "unknown"
    module_name = ""
    default_size_gb = 1.0
    min_size_gb = 0.1
    max_size_gb = 1024.0
    approximate_row_bytes = 256
    default_target_name = "generated_data"
    supports_cancel = True
    supports_cleanup = True
    tags: tuple[str, ...] = ()

    def definition(self) -> DataGeneratorDefinition:
        return DataGeneratorDefinition(
            generator_id=self.generator_id,
            title=self.title,
            description=self.description,
            target_kind=self.target_kind,
            module_name=self.module_name or self.__class__.__module__,
            default_size_gb=self.default_size_gb,
            min_size_gb=self.min_size_gb,
            max_size_gb=self.max_size_gb,
            approximate_row_bytes=self.approximate_row_bytes,
            default_target_name=self.default_target_name,
            supports_cancel=self.supports_cancel,
            supports_cleanup=self.supports_cleanup,
            tags=list(self.tags),
        )

    def normalize_size_gb(self, value: float) -> float:
        return round(clamp_size_gb(value, self.min_size_gb, self.max_size_gb), 3)

    @abstractmethod
    def run(self, context: DataGeneratorContext) -> DataGeneratorResult:
        raise NotImplementedError

    @abstractmethod
    def cleanup(
        self,
        context: DataGeneratorContext,
        job: DataGenerationJobDefinition,
    ) -> DataGeneratorResult:
        raise NotImplementedError
