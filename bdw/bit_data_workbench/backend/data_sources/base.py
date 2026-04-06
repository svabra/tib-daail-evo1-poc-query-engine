from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


class DataSourceOperationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class DataSourceCreateRequest:
    kind: str
    name: str = ""
    container: str = ""
    path: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class DataSourceDeleteRequest:
    kind: str
    container: str = ""
    path: str = ""
    name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


class DataSourcePlugin(ABC):
    @property
    @abstractmethod
    def source_id(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def source_label(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def source_type(self) -> str:
        raise NotImplementedError

    def supports_create(self, kind: str) -> bool:
        return False

    def supports_delete(self, kind: str) -> bool:
        return False

    def create(self, request: DataSourceCreateRequest):
        raise DataSourceOperationError(
            f"{self.source_label} does not support creating '{request.kind}' entries."
        )

    def delete(self, request: DataSourceDeleteRequest):
        raise DataSourceOperationError(
            f"{self.source_label} does not support deleting '{request.kind}' entries."
        )