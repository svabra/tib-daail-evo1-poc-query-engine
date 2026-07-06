from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any


CONTENT_PATH = (
    Path(__file__).resolve().parents[1] / "data" / "data_platform_content.json"
)


@dataclass(frozen=True)
class DataPlatformCapability:
    id: str
    label: str
    description: str


@dataclass(frozen=True)
class DataPlatformService:
    id: str
    label: str
    description: str


@dataclass(frozen=True)
class DataPlatformLink:
    capability: str
    service: str


@dataclass(frozen=True)
class DataPlatformTopic:
    slug: str
    title: str
    summary: str
    capabilities: tuple[DataPlatformCapability, ...]
    services: tuple[DataPlatformService, ...]
    links: tuple[DataPlatformLink, ...]


@dataclass(frozen=True)
class DataPlatformTopicUseCase:
    id: str
    source: str
    target: str
    label: str
    path: str
    duration: str
    label_x: int
    label_y: int


def _require_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Data Platform content field '{key}' must be a non-empty string")
    return value


def _require_int(data: dict[str, Any], key: str) -> int:
    value = data.get(key)
    if not isinstance(value, int):
        raise ValueError(f"Data Platform content field '{key}' must be an integer")
    return value


def _load_raw_content() -> dict[str, Any]:
    with CONTENT_PATH.open(encoding="utf-8") as content_file:
        payload = json.load(content_file)
    if not isinstance(payload, dict):
        raise ValueError("Data Platform content must be a JSON object")
    return payload


def _load_capability(data: dict[str, Any]) -> DataPlatformCapability:
    return DataPlatformCapability(
        id=_require_str(data, "id"),
        label=_require_str(data, "label"),
        description=_require_str(data, "description"),
    )


def _load_service(data: dict[str, Any]) -> DataPlatformService:
    return DataPlatformService(
        id=_require_str(data, "id"),
        label=_require_str(data, "label"),
        description=_require_str(data, "description"),
    )


def _load_link(data: dict[str, Any]) -> DataPlatformLink:
    return DataPlatformLink(
        capability=_require_str(data, "capability"),
        service=_require_str(data, "service"),
    )


def _load_topic(data: dict[str, Any]) -> DataPlatformTopic:
    capabilities = tuple(
        _load_capability(capability) for capability in data.get("capabilities", [])
    )
    services = tuple(_load_service(service) for service in data.get("services", []))
    links = tuple(_load_link(link) for link in data.get("links", []))
    return DataPlatformTopic(
        slug=_require_str(data, "slug"),
        title=_require_str(data, "title"),
        summary=_require_str(data, "summary"),
        capabilities=capabilities,
        services=services,
        links=links,
    )


def _load_topic_use_case(data: dict[str, Any]) -> DataPlatformTopicUseCase:
    return DataPlatformTopicUseCase(
        id=_require_str(data, "id"),
        source=_require_str(data, "source"),
        target=_require_str(data, "target"),
        label=_require_str(data, "label"),
        path=_require_str(data, "path"),
        duration=_require_str(data, "duration"),
        label_x=_require_int(data, "labelX"),
        label_y=_require_int(data, "labelY"),
    )


def _require_unique(values: list[str], label: str) -> None:
    duplicates = {value for value in values if values.count(value) > 1}
    if duplicates:
        raise ValueError(f"Duplicate Data Platform {label}: {', '.join(sorted(duplicates))}")


def _validate_content(
    topics: tuple[DataPlatformTopic, ...],
    topic_use_cases: tuple[DataPlatformTopicUseCase, ...],
) -> None:
    topic_slugs = [topic.slug for topic in topics]
    _require_unique(topic_slugs, "topic slugs")
    topic_slug_set = set(topic_slugs)

    for topic in topics:
        capability_ids = [capability.id for capability in topic.capabilities]
        service_ids = [service.id for service in topic.services]
        _require_unique(capability_ids, f"capability ids in {topic.slug}")
        _require_unique(service_ids, f"service ids in {topic.slug}")
        capability_id_set = set(capability_ids)
        service_id_set = set(service_ids)

        for link in topic.links:
            if link.capability not in capability_id_set:
                raise ValueError(
                    f"Capability-service link in {topic.slug} references unknown "
                    f"capability '{link.capability}'"
                )
            if link.service not in service_id_set:
                raise ValueError(
                    f"Capability-service link in {topic.slug} references unknown "
                    f"service '{link.service}'"
                )

    use_case_ids = [use_case.id for use_case in topic_use_cases]
    _require_unique(use_case_ids, "topic use-case ids")
    for use_case in topic_use_cases:
        if use_case.source not in topic_slug_set:
            raise ValueError(
                f"Topic use-case '{use_case.id}' references unknown source "
                f"'{use_case.source}'"
            )
        if use_case.target not in topic_slug_set:
            raise ValueError(
                f"Topic use-case '{use_case.id}' references unknown target "
                f"'{use_case.target}'"
            )


def _load_content() -> tuple[tuple[DataPlatformTopic, ...], tuple[DataPlatformTopicUseCase, ...]]:
    payload = _load_raw_content()
    topics = tuple(_load_topic(topic) for topic in payload.get("topics", []))
    topic_use_cases = tuple(
        _load_topic_use_case(use_case)
        for use_case in payload.get("topicUseCases", [])
    )
    _validate_content(topics, topic_use_cases)
    return topics, topic_use_cases


DATA_PLATFORM_TOPICS, DATA_PLATFORM_TOPIC_USE_CASES = _load_content()
TOPICS_BY_SLUG = {topic.slug: topic for topic in DATA_PLATFORM_TOPICS}


def get_data_platform_topic(slug: str) -> DataPlatformTopic | None:
    return TOPICS_BY_SLUG.get(slug)
