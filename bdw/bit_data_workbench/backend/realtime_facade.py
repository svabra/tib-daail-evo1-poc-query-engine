from __future__ import annotations

from typing import Any, Sequence


class WorkbenchRealtimeFacade:
    def __init__(self, service: Any, topic_order: Sequence[str]) -> None:
        self._service = service
        self._topic_order = tuple(topic_order)

    def initialize_state(self) -> None:
        self._service._client_connections_version = 0
        self._service._active_realtime_clients = 0
        self._service._realtime_signal_version = 0
        self._service._realtime_topic_versions = {
            topic: 0 for topic in self._topic_order
        }
        self._service._realtime_snapshots = {}

    def client_connections_state(self) -> dict[str, object]:
        with self._service._condition:
            return self.client_connections_state_locked()

    def client_connections_state_locked(self) -> dict[str, object]:
        return {
            "version": self._service._client_connections_version,
            "count": self._service._active_realtime_clients,
        }

    def register_client(self) -> dict[str, object]:
        with self._service._condition:
            self._service._active_realtime_clients += 1
            self._service._client_connections_version += 1
            snapshot = self.client_connections_state_locked()
            self.set_snapshot_locked(
                "client-connections",
                snapshot,
                notify=True,
            )
            return snapshot

    def unregister_client(self) -> dict[str, object]:
        with self._service._condition:
            self._service._active_realtime_clients = max(
                0,
                self._service._active_realtime_clients - 1,
            )
            self._service._client_connections_version += 1
            snapshot = self.client_connections_state_locked()
            self.set_snapshot_locked(
                "client-connections",
                snapshot,
                notify=True,
            )
            return snapshot

    def wait_for_updates(
        self,
        last_versions: dict[str, int] | None,
        timeout: float,
    ) -> list[dict[str, Any]]:
        normalized_versions = {
            topic: int((last_versions or {}).get(topic, -1))
            for topic in self._topic_order
        }
        with self._service._condition:
            if self.has_updates_locked(normalized_versions):
                return self.updates_locked(normalized_versions)

            self._service._condition.wait_for(
                lambda: self.has_updates_locked(normalized_versions),
                timeout=timeout,
            )
            return self.updates_locked(normalized_versions)

    def set_snapshot(
        self,
        topic: str,
        snapshot: dict[str, Any],
        *,
        notify: bool,
    ) -> None:
        with self._service._condition:
            self.set_snapshot_locked(topic, snapshot, notify=notify)

    def set_snapshot_locked(
        self,
        topic: str,
        snapshot: dict[str, Any],
        *,
        notify: bool,
    ) -> None:
        self._service._realtime_snapshots[topic] = snapshot
        self._service._realtime_topic_versions[topic] = int(
            snapshot.get("version", 0)
        )
        if notify:
            self.notify_listeners_locked()

    def publish_snapshot(self, topic: str, snapshot: dict[str, Any]) -> None:
        with self._service._condition:
            self.set_snapshot_locked(topic, snapshot, notify=True)

    def notify_listeners_locked(self) -> None:
        self._service._realtime_signal_version += 1
        self._service._condition.notify_all()

    def has_updates_locked(self, last_versions: dict[str, int]) -> bool:
        return any(
            self._service._realtime_topic_versions.get(topic, 0)
            != last_versions.get(topic, -1)
            for topic in self._topic_order
        )

    def updates_locked(self, last_versions: dict[str, int]) -> list[dict[str, Any]]:
        updates: list[dict[str, Any]] = []
        for topic in self._topic_order:
            version = self._service._realtime_topic_versions.get(topic, 0)
            if version == last_versions.get(topic, -1):
                continue
            updates.append(
                {
                    "topic": topic,
                    "snapshot": self._service._realtime_snapshots.get(
                        topic, {"version": version}
                    ),
                }
            )
        return updates