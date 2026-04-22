from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from queue import Empty
from typing import Any, Callable

from ...models import PythonJobOutputDefinition
from .output_parser import error_output, mime_bundle_to_output, stream_output


DEFAULT_KERNEL_IDLE_TIMEOUT_SECONDS = 30 * 60
KERNEL_READY_TIMEOUT_SECONDS = 30
KERNEL_POLL_TIMEOUT_SECONDS = 1


@dataclass(slots=True)
class KernelSessionKey:
    client_id: str
    notebook_id: str


@dataclass(slots=True)
class KernelSession:
    key: KernelSessionKey
    manager: Any
    client: Any
    lock: threading.Lock = field(default_factory=threading.Lock)
    current_msg_id: str = ""
    last_used_at: float = field(default_factory=time.monotonic)


class KernelSessionManager:
    def __init__(self, *, idle_timeout_seconds: int = DEFAULT_KERNEL_IDLE_TIMEOUT_SECONDS) -> None:
        self._idle_timeout_seconds = max(60, int(idle_timeout_seconds))
        self._lock = threading.RLock()
        self._sessions: dict[tuple[str, str], KernelSession] = {}

    def get_session(self, *, client_id: str, notebook_id: str) -> KernelSession:
        normalized_key = self._normalized_key(client_id=client_id, notebook_id=notebook_id)
        with self._lock:
            self._reap_idle_sessions_locked()
            existing = self._sessions.get(normalized_key)
            if existing is not None:
                existing.last_used_at = time.monotonic()
                return existing

            session = self._create_session(client_id=normalized_key[0], notebook_id=normalized_key[1])
            self._sessions[normalized_key] = session
            return session

    def interrupt_session(self, *, client_id: str, notebook_id: str) -> None:
        session = self._session_for(client_id=client_id, notebook_id=notebook_id)
        if session is None:
            return
        try:
            session.manager.interrupt_kernel()
        except Exception:
            pass

    def restart_session(self, *, client_id: str, notebook_id: str) -> None:
        normalized_key = self._normalized_key(client_id=client_id, notebook_id=notebook_id)
        with self._lock:
            existing = self._sessions.pop(normalized_key, None)
        if existing is not None:
            self._close_session(existing)

        session = self._create_session(client_id=normalized_key[0], notebook_id=normalized_key[1])
        with self._lock:
            self._sessions[normalized_key] = session

    def shutdown_all(self) -> None:
        with self._lock:
            sessions = list(self._sessions.values())
            self._sessions.clear()
        for session in sessions:
            self._close_session(session)

    def execute(
        self,
        session: KernelSession,
        *,
        code: str,
        context: dict[str, Any],
        is_cancelled: Callable[[], bool],
    ) -> list[PythonJobOutputDefinition]:
        prelude = self._execution_prelude(context)
        msg_id = session.client.execute(
            code=f"{prelude}\n{code}",
            stop_on_error=True,
            allow_stdin=False,
            store_history=True,
        )
        session.current_msg_id = msg_id
        outputs: list[PythonJobOutputDefinition] = []

        while True:
            if is_cancelled():
                try:
                    session.manager.interrupt_kernel()
                except Exception:
                    pass

            try:
                message = session.client.get_iopub_msg(timeout=KERNEL_POLL_TIMEOUT_SECONDS)
            except Empty:
                continue

            parent_id = str(message.get("parent_header", {}).get("msg_id") or "")
            if parent_id != msg_id:
                continue

            message_type = str(message.get("msg_type") or "")
            content = dict(message.get("content") or {})
            if message_type == "stream":
                outputs.append(stream_output(content.get("name", "stdout"), content.get("text", "")))
                continue
            if message_type in {"display_data", "execute_result"}:
                rendered_output = mime_bundle_to_output(content.get("data", {}), content.get("metadata", {}))
                if rendered_output is not None:
                    outputs.append(rendered_output)
                continue
            if message_type == "error":
                outputs.append(
                    error_output(
                        content.get("ename", ""),
                        content.get("evalue", ""),
                        content.get("traceback", []),
                    )
                )
                continue
            if message_type == "status" and str(content.get("execution_state") or "") == "idle":
                break

        session.current_msg_id = ""
        session.last_used_at = time.monotonic()
        return outputs

    def _execution_prelude(self, context: dict[str, Any]) -> str:
        serialized_context = json.dumps(context)
        serialized_context_literal = json.dumps(serialized_context)
        return "\n".join(
            (
                "import json as __bdw_json",
                "from bit_data_workbench.backend.python_execution.kernel_runtime import apply_execution_context as __bdw_apply_execution_context",
                f"__bdw_apply_execution_context(globals(), __bdw_json.loads({serialized_context_literal}))",
                "del __bdw_apply_execution_context",
                "del __bdw_json",
            )
        )

    def _session_for(self, *, client_id: str, notebook_id: str) -> KernelSession | None:
        with self._lock:
            self._reap_idle_sessions_locked()
            return self._sessions.get(self._normalized_key(client_id=client_id, notebook_id=notebook_id))

    def _create_session(self, *, client_id: str, notebook_id: str) -> KernelSession:
        try:
            from jupyter_client import KernelManager
        except ImportError as exc:
            raise RuntimeError(
                "jupyter_client is required for Python notebook execution."
            ) from exc

        python_path_root = Path(__file__).resolve().parents[3]
        env = os.environ.copy()
        current_pythonpath = env.get("PYTHONPATH", "")
        python_paths = [str(python_path_root)]
        if current_pythonpath:
            python_paths.append(current_pythonpath)
        env["PYTHONPATH"] = os.pathsep.join(python_paths)
        env.setdefault("MPLBACKEND", "Agg")
        env.setdefault("PYTHONUNBUFFERED", "1")

        manager = KernelManager()
        manager.start_kernel(env=env, cwd=str(python_path_root.parent))
        client = manager.client()
        client.start_channels()
        client.wait_for_ready(timeout=KERNEL_READY_TIMEOUT_SECONDS)

        return KernelSession(
            key=KernelSessionKey(client_id=client_id, notebook_id=notebook_id),
            manager=manager,
            client=client,
        )

    def _close_session(self, session: KernelSession) -> None:
        try:
            session.client.stop_channels()
        except Exception:
            pass
        try:
            session.manager.shutdown_kernel(now=True)
        except Exception:
            pass

    def _reap_idle_sessions_locked(self) -> None:
        now = time.monotonic()
        stale_keys = [
            key
            for key, session in self._sessions.items()
            if now - session.last_used_at >= self._idle_timeout_seconds and not session.lock.locked()
        ]
        sessions = [self._sessions.pop(key) for key in stale_keys]
        for session in sessions:
            self._close_session(session)

    def _normalized_key(self, *, client_id: str, notebook_id: str) -> tuple[str, str]:
        normalized_client_id = str(client_id or "").strip()
        normalized_notebook_id = str(notebook_id or "").strip()
        if not normalized_client_id:
            raise ValueError("Missing workbench client id for Python kernel execution.")
        if not normalized_notebook_id:
            raise ValueError("Missing notebook id for Python kernel execution.")
        return normalized_client_id, normalized_notebook_id
