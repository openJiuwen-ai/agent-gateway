from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any

from .config import load_settings
from .index_service import SkillIndexService


@dataclass
class _BuildTask:
    thread: threading.Thread
    cancel_event: threading.Event
    force: bool
    source: str


_TASKS: dict[str, _BuildTask] = {}
_LOCK = threading.RLock()


def start_skill_index_build(
    manager: Any,
    *,
    force: bool = False,
    source: str = "manual",
) -> dict[str, Any]:
    settings = load_settings()
    key = str(settings.artifact_root)
    service = SkillIndexService(manager)
    if not settings.enabled:
        return {"success": False, "result": service.build_index(force=force, source=source).get("result", "")}

    with _LOCK:
        existing = _TASKS.get(key)
        if existing is not None and existing.thread.is_alive():
            return {
                "success": True,
                "background": True,
                "build_status": "running",
                "result": (
                    "# Skill Index Build\n\n"
                    "A skill index build is already running in the background. "
                    "Open the Skill Index tab to watch progress."
                ),
            }
        if existing is not None:
            _TASKS.pop(key, None)

        cancel_event = threading.Event()
        service.mark_background_started(source=source)
        thread = threading.Thread(
            target=_run_build,
            args=(manager, key, force, source, cancel_event),
            name="skill-index-build",
            daemon=True,
        )
        _TASKS[key] = _BuildTask(thread=thread, cancel_event=cancel_event, force=force, source=source)
        thread.start()

    return {
        "success": True,
        "background": True,
        "build_status": "running",
        "result": (
            "# Skill Index Build\n\n"
            "Skill index build started in the background. "
            "Open the Skill Index tab to watch progress."
        ),
    }


def cancel_skill_index_build(manager: Any) -> dict[str, Any]:
    settings = load_settings()
    key = str(settings.artifact_root)
    with _LOCK:
        task = _TASKS.get(key)
        if task is not None and task.thread.is_alive():
            task.cancel_event.set()
            SkillIndexService(manager).request_cancel()
            return {
                "success": True,
                "build_status": "cancel_requested",
                "result": "# Skill Index Build\n\nCancellation requested.",
            }

    return {
        "success": False,
        "build_status": "idle",
        "result": "# Skill Index Build\n\nNo running skill index build was found.",
    }


def _run_build(
    manager: Any,
    key: str,
    force: bool,
    source: str,
    cancel_event: threading.Event,
) -> None:
    try:
        SkillIndexService(manager).build_index(
            force=force,
            source=source,
            cancel_check=cancel_event.is_set,
        )
    finally:
        with _LOCK:
            task = _TASKS.get(key)
            if task is not None and task.cancel_event is cancel_event:
                _TASKS.pop(key, None)
