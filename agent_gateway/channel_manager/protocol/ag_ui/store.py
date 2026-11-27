"""Durable routing and message identities, without a second conversation store."""

from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from typing import Any


class AGUIError(Exception):
    def __init__(self, message: str, code: str = "BAD_REQUEST", status: int = 400):
        super().__init__(message)
        self.code = code
        self.status = status


class ThreadStore:
    """Use unique constraints and an execution claim across Gateway workers.

    An abandoned claim stays blocked after a crash. It must never expire into
    an automatic retry of an execution with unknown side effects.
    """

    def __init__(self, path: Path):
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._db() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS ag_ui_threads (
                principal TEXT NOT NULL, thread_id TEXT NOT NULL,
                metadata TEXT NOT NULL, active TEXT,
                PRIMARY KEY (principal, thread_id))""")
            db.execute("""CREATE TABLE IF NOT EXISTS ag_ui_runs (
                principal TEXT NOT NULL, thread_id TEXT NOT NULL,
                run_id TEXT NOT NULL, request_id TEXT NOT NULL,
                PRIMARY KEY (principal, thread_id, run_id))""")

    @contextmanager
    def _db(self):
        with closing(sqlite3.connect(self.path, timeout=5)) as db:
            with db:
                db.execute("BEGIN IMMEDIATE")
                yield db

    def claim(self, principal: str, thread_id: str, run_id: str) -> dict[str, Any]:
        with self._db() as db:
            row = db.execute(
                "SELECT metadata, active FROM ag_ui_threads WHERE principal=? AND thread_id=?",
                (principal, thread_id),
            ).fetchone()
            if row and row[1]:
                raise AGUIError("thread is executing or awaiting cancellation", "THREAD_BUSY", 409)
            metadata = json.loads(row[0]) if row else {
                "session_id": None, "create_token": uuid.uuid4().hex,
                "messages": [], "pending": [], "blocked": False,
            }
            if metadata["blocked"]:
                raise AGUIError("thread history is incomplete; start a new thread", "HISTORY_INCOMPLETE", 409)
            request_id = "agui-" + uuid.uuid4().hex
            try:
                db.execute("INSERT INTO ag_ui_runs VALUES (?, ?, ?, ?)",
                           (principal, thread_id, run_id, request_id))
            except sqlite3.IntegrityError as exc:
                raise AGUIError("runId has already been submitted", "DUPLICATE_RUN", 409) from exc
            db.execute("INSERT OR REPLACE INTO ag_ui_threads VALUES (?, ?, ?, ?)",
                       (principal, thread_id, json.dumps(metadata), request_id))
            return {**metadata, "request_id": request_id}

    def save(self, principal: str, thread_id: str, data: dict[str, Any], *, release: bool = False) -> None:
        request_id = data["request_id"]
        metadata = {k: v for k, v in data.items() if k != "request_id"}
        with self._db() as db:
            changed = db.execute(
                "UPDATE ag_ui_threads SET metadata=?, active=? "
                "WHERE principal=? AND thread_id=? AND active=?",
                (json.dumps(metadata), None if release else request_id, principal, thread_id, request_id),
            ).rowcount
            if changed != 1:
                raise RuntimeError("AG-UI execution claim was lost")
