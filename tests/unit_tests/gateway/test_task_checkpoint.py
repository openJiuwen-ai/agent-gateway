# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Exercise Gateway checkpoint transactions without importing Swarm's store."""

import json
import sqlite3
from contextlib import contextmanager

import pytest

from agent_gateway.extensions.video_duplex.backend.tasks.bridge import GatewayTaskEndpoint


class CheckpointStore:
    """SQLite fixture implementing only the endpoint's injected store contract."""

    def __init__(self, task):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE tasks (id TEXT PRIMARY KEY, payload TEXT)")
        self.db.execute("CREATE TABLE commands (id TEXT PRIMARY KEY, fingerprint TEXT, result TEXT)")
        self.put(self.db, task)
        self.db.commit()

    @contextmanager
    def transaction(self):
        with self.db:
            yield self.db

    @staticmethod
    def get(db, task_id):
        return json.loads(db.execute("SELECT payload FROM tasks WHERE id = ?", (task_id,)).fetchone()[0])

    @staticmethod
    def put(db, task):
        db.execute("INSERT OR REPLACE INTO tasks VALUES (?, ?)", (task["id"], json.dumps(task)))

    @staticmethod
    def replay(db, owner, session, command_id, command):
        fingerprint = json.dumps(command, sort_keys=True)
        row = db.execute("SELECT fingerprint, result FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is not None:
            if row[0] != fingerprint:
                raise ValueError("Command ID reused")
            return json.loads(row[1]), fingerprint
        return None, fingerprint

    @staticmethod
    def command(db, task, command_id, fingerprint, result):
        db.execute("INSERT INTO commands VALUES (?, ?, ?)", (command_id, fingerprint, json.dumps(result)))


@pytest.fixture
def endpoint():
    store = CheckpointStore(dict(
        id="task", owner="alice", session="chat", core_session_id="core", request_id="request",
        status="running", sequence=1, checkpoint_open=True, changes=[],
    ))
    current = GatewayTaskEndpoint(store)
    yield current
    current.close()
    store.db.close()


def command(action, **extra):
    return dict(action=action, owner="alice", session_id="core", request_id="request",
                task_id="task", command_id=action, **extra)


@pytest.mark.parametrize("status,cancelled", [("cancelling", False), ("cancelled", True)])
def test_revoked_admission_is_acknowledged_and_replay_is_idempotent(endpoint, status, cancelled):
    with endpoint.store.transaction() as db:
        task = endpoint.store.get(db, "task")
        task.update(status=status, execution_cancelled=cancelled)
        endpoint.store.put(db, task)
    assert endpoint.execute(command("bind")) == {"cancelled": True}
    stored = endpoint.store.get(endpoint.store.db, "task")
    assert not stored.get("execution_bound") and not stored["checkpoint_open"]
    assert stored["execution_settled"] and stored["execution_cancelled"] and stored["output_closed"]
    assert stored["sequence"] == 2
    assert endpoint.execute(command("bind")) == {"cancelled": True}
    assert endpoint.store.get(endpoint.store.db, "task") == stored


def test_cancelled_settlement_closes_checkpoint_atomically(endpoint):
    endpoint.execute(command("bind"))
    assert endpoint.execute(command("settle", cancelled=True)) == {}
    stored = endpoint.store.get(endpoint.store.db, "task")
    assert not stored["checkpoint_open"]
    assert stored["execution_settled"] and stored["execution_cancelled"]
    assert stored["sequence"] == 3
    assert endpoint.execute(command("settle", cancelled=True)) == {}
    assert endpoint.store.get(endpoint.store.db, "task") == stored


def test_normal_settlement_requires_close_and_failed_command_is_not_saved(endpoint):
    endpoint.execute(command("bind"))
    with pytest.raises(RuntimeError, match="still open"):
        endpoint.execute(command("settle"))
    stored = endpoint.store.get(endpoint.store.db, "task")
    assert stored["checkpoint_open"] and not stored.get("execution_settled")
    assert stored["sequence"] == 2
    endpoint.execute(command("close"))
    endpoint.execute(command("settle"))
    stored = endpoint.store.get(endpoint.store.db, "task")
    assert stored["execution_settled"] and not stored["execution_cancelled"]


@pytest.mark.parametrize("field,value", [("owner", "bob"), ("session_id", "other"), ("request_id", "stale")])
def test_revoked_admission_still_rejects_foreign_execution(endpoint, field, value):
    with endpoint.store.transaction() as db:
        task = endpoint.store.get(db, "task")
        task["status"] = "cancelling"
        endpoint.store.put(db, task)
    before = endpoint.store.get(endpoint.store.db, "task")
    request = command("bind")
    request[field] = value
    with pytest.raises(ValueError, match="STALE"):
        endpoint.execute(request)
    assert endpoint.store.get(endpoint.store.db, "task") == before
