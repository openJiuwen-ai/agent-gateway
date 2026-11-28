# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Workspace, pool and group gates that run before any Cron Worker is spawned."""

import json
import sys

import pytest

from agent_gateway.common.ingress_identity import LocalIdentityPolicy
from agent_gateway.common.worker_affinity import WorkerManifest
from agent_gateway.cron.ownership import CronOwnership
from agent_gateway.cron.store import FileCronJobStore
from tests.unit_tests.gateway.test_cron_preflight import raw_job


def workspace(monkeypatch, tmp_path, jobs=()):
    """Point the personal workspace at one Cron file; nothing else is read."""
    from agent_gateway.common import config, cron_settings

    store = tmp_path / "cron_jobs.json"
    store.write_text(json.dumps({"version": 1, "jobs": list(jobs)}), encoding="utf-8")
    monkeypatch.setattr(config, "get_config", dict)
    monkeypatch.setattr(cron_settings, "get_cron_jobs_path", lambda: store)
    return store


def snapshot(tmp_path):
    return {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}


@pytest.mark.parametrize("workers", [1, 2, 32])
async def test_cron_workspace_with_supported_jobs_passes_without_touching_the_file(monkeypatch, tmp_path, workers):
    from agent_gateway.application.multiprocess import validate_workspace

    store = workspace(monkeypatch, tmp_path, [
        raw_job("alice-task", user_id="alice"),
        raw_job("paused", user_id="bob", enabled=False, credential_ref="paused-login"),
    ])
    before = snapshot(tmp_path)
    await validate_workspace(cron=True, workers=workers)
    assert snapshot(tmp_path) == before and store.read_bytes() == before[store.relative_to(tmp_path)]


@pytest.mark.parametrize(
    "job,reason",
    [
        (raw_job("legacy-job", user_id=""), "legacy_owner"),
        (raw_job("default-job", user_id="default"), "legacy_owner"),
        (raw_job("login-job", credential_ref="do-not-print"), "credential_ref"),
        (raw_job("project-job", project_id="private-project"), "non_default_project"),
        (raw_job("tick-job", id="proactive-tick-auto", mode="proactive.tick"), "proactive"),
        (raw_job("target-job", targets="dingtalk"), "target"),
    ],
)
async def test_cron_workspace_names_blocking_jobs_but_no_private_text(monkeypatch, tmp_path, job, reason):
    from agent_gateway.application.multiprocess import validate_workspace

    workspace(monkeypatch, tmp_path, [job])
    before = snapshot(tmp_path)
    with pytest.raises(ValueError, match="Cron preflight rejected") as failure:
        await validate_workspace(cron=True, workers=2)
    assert f"{job['id']}: {reason}" in str(failure.value)
    assert "private task text" not in str(failure.value) and "do-not-print" not in str(failure.value)
    assert snapshot(tmp_path) == before


async def test_disabled_unsupported_job_is_a_warning_not_a_blocker(monkeypatch, tmp_path):
    from agent_gateway.application.cron_preflight import require_controlled_cron

    workspace(monkeypatch, tmp_path, [raw_job("paused", enabled=False, credential_ref="do-not-print",
                                             project_id="private-project")])
    report = await require_controlled_cron(CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy()), config={})
    assert report.ready and report.total == 1
    assert [(issue.reason, issue.blocking) for issue in report.issues] == [("credential_ref", False), ("non_default_project", False)]


async def test_proactive_configuration_blocks_cron_workspace(monkeypatch, tmp_path):
    from agent_gateway.application.multiprocess import validate_workspace
    from agent_gateway.common import config

    workspace(monkeypatch, tmp_path, [raw_job("alice-task", user_id="alice")])
    monkeypatch.setattr(config, "get_config", lambda: {"proactive_recommendation": {"enabled": True}})
    with pytest.raises(ValueError, match="proactive"):
        await validate_workspace(cron=True, workers=2)


async def test_cron_workspace_requires_a_file_backend(monkeypatch, tmp_path):
    from agent_gateway.application.multiprocess import validate_workspace
    from agent_gateway.common import config

    workspace(monkeypatch, tmp_path)
    monkeypatch.setattr(config, "get_config", lambda: {"gateway": {"cron": {"store_backend": "etcd"}}})
    # The trial keeps its one message for every unsupported Cron backend.
    with pytest.raises(ValueError, match="Cron is not supported by this trial"):
        await validate_workspace(cron=True, workers=2)


@pytest.mark.parametrize("cron,workers", [(1, 2), (True, 0), (True, 33), (True, True), (None, 2)])
async def test_cron_workspace_rejects_implicit_flags_and_worker_counts(monkeypatch, tmp_path, cron, workers):
    from agent_gateway.application.multiprocess import validate_workspace

    workspace(monkeypatch, tmp_path)
    with pytest.raises(ValueError):
        await validate_workspace(cron=cron, workers=workers)


async def test_workspace_without_cron_keeps_the_single_mode_gate(monkeypatch, tmp_path):
    from agent_gateway.application.multiprocess import validate_workspace

    store = workspace(monkeypatch, tmp_path, [raw_job("alice-task", user_id="alice")])
    before = snapshot(tmp_path)
    with pytest.raises(ValueError, match="use single mode"):
        await validate_workspace()
    # The same file is acceptable once Cron is explicitly enabled.
    await validate_workspace(cron=True, workers=2)
    assert snapshot(tmp_path) == before and store.exists()


async def test_require_controlled_cron_takes_ownership_not_a_manifest():
    """Regression: the CLI workspace gate once passed a WorkerManifest here."""
    from agent_gateway.application.cron_preflight import require_controlled_cron

    with pytest.raises(AttributeError):
        await require_controlled_cron(WorkerManifest((0, 1)))


async def test_pool_cron_manifest_must_be_the_full_range_with_control_services():
    from agent_gateway.processes.control_service import ControlServices
    from agent_gateway.processes.pool import WorkerPool

    with pytest.raises(ValueError, match="complete Worker range"):
        WorkerPool("ws://127.0.0.1:1", 2, cron_manifest=WorkerManifest((0,)))
    with pytest.raises(ValueError, match="requires control services"):
        WorkerPool("ws://127.0.0.1:1", 2, cron_manifest=WorkerManifest((0, 1)))
    with pytest.raises(ValueError, match="account services require"):
        WorkerPool("ws://127.0.0.1:1", 2, account_services=True)
    with pytest.raises(ValueError, match="explicit boolean"):
        WorkerPool("ws://127.0.0.1:1", 2, control_services=ControlServices(), account_services=1)
    pool = WorkerPool("ws://127.0.0.1:1", 2, control_services=ControlServices(),
                      cron_manifest=WorkerManifest((0, 1)), account_services=True)
    assert pool.cron_manifest.slots == pool.slots == (0, 1)
    # Activation is refused before the pool is ready, and never without Cron.
    with pytest.raises(RuntimeError, match="requires a ready Worker pool"):
        await pool.activate_cron()
    bare = WorkerPool("ws://127.0.0.1:1", 2)
    with pytest.raises(RuntimeError, match="not enabled"):
        await bare.activate_cron()


async def test_group_binds_one_cron_ownership_and_merges_control_operations():
    from agent_gateway.application.worker_group import WorkerGroup
    from agent_gateway.cron.control_commands import ControlCronCommands
    from agent_gateway.processes.control_service import ControlServiceIdentity

    with pytest.raises(ValueError, match="explicit boolean"):
        WorkerGroup("ws://127.0.0.1:1", workers=2, web_port=0, protocol_port=0, cron=1)
    for workers in (0, 33, True):
        with pytest.raises(ValueError, match="between 1 and 32"):
            WorkerGroup("ws://127.0.0.1:1", workers=workers, web_port=0, protocol_port=0)
    group = WorkerGroup("ws://127.0.0.1:1", workers=3, web_port=0, protocol_port=0, cron=True)
    assert isinstance(group._cron_commands, ControlCronCommands)
    assert isinstance(group._cron_ownership, CronOwnership)
    assert group._cron_ownership.manifest.slots == (0, 1, 2)
    assert group.pool.cron_manifest == group._cron_ownership.manifest
    assert group.pool._account_services is False
    handlers = group.pool._control_services.handlers_factory(ControlServiceIdentity("g", 0, 1, 2, 3))
    assert set(handlers) == {"cron.route_command"}
    plain = WorkerGroup("ws://127.0.0.1:1", workers=2, web_port=0, protocol_port=0)
    assert plain.pool.cron_manifest is None and plain.pool._control_services is None


def test_data_frame_gate_admits_only_owned_cron_panel_methods():
    from agent_gateway.application.worker_data import is_data_frame
    from agent_gateway.auth.passthrough import MODEL_AUTH_PARAM_KEY

    def frame(method, **params):
        return json.dumps({"type": "req", "id": "1", "method": method, "params": params})

    assert is_data_frame(frame("cron.job.list"), cron=True)
    assert is_data_frame(frame("cron.job.create", name="task"), cron=True)
    assert is_data_frame(frame("cron.job.update", patch={"enabled": False}), cron=True)
    assert is_data_frame(frame("cron.job.run_now"), cron=True)
    assert is_data_frame(frame("cron.run_active_tasks"), cron=True) is False
    assert is_data_frame(frame("cron.job.list")) is False
    assert is_data_frame(frame("config.save"), cron=True) is False
    assert is_data_frame(frame("chat.send", session_id="s", content="hi"), cron=True)
    # A credential selector is refused on every Cron frame, as on data frames.
    # ``_auth_session`` is not a gate concern: the owned ingress strips it.
    assert is_data_frame(frame("cron.job.create", credential_ref="private"), cron=True) is False
    assert is_data_frame(frame("cron.job.update", **{MODEL_AUTH_PARAM_KEY: "private"}), cron=True) is False
    assert is_data_frame(frame("cron.job.create", _jiuwen_auth_session="private"), cron=True) is False
    assert is_data_frame("not json", cron=True) is False
    assert is_data_frame(json.dumps([frame("cron.job.list")]), cron=True) is False


async def test_cron_push_admission_takes_only_the_cron_tool_frame_and_releases(monkeypatch):
    """A Worker has no general push callback; sharded Cron admits one frame."""
    from e2a.agent_models import AgentResponseChunk
    from e2a.wire_codec import encode_agent_chunk_for_wire
    from agent_gateway.application.worker_cron_commands import WorkerCronCommands
    from agent_gateway.application.worker_data import _admit_cron_tool_pushes

    class PushClient:
        handler = None

        def set_server_push_handler(self, handler):
            self.handler = handler

    class Control:
        async def call(self, operation, arguments, *, timeout):
            raise AssertionError("no command is routed in this check")

    class Handler:
        async def _handle_cron_push_payload(self, **kwargs):
            raise AssertionError("no command is executed in this check")

        async def _push_cron_command_ack(self, **kwargs):
            raise AssertionError("no command is answered in this check")

        async def _push_cron_run_now_ack(self, **kwargs):
            raise AssertionError("no command is answered in this check")

    commands = WorkerCronCommands(
        client=Control(), message_handler=Handler(),
        ownership=CronOwnership(WorkerManifest((0, 1)), LocalIdentityPolicy()), slot_id=0,
    )
    received = []

    async def handle_push(**kwargs):
        received.append(kwargs)

    monkeypatch.setattr(commands, "handle_push", handle_push)
    client = PushClient()
    release = _admit_cron_tool_pushes(client, commands)
    serve = client.handler

    def frame(payload, metadata):
        chunk = AgentResponseChunk(request_id="request-1", channel_id="web", payload=payload,
                                   is_complete=False)
        wire = encode_agent_chunk_for_wire(chunk, response_id="request-1", sequence=0)
        wire["metadata"] = metadata
        wire["session_id"] = "session-1"
        return json.loads(json.dumps(wire))

    cron_tool = {"event_type": "cron.response", "action": "list", "command_id": "command-1", "data": {}}
    owner = {"_jiuwenswarm_cron_owner_user_id": "owner"}
    # Other AgentServer pushes and unreadable frames keep being dropped.
    await serve(frame({"event_type": "chat.file", "name": "report"}, {}))
    await serve(frame({"event_type": "models.updated"}, {}))
    await serve({"not": "a chunk"})
    assert received == []

    await serve(frame(cron_tool, owner))
    assert received == [{"payload": cron_tool, "request_id": "request-1", "channel_id": "web",
                         "session_id": "session-1", "metadata": owner}]

    await release()
    assert client.handler is None
    # A delivery already scheduled when the Worker stops meets a closed door.
    await serve(frame(cron_tool, owner))
    assert len(received) == 1
    # A client without push support cannot host a Cron shard.
    with pytest.raises(ValueError, match="server-push support"):
        _admit_cron_tool_pushes(object(), commands)


async def test_data_worker_requires_a_control_client_and_a_manifest_slot(monkeypatch):
    from agent_gateway.application.worker_data import data_worker
    from agent_gateway.southbound.orchestration import client as client_module

    class StopHere(Exception):
        pass

    class Client:
        async def connect(self, url):
            raise StopHere

        async def disconnect(self):
            pass

    monkeypatch.setattr(client_module, "assemble_client", lambda *args, **kwargs: Client())
    manifest = WorkerManifest((0, 1))
    for arguments in ({"cron_slot": 0}, {"cron_manifest": manifest},
                      {"cron_slot": 2, "cron_manifest": manifest},
                      {"cron_slot": "0", "cron_manifest": manifest},
                      {"cron_slot": 0, "cron_manifest": (0, 1)}):
        with pytest.raises(ValueError, match="manifest and slot"):
            async with data_worker("ws://127.0.0.1:1", **arguments):
                pytest.fail("invalid Cron arguments were accepted")
    with pytest.raises(ValueError, match="Control client"):
        async with data_worker("ws://127.0.0.1:1", cron_slot=0, cron_manifest=manifest):
            pytest.fail("Cron without a Control client was accepted")
    # Valid arguments reach the southbound connection; nothing else is built.
    with pytest.raises(StopHere):
        async with data_worker("ws://127.0.0.1:1", control_client=object(), accounts=False,
                               cron_slot=1, cron_manifest=manifest):
            pytest.fail("unreachable")


@pytest.mark.parametrize(
    "arguments,expected",
    [
        (["--enable-cron"], "--enable-cron requires --mode multiprocess"),
        (["--check-cron"], "--check-cron requires --mode multiprocess --enable-cron"),
        (["--mode", "multiprocess", "--check-cron"],
         "--check-cron requires --mode multiprocess --enable-cron"),
        (["--check-cron", "--enable-cron"], "--enable-cron requires --mode multiprocess"),
    ],
)
def test_cli_rejects_cron_flags_outside_the_multiprocess_trial(monkeypatch, capsys, arguments, expected):
    from agent_gateway import app_gateway

    monkeypatch.setattr(sys, "argv", ["agent-gateway", *arguments])
    with pytest.raises(SystemExit) as error:
        app_gateway.main()
    assert error.value.code == 2
    assert expected in capsys.readouterr().err


def test_cli_options_require_explicit_cron_and_account_flags(monkeypatch):
    from agent_gateway.application.multiprocess import MultiprocessOptions

    for keyword in ("cron", "accounts"):
        with pytest.raises(ValueError, match="explicit flags"):
            MultiprocessOptions.from_cli(
                workers=None, web_host="127.0.0.1", web_port=0, web_path="/ws", web_ws_deflate=False,
                required_client_type=None, **{keyword: 1},
            )
    options = MultiprocessOptions.from_cli(
        workers=None, web_host="127.0.0.1", web_port=0, web_path="/ws", web_ws_deflate=False,
        required_client_type=None, cron=True,
    )
    assert (options.workers, options.cron, options.accounts) == (2, True, False)
