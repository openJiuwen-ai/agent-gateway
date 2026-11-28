# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""The static route directory reaches every Worker slot and nothing else changes."""

from contextlib import asynccontextmanager
import asyncio
import json
import os
import re
import socket
import sys
import types
from typing import Any

import pytest

from agent_gateway.application.worker_routed_client import RoutedTargetsUnreachable
from agent_gateway.common.ingress_identity import LocalIdentityPolicy, resolve_local_identity
from agent_gateway.common.southbound_routes import MAX_ROUTE_DIRECTORY_BYTES, SouthboundTarget, StaticRouteDirectory
from agent_gateway.common.worker_affinity import FixedUserHash, WorkerManifest


def _user_of_slot(slot: int, slots: tuple[int, ...], *, start: int = 0) -> str:
    policy = LocalIdentityPolicy()
    affinity = FixedUserHash(WorkerManifest(slots), policy)
    for index in range(start, start + 5000):
        user = f"user-{index}"
        identity = resolve_local_identity(f"user_id={user}".encode(), (), entry="web", policy=policy)
        if affinity.select(identity) == slot:
            return user
    raise AssertionError("no user for slot")


def directory(*targets: SouthboundTarget, slots: tuple[int, ...] = (0, 1)) -> StaticRouteDirectory:
    return StaticRouteDirectory(revision=3, identity_policy=LocalIdentityPolicy(), manifest=WorkerManifest(slots),
                                targets=targets)


def target(user: str, index: int, slot: int, port: int) -> SouthboundTarget:
    return SouthboundTarget(
        target_id=f"target-{index}", physical_id=f"physical-{index}", generation=2, user_id=user,
        agent_type="jiuwenswarm", owner_slot=slot, ws_url=f"ws://127.0.0.1:{port}/agent",
    )


def unused_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture
def routes() -> StaticRouteDirectory:
    return directory(
        target(_user_of_slot(0, (0, 1)), 0, 0, unused_port()),
        target(_user_of_slot(1, (0, 1)), 1, 1, unused_port()),
    )


def _write(tmp_path, encoded: str):
    path = tmp_path / "routes.json"
    path.write_text(encoded, encoding="utf-8")
    return str(path)


def _from_cli(**overrides):
    from agent_gateway.application.multiprocess import MultiprocessOptions

    arguments = dict(
        workers=2, web_host="127.0.0.1", web_port=0, web_path="/ws", web_ws_deflate=False,
        required_client_type=None,
    )
    arguments.update(overrides)
    return MultiprocessOptions.from_cli(**arguments)


def test_cli_loads_the_directory_and_keeps_every_other_option_default(tmp_path, routes):
    options = _from_cli(southbound_routes_file=_write(tmp_path, routes.to_json()))
    assert options.southbound_routes is not None
    assert options.southbound_routes.to_json() == routes.to_json()
    assert options.southbound_routes.targets_for_slot(1)[0].owner_slot == 1
    assert (options.workers, options.accounts, options.cron) == (2, False, False)
    # The file is a snapshot: nothing is written next to it.
    assert [path.name for path in tmp_path.iterdir()] == ["routes.json"]


def test_cli_refuses_directories_that_do_not_fit_the_run(tmp_path, routes):
    from agent_gateway.application.multiprocess import load_static_routes

    path = _write(tmp_path, routes.to_json())
    with pytest.raises(ValueError, match="exactly the --workers slots"):
        load_static_routes(path, workers=3)
    with pytest.raises(ValueError, match="exactly the --workers slots"):
        _from_cli(workers=1, southbound_routes_file=path)
    assert _from_cli(accounts=True, southbound_routes_file=path).accounts
    options = _from_cli(accounts=True, cron=True, southbound_routes_file=path)
    assert options.accounts and options.cron


@pytest.mark.parametrize("encoded", ["", "{", "[]", "{}", "not json", '{"schema_version":2}',
                                     '{"schema_version":1,"extra":1}'])
def test_cli_refuses_invalid_directory_files(tmp_path, encoded):
    with pytest.raises(ValueError, match="invalid --southbound-routes-file"):
        _from_cli(southbound_routes_file=_write(tmp_path, encoded))


def test_cli_refuses_missing_and_oversized_files(tmp_path):
    with pytest.raises(ValueError, match="cannot read --southbound-routes-file"):
        _from_cli(southbound_routes_file=str(tmp_path / "missing.json"))
    big = tmp_path / "big.json"
    big.write_bytes(b" " * (MAX_ROUTE_DIRECTORY_BYTES + 1))
    with pytest.raises(ValueError, match="invalid --southbound-routes-file"):
        _from_cli(southbound_routes_file=str(big))
    from agent_gateway.application.multiprocess import load_static_routes

    with pytest.raises(ValueError, match="requires a path"):
        load_static_routes("  ", workers=2)
    assert load_static_routes.__doc__


def test_directory_round_trips_through_argv_for_every_slot(routes):
    # A Worker receives the directory as one bounded argument.
    encoded = routes.to_json()
    assert "\n" not in encoded and len(encoded.encode()) <= MAX_ROUTE_DIRECTORY_BYTES
    restored = StaticRouteDirectory.from_json(encoded)
    assert restored == routes
    assert [item.user_id for item in restored.targets_for_slot(0)] == [_user_of_slot(0, (0, 1))]


def test_cli_option_requires_multiprocess_mode(monkeypatch, capsys, tmp_path):
    from agent_gateway import app_gateway

    monkeypatch.setattr(sys, "argv", ["agent-gateway", "--southbound-routes-file", "routes.json"])
    with pytest.raises(SystemExit) as error:
        app_gateway.main()
    assert error.value.code == 2
    assert "--southbound-routes-file requires --mode multiprocess" in capsys.readouterr().err


async def test_pool_hands_the_directory_to_every_slot_and_refuses_a_mixed_source(routes):
    from agent_gateway.processes.pool import WorkerPool

    pool = WorkerPool(None, 2, southbound_routes=routes)
    assert pool.southbound_routes is routes and pool.slots == (0, 1)
    with pytest.raises(ValueError, match="directory only"):
        WorkerPool("ws://127.0.0.1:18092", 2, southbound_routes=routes)
    with pytest.raises(ValueError, match="requires an AgentServer URL or a static route directory"):
        WorkerPool(None, 2)
    with pytest.raises(ValueError, match="complete Worker range"):
        WorkerPool(None, 3, southbound_routes=routes)
    with pytest.raises(ValueError, match="explicit static route directory"):
        WorkerPool(None, 2, southbound_routes=routes.to_json())


async def test_pool_spawn_arguments_name_the_static_marker_and_the_directory(monkeypatch, routes):
    from agent_gateway.processes import pool as pool_module
    from agent_gateway.processes.pool import STATIC_SOUTHBOUND

    seen: list[tuple] = []

    async def capture(_program, *arguments, **_options):
        seen.append(arguments)
        raise RuntimeError("stop before any Worker starts")

    monkeypatch.setattr(pool_module.asyncio, "create_subprocess_exec", capture)
    pool = pool_module.WorkerPool(None, 2, southbound_routes=routes)
    with pytest.raises(RuntimeError, match="stop before any Worker starts"):
        await pool.start()
    # argv = interpreter, -m, module, manager fd, transfer fd, AgentServer or marker, options
    arguments = seen[0]
    assert arguments[:2] == ("-m", "agent_gateway.processes.worker")
    assert arguments[4] == STATIC_SOUTHBOUND
    assert arguments[-2:] == ("--southbound-routes-json", routes.to_json())
    assert "--accounts" not in arguments and "--cron-slot" not in arguments
    single = pool_module.WorkerPool("ws://127.0.0.1:18092", 2)
    seen.clear()
    with pytest.raises(RuntimeError, match="stop before any Worker starts"):
        await single.start()
    assert seen[0][4] == "ws://127.0.0.1:18092" and "--southbound-routes-json" not in seen[0]


async def test_group_serves_static_routes_without_the_single_server_contract(routes):
    from agent_gateway.application.worker_group import WorkerGroup

    group = WorkerGroup(None, workers=2, web_port=0, protocol_port=0, southbound_routes=routes)
    assert group.pool.southbound_routes is routes
    with pytest.raises(ValueError, match="directory only"):
        WorkerGroup("ws://127.0.0.1:18092", workers=2, southbound_routes=routes)
    with pytest.raises(ValueError, match="requires an AgentServer URL or a static route directory"):
        WorkerGroup(workers=2)
    cron_group = WorkerGroup(None, workers=2, cron=True, southbound_routes=routes)
    assert cron_group.pool.cron_manifest == routes.manifest
    account_group = WorkerGroup(None, workers=2, auth_service_provider=lambda: None, southbound_routes=routes)
    assert account_group.pool.southbound_routes is routes
    with pytest.raises(ValueError, match="Worker range"):
        WorkerGroup(None, workers=1, southbound_routes=routes)
    with pytest.raises(ValueError, match="between 1 and 32"):
        WorkerGroup(None, workers="2", southbound_routes=routes)


def test_worker_role_parses_the_directory_once_and_strictly(routes):
    from agent_gateway.processes.worker import _parse_role_arguments, _parse_worker_role

    encoded = routes.to_json()
    # A Control fd alone keeps the original account contract.
    assert _parse_worker_role(["7", "--southbound-routes-json", encoded]) == (
        7, True, None, None, routes,
    )
    assert _parse_worker_role(["--southbound-routes-json", encoded]) == (None, False, None, None, routes)
    # The single-AgentServer contract of the role parser is unchanged.
    assert _parse_role_arguments(["7", "--accounts"]) == (7, True, None, None)
    assert _parse_role_arguments([]) == (None, False, None, None)
    with pytest.raises(ValueError, match="must be a number"):
        _parse_role_arguments(["ws://x"])
    with pytest.raises(ValueError, match="requires one directory"):
        _parse_worker_role(["--southbound-routes-json", encoded, "--southbound-routes-json", encoded])
    with pytest.raises(ValueError, match="requires one directory"):
        _parse_worker_role(["--southbound-routes-json"])
    with pytest.raises(ValueError, match="invalid static southbound route JSON"):
        _parse_worker_role(["--southbound-routes-json", "{}"])
    with pytest.raises(ValueError, match="unknown Worker argument"):
        _parse_worker_role(["--southbound-routes-file", "x.json"])


# ---------------------------------------------------------------------- #
# The Worker entry point reports why a static Worker could not serve.
# It is driven in-process over a real management socket pair; only the
# data assembly is replaced.
# ---------------------------------------------------------------------- #

def _management(kind: str, slot: int, pid: int | None = None, **fields) -> dict:
    return {"protocol_version": 1, "group_id": "static-group", "slot_id": slot, "generation": 1,
            "type": kind, **({"pid": pid} if pid is not None else {}), **fields}


@asynccontextmanager
async def _static_worker(monkeypatch, *, directory, url, failure):
    """A Worker whose data assembly records its role and raises ``failure``."""
    from agent_gateway.processes.channel import ManagementChannel
    from agent_gateway.processes.pool import STATIC_SOUTHBOUND
    from agent_gateway.processes import worker

    builds = []

    @asynccontextmanager
    async def data_worker(agent_server_url, *, control_client=None, on_credentials=None, accounts=True,
                          cron_slot=None, cron_manifest=None, southbound_routes=None, slot_id=None):
        builds.append({"url": agent_server_url, "directory": southbound_routes, "slot": slot_id})
        raise failure
        yield  # pragma: no cover - the assembly fails before it yields

    module = types.ModuleType("agent_gateway.application.worker_data")
    module.data_worker = data_worker
    monkeypatch.setitem(sys.modules, "agent_gateway.application.worker_data", module)
    management_parent, management_child = socket.socketpair()
    transfer_parent, transfer_child = socket.socketpair()
    channel = ManagementChannel(management_parent)
    task = asyncio.create_task(worker._run(
        management_child.detach(), transfer_child.detach(), url, accounts=False,
        southbound_routes=directory if url == STATIC_SOUTHBOUND else None,
    ))
    try:
        yield channel, task, builds
    finally:
        channel.close()
        transfer_parent.close()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def _expect_failure(
    channel, task, code: str, *, slot: int = 0, ready: bool = True, error: Any = None,
) -> None:
    """The management frames of a Worker that failed, then its own exception.

    ``ready`` is False when it failed before it could say READY. ``error`` is
    the exception the task ends with when that is not the failure itself.
    """
    from agent_gateway.processes import worker

    if ready:
        assert await channel.receive(timeout=5) == _management(
            "READY", slot, os.getpid(), stage="management_ready",
        )
    assert await channel.receive(timeout=5) == _management("FAILED", slot, os.getpid(), error_code=code)
    expected = worker._WorkerFailure(code) if error is None else error
    with pytest.raises(type(expected), match=re.escape(str(expected))):
        await asyncio.wait_for(task, 5)


@pytest.mark.parametrize("failure, code", [
    (RoutedTargetsUnreachable("static targets unreachable at activation: target-0"), "SOUTHBOUND_UNREACHABLE"),
    (ValueError("static routes do not serve accounts in this slice"), "SOUTHBOUND_CONFIGURATION"),
])
async def test_static_worker_reports_why_it_could_not_serve(monkeypatch, routes, failure, code):
    from agent_gateway.processes.pool import STATIC_SOUTHBOUND

    async with _static_worker(monkeypatch, directory=routes, url=STATIC_SOUTHBOUND, failure=failure) as (
        channel, task, builds,
    ):
        await channel.send(_management("INIT", 0), timeout=2)
        await _expect_failure(channel, task, code)
    # The Worker received its directory and slot, and no single AgentServer URL.
    assert builds == [{"url": None, "directory": routes, "slot": 0}]


async def test_a_slot_outside_the_directory_is_a_configuration_failure(monkeypatch, routes):
    from agent_gateway.processes.pool import STATIC_SOUTHBOUND

    async with _static_worker(monkeypatch, directory=routes, url=STATIC_SOUTHBOUND, failure=None) as (
        channel, task, builds,
    ):
        # The parent names slot 7; the directory covers 0 and 1 only.
        await channel.send(_management("INIT", 7), timeout=2)
        await _expect_failure(channel, task, "SOUTHBOUND_CONFIGURATION", slot=7, ready=False)
    assert builds == []


async def test_the_static_marker_without_a_directory_is_a_configuration_failure(monkeypatch):
    from agent_gateway.processes.pool import STATIC_SOUTHBOUND

    async with _static_worker(monkeypatch, directory=None, url=STATIC_SOUTHBOUND, failure=None) as (
        channel, task, builds,
    ):
        await channel.send(_management("INIT", 0), timeout=2)
        await _expect_failure(channel, task, "SOUTHBOUND_CONFIGURATION", ready=False)
    assert builds == []


async def test_a_single_server_worker_keeps_its_generic_failure_for_a_bad_assembly(monkeypatch):
    async with _static_worker(
        monkeypatch, directory=None, url="ws://127.0.0.1:8000/agent", failure=ValueError("unrelated"),
    ) as (channel, task, builds):
        await channel.send(_management("INIT", 0), timeout=2)
        # It reports the generic code and, as before, re-raises its own error.
        await _expect_failure(channel, task, "WORKER_FAILURE", error=ValueError("unrelated"))
    assert builds == [{"url": "ws://127.0.0.1:8000/agent", "directory": None, "slot": None}]
