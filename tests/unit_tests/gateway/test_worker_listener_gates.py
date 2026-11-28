# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""A Worker listener admits only the personal compatibility installed for it."""

import json

import pytest

from agent_gateway.application.personal_tui import PERSONAL_TUI_METHODS
from agent_gateway.application.worker_data import (
    PERSONAL_WEB_WORKER_METHODS, guarded_web_methods, listener_frame_gates, personal_compatibility,
    personal_web_methods,
)

LOOPBACK = "ws://127.0.0.1:8080"
REMOTE = "ws://10.0.0.5:8080"
# The requests whose guard needs a loopback AgentServer.
GUARDED_WEB = guarded_web_methods()


def frame(method, **params):
    return json.dumps({"type": "req", "id": "1", "method": method, "params": params})


def gates(url, *, personal):
    return listener_frame_gates(url, personal=personal)


@pytest.mark.parametrize("url", [LOOPBACK, "ws://localhost:8080", "ws://[::1]:8080"])
def test_personal_worker_with_a_loopback_agentserver_admits_what_it_guards(url):
    protocol, web = gates(url, personal=True)
    assert personal_compatibility(url, personal=True) == (True, True)
    for method in sorted(PERSONAL_TUI_METHODS):
        assert protocol(frame(method), tui=True), method
        assert not protocol(frame(method)), method  # /acp and the other routes keep it out
    for method in sorted(personal_web_methods()):
        assert web(frame(method)), method
    # What no Worker answers on these listeners.
    for method in ("cron.job.list", "session.create", "models.replace_all"):
        assert not web(frame(method)) and not protocol(frame(method), tui=True), method


def test_personal_worker_with_a_remote_agentserver_keeps_unguarded_requests_out():
    # The guards are installed for a loopback AgentServer only. Where they are
    # not, the requests they would have checked stay closed instead of reaching
    # the regular handlers without them.
    protocol, web = gates(REMOTE, personal=True)
    assert personal_compatibility(REMOTE, personal=True) == (False, False)
    # The two families do not overlap: the regular reads are not guarded, the
    # guarded ones are not regular reads.
    assert GUARDED_WEB and GUARDED_WEB.isdisjoint(PERSONAL_WEB_WORKER_METHODS)
    assert personal_web_methods() == PERSONAL_WEB_WORKER_METHODS | GUARDED_WEB
    for method in sorted(GUARDED_WEB):
        assert not web(frame(method)), method
    for method in sorted(PERSONAL_TUI_METHODS):
        assert not protocol(frame(method), tui=True), method
    # The regular reads and the data plane do not depend on the guards.
    for method in sorted(PERSONAL_WEB_WORKER_METHODS):
        assert web(frame(method)), method
    assert protocol(frame("history.get", session_id="s")) and protocol(frame("chat.send", session_id="s"))


def test_account_worker_admits_no_personal_compatibility():
    for url in (LOOPBACK, REMOTE):
        protocol, web = gates(url, personal=False)
        assert personal_compatibility(url, personal=False) == (False, False)
        for method in ("command.diff", "session.rewind", "config.get", "models.list"):
            assert not protocol(frame(method), tui=True), method
        for method in sorted(personal_web_methods()):
            assert not web(frame(method)), method
    # Its data plane and ACP are as before. The Huawei MaaS source is the
    # account flow itself, so only the raw private keys are dropped.
    protocol, web = gates(LOOPBACK, personal=False)
    assert protocol(frame("history.get", session_id="s")) and web(frame("chat.send", session_id="s"))
    assert protocol(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize"}), acp=True)
    assert web(frame("chat.send", session_id="s", source="huawei-maas-login"))
    assert not web(frame("chat.send", session_id="s", credential_ref="c"))


def test_personal_gates_drop_private_authority_whatever_the_agentserver():
    for url in (LOOPBACK, REMOTE):
        protocol, web = gates(url, personal=True)
        for key in ("credential_ref", "_jiuwen_auth_session"):
            assert not web(frame("history.get", session_id="s", **{key: "x"})), key
            assert not protocol(frame("chat.send", session_id="s", **{key: "x"})), key
        assert not web(frame("chat.send", session_id="s", source="huawei-maas-login"))
        assert not protocol(frame("chat.send", session_id="s", model_source="huawei-maas-login"))
