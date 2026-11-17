# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest


from agent_gateway.auth import login_credentials as lc
from agent_gateway.auth.login_credentials import credential_ref_for_user
from agent_gateway.message_handler.message_handler import MessageHandler

REF = "abe633f3a47a2758174eabe9160daf36"


@pytest.fixture(autouse=True)
def _clean_registry():
    lc.reset_for_test()
    yield
    lc.reset_for_test()


def _handler(sent: list) -> MessageHandler:
    handler = object.__new__(MessageHandler)

    async def _send(env):
        sent.append(env)
        return SimpleNamespace(ok=True, payload={"applied": True})

    handler._send_non_stream_agent_request = _send
    return handler


def _refresh_chunk(ref: str = REF) -> SimpleNamespace:
    return SimpleNamespace(
        payload={"event_type": lc.CREDENTIAL_REFRESH_EVENT, "credential_ref": ref}
    )


@pytest.mark.asyncio
async def test_refresh_request_is_answered_with_an_update(monkeypatch):
    seen_refs: list[str] = []

    def _refresh(ref):
        seen_refs.append(ref)
        return {"credential_ref": ref, "api_key": "id-new"}

    monkeypatch.setattr(
        "agent_gateway.auth.passthrough.refreshed_credential_for_ref", _refresh
    )
    sent: list = []

    assert await _handler(sent)._handle_login_credential_refresh_push(_refresh_chunk()) is True

    assert seen_refs == [REF]
    assert len(sent) == 1
    assert sent[0].method == 'auth.credentials.update'
    assert sent[0].params == {"credential_ref": REF, "api_key": "id-new"}


@pytest.mark.asyncio
async def test_refresh_runs_off_the_event_loop(monkeypatch):
    loop_thread = threading.get_ident()
    ran_in: list[int] = []

    def _refresh(ref):
        del ref
        ran_in.append(threading.get_ident())
        return None

    monkeypatch.setattr(
        "agent_gateway.auth.passthrough.refreshed_credential_for_ref", _refresh
    )
    await _handler([])._handle_login_credential_refresh_push(_refresh_chunk())
    assert ran_in and ran_in[0] != loop_thread


@pytest.mark.asyncio
async def test_failed_refresh_sends_nothing_and_is_still_consumed(monkeypatch):
    def _boom(ref):
        del ref
        raise RuntimeError("exchange service down")

    monkeypatch.setattr("agent_gateway.auth.passthrough.refreshed_credential_for_ref", _boom)
    sent: list = []
    assert await _handler(sent)._handle_login_credential_refresh_push(_refresh_chunk()) is True
    assert sent == []


@pytest.mark.asyncio
async def test_other_pushes_are_not_touched():
    sent: list = []
    handler = _handler(sent)
    assert await handler._handle_login_credential_refresh_push(
        SimpleNamespace(payload={"event_type": "cron.response"})
    ) is False
    assert await handler._handle_login_credential_refresh_push(
        SimpleNamespace(payload=None)
    ) is False
    assert sent == []


@pytest.mark.asyncio
async def test_logout_revokes_the_agent_server_credential(monkeypatch):
    from agent_gateway.channel_manager.web import web_http_auth

    monkeypatch.setattr(lc, "_ref_key_cache", b"k" * 32)
    sent: list = []
    handler = _handler(sent)
    monkeypatch.setattr(MessageHandler, "try_get_instance", classmethod(lambda cls: handler))

    await web_http_auth._revoke_agent_server_credential("openid-1")

    assert len(sent) == 1
    assert sent[0].method == 'auth.credentials.update'
    assert sent[0].params == {
        "credential_ref": credential_ref_for_user("openid-1"),
        "revoked": True,
    }


@pytest.mark.asyncio
async def test_logout_revoke_is_best_effort(monkeypatch):
    from agent_gateway.channel_manager.web import web_http_auth

    monkeypatch.setattr(lc, "_ref_key_cache", b"k" * 32)
    monkeypatch.setattr(MessageHandler, "try_get_instance", classmethod(lambda cls: None))
    await web_http_auth._revoke_agent_server_credential("openid-1")

    handler = object.__new__(MessageHandler)

    async def _down(_env):
        raise ConnectionError("agent server gone")

    handler._send_non_stream_agent_request = _down
    monkeypatch.setattr(MessageHandler, "try_get_instance", classmethod(lambda cls: handler))
    await web_http_auth._revoke_agent_server_credential("openid-1")
