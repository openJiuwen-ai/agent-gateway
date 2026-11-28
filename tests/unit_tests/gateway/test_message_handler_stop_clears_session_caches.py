"""停机时必须一并释放会话级缓存，否则单例复用会把请求指回旧会话。"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent_gateway.message_handler.message_handler import MessageHandler
from tests._support.null_session_login_bindings import NullSessionLoginBindings


@pytest.fixture(autouse=True)
def _reset_message_handler_singleton():
    """``MessageHandler`` 是进程内单例，用例前后都要重置，避免跨模块污染。"""
    MessageHandler._instance = None
    yield
    MessageHandler._instance = None


def _handler() -> MessageHandler:
    return MessageHandler(
        SimpleNamespace(),
        session_login_bindings=NullSessionLoginBindings(),
    )


@pytest.mark.asyncio
async def test_stop_forwarding_clears_session_scoped_caches() -> None:
    """``_session_last_originator`` 与两张别名表要和 ``_session_last_user_query`` 一起清。"""
    handler = _handler()
    handler._session_last_user_query["sess-1"] = "hello"
    handler._session_last_originator["sess-1"] = ("feishu", "alice")
    handler._acp_session_aliases["ext-1"] = "int-1"
    handler._external_session_aliases[("acp", "sess-1")] = "ext-1"

    await handler.stop_forwarding()

    assert handler._session_last_user_query == {}
    assert handler._session_last_originator == {}
    assert handler._acp_session_aliases == {}
    assert handler._external_session_aliases == {}
