# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.

"""Host-completion waits die with the Session that owns them."""

from types import SimpleNamespace

from agent_gateway.channel_manager.web.app_web_handlers import _bind_session_close_release
from agent_gateway.message_handler.message_handler import MessageHandler
from tests._support.null_session_login_bindings import NullSessionLoginBindings


def _new_handler() -> MessageHandler:
    setattr(MessageHandler, "_instance", None)
    return MessageHandler(
        SimpleNamespace(), session_login_bindings=NullSessionLoginBindings()
    )


def test_release_session_completion_waits_drops_only_that_session() -> None:
    handler = _new_handler()
    handler._awaiting_host_completion.update(
        {"req-1": "sess-1", "req-2": "sess-1", "req-3": "sess-2"}
    )

    released = handler.release_session_completion_waits("sess-1")

    assert released == 2
    assert handler._awaiting_host_completion == {"req-3": "sess-2"}


def test_release_session_completion_waits_ignores_unknown_session() -> None:
    handler = _new_handler()
    handler._awaiting_host_completion.update({"req-1": "sess-1"})

    assert handler.release_session_completion_waits("sess-other") == 0
    assert handler.release_session_completion_waits("") == 0
    assert handler._awaiting_host_completion == {"req-1": "sess-1"}


def test_web_channel_session_close_is_wired_to_release() -> None:
    handler = _new_handler()
    handler._awaiting_host_completion.update({"req-1": "sess-1"})
    registered = []

    channel = SimpleNamespace(
        set_session_close_hook=registered.append,
    )

    _bind_session_close_release(channel, handler)

    assert registered == [handler.release_session_completion_waits]
    assert registered[0]("sess-1") == 1
    assert handler._awaiting_host_completion == {}
