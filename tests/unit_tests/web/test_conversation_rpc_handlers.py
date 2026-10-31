from types import SimpleNamespace

from agent_gateway.channel_manager.web.chat_rpc_handlers import (
    register_chat_send_handler,
)
from agent_gateway.channel_manager.web.conversation_rpc_handlers import (
    register_conversation_handlers,
)
from agent_gateway.channel_manager.web.media_document_rpc_handlers import (
    register_media_document_handlers,
)


class _FakeChannel:
    def __init__(self):
        self.methods = {}

    def register_method(self, name, handler):
        self.methods[name] = handler


def test_chat_media_and_conversation_routes_are_registered_in_original_order():
    channel = _FakeChannel()
    register_chat_send_handler(channel)
    register_media_document_handlers(channel, SimpleNamespace(), lambda ref: ref)
    register_conversation_handlers(channel, SimpleNamespace(), lambda ref: ref)

    assert list(channel.methods) == [
        "chat.send",
        "media.persist",
        "media.discard",
        "document.persist",
        "document.formats",
        "chat.resume",
        "chat.interrupt",
        "chat.user_answer",
        "chat.swarmflow_reply",
        "history.get",
        "topic_starters.profile",
        "topic_starters.restore_direction",
        "topic_starters.control",
        "topic_starters.status",
        "topic_starters.select_model",
        "topic_starters.get",
        "topic_starters.foreground",
        "topic_starters.event",
        "prompt_suggestion.event",
    ]
