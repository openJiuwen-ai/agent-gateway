"""welink assistant 的消息去重表必须有上限。

``_delivered`` 每处理一条 mention 就增加一条，``_aliases`` 每对齐一条历史消息
就增加一条，两者都没有删除路径，长期运行会随群消息数无界增长。
"""

from agent_gateway.channel_manager.im_platforms.welink.welink_assistant_link import (
    _ALIAS_LIMIT,
    _DELIVERED_LIMIT,
    AssistantLink,
)


class _BoundedLink(AssistantLink):
    @property
    def aliases(self):
        return self._aliases

    @property
    def delivered(self):
        return self._delivered

    def remember_alias(self, group_id: str, message_id: str, canonical: str) -> None:
        with self._lock:
            self._remember_alias(group_id, message_id, canonical)

    def remember_delivered(self, group_id: str, message_id: str) -> None:
        with self._lock:
            self._remember_delivered(group_id, message_id)


def _build() -> _BoundedLink:
    return _BoundedLink("ak-1", "secret", connect=lambda: None)


def test_delivered_set_stays_bounded():
    link = _build()
    total = _DELIVERED_LIMIT * 3
    for i in range(total):
        link.remember_delivered("group-1", f"msg-{i}")

    assert len(link.delivered) == _DELIVERED_LIMIT
    assert ("group-1", "msg-0") not in link.delivered
    assert ("group-1", f"msg-{total - 1}") in link.delivered


def test_alias_map_stays_bounded():
    link = _build()
    total = _ALIAS_LIMIT * 3
    for i in range(total):
        link.remember_alias("group-1", f"poll-{i}", f"msg-{i}")

    assert len(link.aliases) == _ALIAS_LIMIT
    assert link.canonical_message_id("group-1", "poll-0") == "poll-0"
    assert link.canonical_message_id("group-1", f"poll-{total - 1}") == f"msg-{total - 1}"


def test_repeated_delivery_refreshes_position():
    link = _build()
    keep = ("group-1", "msg-keep")
    link.remember_delivered(*keep)
    for i in range(_DELIVERED_LIMIT - 1):
        link.remember_delivered("group-1", f"msg-{i}")

    link.remember_delivered(*keep)
    link.remember_delivered("group-1", "msg-extra")

    assert len(link.delivered) == _DELIVERED_LIMIT
    assert keep in link.delivered
    assert ("group-1", "msg-0") not in link.delivered
