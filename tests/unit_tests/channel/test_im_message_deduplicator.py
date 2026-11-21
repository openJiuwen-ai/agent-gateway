import pytest

from agent_gateway.channel_manager.im_platforms.message_dedup import MessageDeduplicator


def test_deduplicator_reports_first_seen_and_duplicate_ids() -> None:
    deduplicator = MessageDeduplicator()

    assert deduplicator.is_duplicate("message-1") is False
    assert deduplicator.is_duplicate("message-1") is True


def test_duplicate_lookup_does_not_refresh_fifo_position() -> None:
    deduplicator = MessageDeduplicator(capacity=2)

    assert deduplicator.is_duplicate("first") is False
    assert deduplicator.is_duplicate("second") is False
    assert deduplicator.is_duplicate("first") is True
    assert deduplicator.is_duplicate("third") is False

    assert deduplicator.is_duplicate("second") is True
    assert deduplicator.is_duplicate("first") is False


def test_deduplicator_rejects_non_positive_capacity() -> None:
    with pytest.raises(ValueError, match="capacity"):
        MessageDeduplicator(capacity=0)
