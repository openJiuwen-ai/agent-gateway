"""Bounded FIFO deduplication for inbound IM message identifiers."""

from collections import OrderedDict


class MessageDeduplicator:
    """Track recently seen message IDs while keeping memory bounded."""

    def __init__(self, capacity: int = 1000) -> None:
        if capacity < 1:
            raise ValueError("capacity must be greater than zero")
        self._capacity = capacity
        self._seen: OrderedDict[str, None] = OrderedDict()

    def is_duplicate(self, message_id: str) -> bool:
        """Return whether an ID has been seen, recording new IDs in FIFO order."""
        if message_id in self._seen:
            return True

        self._seen[message_id] = None
        while len(self._seen) > self._capacity:
            self._seen.popitem(last=False)
        return False
