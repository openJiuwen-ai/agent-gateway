"""Small LRU-bounded mapping for process-lifetime caches.

A few gateway caches key on session ids that keep coming for as long as the
process runs (one entry per conversation ever seen).  Plain dicts there grow
without limit; this mapping keeps the most recently used ``capacity`` entries
and drops the oldest one on insert.
"""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterator, MutableMapping
from typing import Generic, TypeVar

K = TypeVar("K")
V = TypeVar("V")


class BoundedLruCache(MutableMapping[K, V], Generic[K, V]):
    """A dict-like LRU cache with a hard entry-count limit.

    ``capacity`` must be positive. Entries are ordered by last access: ``get``,
    ``__getitem__`` and ``__setitem__`` mark an entry as most recently used,
    and inserting past ``capacity`` evicts the least recently used entry.
    """

    def __init__(self, capacity: int) -> None:
        if capacity < 1:
            raise ValueError("capacity must be greater than zero")
        self._capacity = capacity
        self._data: OrderedDict[K, V] = OrderedDict()

    @property
    def capacity(self) -> int:
        return self._capacity

    def __getitem__(self, key: K) -> V:
        value = self._data[key]
        self._data.move_to_end(key)
        return value

    def __setitem__(self, key: K, value: V) -> None:
        self._data[key] = value
        self._data.move_to_end(key)
        while len(self._data) > self._capacity:
            self._data.popitem(last=False)

    def __delitem__(self, key: K) -> None:
        del self._data[key]

    def __iter__(self) -> Iterator[K]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __contains__(self, key: object) -> bool:
        return key in self._data

    def get(self, key: K, default: V | None = None) -> V | None:
        try:
            return self[key]
        except KeyError:
            return default

    def clear(self) -> None:
        self._data.clear()

    def peek(self, key: K, default: V | None = None) -> V | None:
        """Read without changing recency (used by diagnostics/tests)."""
        return self._data.get(key, default)
