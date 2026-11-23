from __future__ import annotations

from types import SimpleNamespace

import pytest

from agent_gateway.common import retry


def test_jittered_delay_uses_uniform_bounds(monkeypatch) -> None:
    calls: list[tuple[float, float]] = []

    def fake_uniform(low: float, high: float) -> float:
        calls.append((low, high))
        return 1.5

    monkeypatch.setattr(retry.random, "uniform", fake_uniform)
    assert retry.jittered_delay(2.0) == 3.0
    assert calls == [(retry.JITTER_LOW, retry.JITTER_HIGH)]


def test_jittered_delay_never_negative(monkeypatch) -> None:
    monkeypatch.setattr(retry.random, "uniform", lambda low, high: 0.5)
    assert retry.jittered_delay(-1.0) == 0.0


@pytest.mark.asyncio
async def test_jittered_sleep_awaits_jittered_delay(monkeypatch) -> None:
    delays: list[float] = []

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(retry, "jittered_delay", lambda base: 0.25)
    monkeypatch.setattr(retry, "asyncio", SimpleNamespace(sleep=fake_sleep))

    await retry.jittered_sleep(1.0)

    assert delays == [0.25]
