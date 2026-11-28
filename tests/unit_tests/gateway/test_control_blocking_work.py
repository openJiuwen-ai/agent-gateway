# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
"""Actual-work budgets and owned shutdown for Control's blocking calls."""

from __future__ import annotations

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
import threading
from typing import TYPE_CHECKING

import pytest

from agent_gateway.processes.control_service import ControlServiceBusy, ControlServiceError

if TYPE_CHECKING:
    from agent_gateway.auth.control_blocking_work import ControlBlockingWork


def make_work(limit: int = 4) -> ControlBlockingWork:
    from agent_gateway.auth.control_blocking_work import ControlBlockingWork

    return ControlBlockingWork(limit=limit)


async def wait_until(predicate) -> None:
    async with asyncio.timeout(2.0):
        while not predicate():
            await asyncio.sleep(0.001)


def blocked_call(started: threading.Event, release: threading.Event, value=None):
    started.set()
    if not release.wait(5.0):
        raise AssertionError('test blocking call was not released')
    return value


async def test_constructor_does_not_start_threads_and_run_accepts_positional_args() -> None:
    before = set(threading.enumerate())
    work = make_work()
    assert set(threading.enumerate()) == before
    assert work.pending_count == 0

    def add(left: int, right: int) -> tuple[int, int]:
        return threading.get_ident(), left + right

    try:
        thread_id, result = await work.run(add, 2, 3)
        assert result == 5
        assert thread_id != threading.get_ident()
        await wait_until(lambda: work.pending_count == 0)
    finally:
        await work.stop()


@pytest.mark.parametrize('limit', [0, -1, True, 1.5, None])
def test_limit_must_be_a_positive_integer(limit) -> None:
    with pytest.raises(ValueError, match='positive integer'):
        make_work(limit)


async def test_default_limit_is_four_and_submit_rejects_without_queueing() -> None:
    work = make_work()
    release = threading.Event()
    started = [threading.Event() for _ in range(4)]
    rejected_started = threading.Event()
    futures = []
    try:
        for index, signal in enumerate(started):
            future = work.submit(blocked_call, signal, release, index)
            assert isinstance(future, Future)
            futures.append(future)
        await wait_until(lambda: all(signal.is_set() for signal in started))
        assert work.pending_count == 4
        for _ in range(8):
            with pytest.raises(ControlServiceBusy) as caught:
                work.submit(blocked_call, rejected_started, release)
            assert str(caught.value) == str(ControlServiceBusy())
        assert work.pending_count == 4
        assert not rejected_started.is_set()
    finally:
        release.set()
        await work.stop()
    assert [future.result() for future in futures] == [0, 1, 2, 3]
    assert not rejected_started.is_set()


async def test_cancelled_run_keeps_slot_until_actual_work_finishes() -> None:
    work = make_work(limit=1)
    started = threading.Event()
    release = threading.Event()
    task = asyncio.create_task(work.run(blocked_call, started, release))
    try:
        await wait_until(started.is_set)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert work.pending_count == 1
        with pytest.raises(ControlServiceBusy):
            await work.run(lambda: 'must not run')
        release.set()
        await wait_until(lambda: work.pending_count == 0)
        assert await work.run(lambda: 'slot reused') == 'slot reused'
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await work.stop()


async def test_run_timeout_keeps_slot_and_does_not_starve_default_executor() -> None:
    work = make_work(limit=1)
    loop = asyncio.get_running_loop()
    loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    started = threading.Event()
    release = threading.Event()
    task = asyncio.create_task(work.run(blocked_call, started, release))
    try:
        await wait_until(started.is_set)
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(task, timeout=0.01)
        assert work.pending_count == 1
        assert await asyncio.wait_for(asyncio.to_thread(lambda: 'unrelated'), timeout=0.5) == 'unrelated'
        with pytest.raises(ControlServiceBusy):
            work.submit(lambda: 'must not queue')
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await work.stop()


async def test_worker_exception_propagates_and_releases_slot() -> None:
    work = make_work(limit=1)

    def fail() -> None:
        raise LookupError('test failure')

    try:
        with pytest.raises(LookupError, match='test failure'):
            await work.run(fail)
        await wait_until(lambda: work.pending_count == 0)
        assert await work.run(lambda: 7) == 7
    finally:
        await work.stop()


async def test_cancelled_run_consumes_late_exception_without_secret_logging(caplog) -> None:
    work = make_work(limit=1)
    loop = asyncio.get_running_loop()
    errors = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: errors.append(context))
    started = threading.Event()
    release = threading.Event()
    secret = 'test-secret-not-for-logs'

    def fail_later() -> None:
        blocked_call(started, release)
        raise RuntimeError(secret)

    task = asyncio.create_task(work.run(fail_later))
    try:
        await wait_until(started.is_set)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        await wait_until(lambda: work.pending_count == 0)
        await work.stop()
        await asyncio.sleep(0.01)
        assert errors == []
        assert secret not in caplog.text
        assert secret not in repr(work)
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await work.stop()
        loop.set_exception_handler(previous_handler)


async def test_stop_waits_for_actual_work_and_rejects_new_submissions() -> None:
    work = make_work(limit=1)
    started = threading.Event()
    release = threading.Event()
    future = work.submit(blocked_call, started, release, 'finished')
    stop_task = None
    try:
        await wait_until(started.is_set)
        stop_task = asyncio.create_task(work.stop(timeout=0.5))
        await asyncio.sleep(0)
        with pytest.raises(ControlServiceError) as caught:
            work.submit(lambda: 'must not start')
        assert type(caught.value) is ControlServiceError
        assert str(caught.value) == str(ControlServiceError())
        assert not stop_task.done()
        assert work.pending_count == 1
        release.set()
        await stop_task
        assert future.result() == 'finished'
        assert work.pending_count == 0
        await work.stop()
        with pytest.raises(ControlServiceError):
            await work.run(lambda: 'still closed')
    finally:
        release.set()
        if stop_task is not None:
            await asyncio.gather(stop_task, return_exceptions=True)
        await work.stop()


async def test_stop_timeout_is_bounded_keeps_residual_visible_and_can_be_repeated() -> None:
    work = make_work(limit=1)
    started = threading.Event()
    release = threading.Event()
    future = work.submit(blocked_call, started, release, 'late result')
    try:
        await wait_until(started.is_set)
        before = asyncio.get_running_loop().time()
        with pytest.raises(ControlServiceError) as caught:
            await work.stop(timeout=0.01)
        assert type(caught.value) is ControlServiceError
        assert str(caught.value) == str(ControlServiceError())
        assert asyncio.get_running_loop().time() - before < 0.5
        assert work.pending_count == 1
        assert not future.done()
        with pytest.raises(ControlServiceError):
            await work.stop(timeout=0.01)
        assert work.pending_count == 1
        with pytest.raises(ControlServiceError):
            work.submit(lambda: 'closed')
        release.set()
        await wait_until(lambda: work.pending_count == 0)
        await work.stop()
        assert future.result() == 'late result'
    finally:
        release.set()
        await work.stop()


async def test_repeated_stop_cancellation_does_not_interrupt_owned_cleanup() -> None:
    work = make_work(limit=1)
    started = threading.Event()
    release = threading.Event()
    future = work.submit(blocked_call, started, release)
    stop_task = None
    try:
        await wait_until(started.is_set)
        stop_task = asyncio.create_task(work.stop(timeout=0.5))
        await asyncio.sleep(0)
        stop_task.cancel()
        await asyncio.sleep(0)
        stop_task.cancel()
        await asyncio.sleep(0)
        assert not stop_task.done()
        assert work.pending_count == 1
        with pytest.raises(ControlServiceError):
            work.submit(lambda: 'closed')
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await stop_task
        assert future.done()
        assert work.pending_count == 0
        await work.stop()
    finally:
        release.set()
        if stop_task is not None:
            await asyncio.gather(stop_task, return_exceptions=True)
        await work.stop()


async def test_cancelled_stop_is_still_bounded_when_work_remains() -> None:
    work = make_work(limit=1)
    started = threading.Event()
    release = threading.Event()
    work.submit(blocked_call, started, release)
    stop_task = None
    try:
        await wait_until(started.is_set)
        stop_task = asyncio.create_task(work.stop(timeout=0.02))
        await asyncio.sleep(0)
        stop_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(stop_task, timeout=0.5)
        assert work.pending_count == 1
    finally:
        release.set()
        if stop_task is not None:
            await asyncio.gather(stop_task, return_exceptions=True)
        await work.stop()


async def test_stop_without_work_is_idempotent_and_accepts_zero_timeout() -> None:
    work = make_work()
    await work.stop(timeout=0)
    await work.stop(timeout=0)
    assert work.pending_count == 0
    with pytest.raises(ControlServiceError):
        work.submit(lambda: 'closed')


@pytest.mark.parametrize('timeout', [-1, True, float('nan'), float('inf'), '1'])
async def test_stop_rejects_non_finite_or_negative_timeout(timeout) -> None:
    work = make_work()
    try:
        with pytest.raises(ValueError, match='non-negative finite'):
            await work.stop(timeout=timeout)
        assert await work.run(lambda: 'still open') == 'still open'
    finally:
        await work.stop()


def test_submit_and_run_reject_a_different_event_loop() -> None:
    work = make_work(limit=1)

    async def first_loop() -> None:
        assert await work.run(lambda: 1) == 1
        await wait_until(lambda: work.pending_count == 0)

    async def second_loop() -> None:
        with pytest.raises(ControlServiceError):
            work.submit(lambda: 2)
        with pytest.raises(ControlServiceError):
            await work.run(lambda: 3)

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(first_loop())
        asyncio.run(second_loop())
    finally:
        loop.run_until_complete(work.stop())
        loop.close()


def test_submit_requires_running_event_loop_without_starting_work() -> None:
    work = make_work()
    with pytest.raises(ControlServiceError):
        work.submit(lambda: 'not submitted')
    assert work.pending_count == 0
    asyncio.run(work.stop())


def test_completion_after_event_loop_closes_releases_pending_without_logs(caplog) -> None:
    work = make_work(limit=1)
    started = threading.Event()
    release = threading.Event()
    loop = asyncio.new_event_loop()
    errors = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    def fail_later() -> None:
        blocked_call(started, release)
        raise RuntimeError('closed-loop-test-secret')

    async def submit_and_stop() -> Future:
        future = work.submit(fail_later)
        await wait_until(started.is_set)
        with pytest.raises(ControlServiceError):
            await work.stop(timeout=0.01)
        assert work.pending_count == 1
        return future

    try:
        future = loop.run_until_complete(submit_and_stop())
    finally:
        loop.close()
        release.set()
    with pytest.raises(RuntimeError, match='closed-loop-test-secret'):
        future.result(timeout=2.0)
    # A worker's done callback can still be running just after result() wakes.
    deadline = threading.Event()
    for _ in range(2000):
        if work.pending_count == 0:
            break
        deadline.wait(0.001)
    assert work.pending_count == 0
    assert errors == []
    assert 'closed-loop-test-secret' not in caplog.text


async def test_concurrent_stop_callers_share_cleanup_without_cross_cancellation() -> None:
    work = make_work(limit=1)
    started = threading.Event()
    release = threading.Event()
    future = work.submit(blocked_call, started, release)
    stops = []
    try:
        await wait_until(started.is_set)
        stops = [asyncio.create_task(work.stop(timeout=0.5)) for _ in range(2)]
        await asyncio.sleep(0)
        stops[0].cancel()
        await asyncio.sleep(0)
        assert not stops[0].done()
        assert not stops[1].done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await stops[0]
        await stops[1]
        assert future.done()
        assert work.pending_count == 0
    finally:
        release.set()
        await asyncio.gather(*stops, return_exceptions=True)
        await work.stop()


async def test_submit_failure_returns_fixed_error_without_leaking_a_slot(monkeypatch, caplog) -> None:
    work = make_work(limit=1)
    secret = 'submit-failure-test-secret'

    def fail_submit(fn, *args):
        raise RuntimeError(secret)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(work._executor, 'submit', fail_submit)
            with pytest.raises(ControlServiceError) as caught:
                work.submit(lambda: secret)
            assert type(caught.value) is ControlServiceError
            assert str(caught.value) == str(ControlServiceError())
            assert caught.value.__suppress_context__
            assert work.pending_count == 0
        assert await work.run(lambda: 'slot restored') == 'slot restored'
        assert secret not in caplog.text
        assert secret not in repr(work)
    finally:
        await work.stop()
