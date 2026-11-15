from concurrent.futures import ThreadPoolExecutor
import threading
from unittest.mock import patch

from rendezqueue.plyswap import Message, PlySwapResponse
from rendezqueue.plyswap_cache import PlySwapCache


class ObservedLock:
    """Expose the second entrant without changing the mutex behavior."""

    def __init__(self, second_waiting):
        self.lock = threading.Lock()
        self.attempts = 0
        self.second_waiting = second_waiting

    def __enter__(self):
        self.attempts += 1
        if self.attempts == 2:
            self.second_waiting.set()
        self.lock.acquire()

    def __exit__(self, *args):
        self.lock.release()


def test_queued_request_cannot_sample_time_before_acquiring_cache_lock():
    first_clock = threading.Event()
    resume_first = threading.Event()
    second_waiting = threading.Event()
    times = []
    now = 0

    def clock():
        times.append(now)
        if len(times) == 1:
            first_clock.set()
            assert resume_first.wait(5)
        return now

    cache = PlySwapCache(ttl=20, clock=clock)
    full = Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"frozen",))
    with (
        patch.object(cache, "_lock", ObservedLock(second_waiting)),
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        first = pool.submit(cache.serve, b"room", full)
        try:
            assert first_clock.wait(5)
            queued = pool.submit(cache.serve, b"room", Message(b"a", next_xid=b"a1"))
            queued.add_done_callback(lambda _: second_waiting.set())
            assert second_waiting.wait(5)
            assert times == [0], "queued request sampled a stale time before locking"
            assert not queued.done()
        finally:
            resume_first.set()
        assert first.result(timeout=5) == PlySwapResponse(b"room", b"a")
        assert queued.result(timeout=5) == PlySwapResponse(b"room", b"a")
    assert times == [0, 0]
    # Later, expiry and a reference miss occur in the same locked transition.
    now = 20
    assert cache.serve(b"room", Message(b"a", next_xid=b"a1")) is None
    assert cache.room_count == 0


def test_sweep_removes_expired_room_before_waiting_request_recreates_it():
    cache = PlySwapCache(ttl=20, clock=lambda: 0)
    cache.serve(b"room", Message(b"old"))
    sweep_paused = threading.Event()
    resume_sweep = threading.Event()
    request_waiting = threading.Event()

    def clock():
        if not sweep_paused.is_set():
            sweep_paused.set()
            assert resume_sweep.wait(5)
        return 20

    with (
        patch.object(cache, "_clock", clock),
        patch.object(cache, "_lock", ObservedLock(request_waiting)),
        ThreadPoolExecutor(max_workers=2) as pool,
    ):
        cleanup = pool.submit(cache.sweep)
        try:
            # Pause the sweep inside its mutex, before expiring the old room.
            assert sweep_paused.wait(5)
            creation = pool.submit(cache.serve, b"room", Message(b"new"))
            creation.add_done_callback(lambda _: request_waiting.set())
            assert request_waiting.wait(5)
            assert not creation.done(), "request bypassed the sweep's cache lock"
            assert list(cache._rooms[b"room"].core.rows) == [b"old"]
        finally:
            resume_sweep.set()
        assert cleanup.result(timeout=5) is None
        assert creation.result(timeout=5) == PlySwapResponse(b"room", b"new")
    assert cache.room_count == 1
    assert list(cache._rooms[b"room"].core.rows) == [b"new"]
    assert cache._rooms[b"room"].refreshed == {b"new": 20}
