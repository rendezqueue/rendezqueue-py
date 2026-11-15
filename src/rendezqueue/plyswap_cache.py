"""Volatile per-ID inactivity policy; not part of the clock-free protocol."""

import math
import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

from rendezqueue.plyswap import Message, PlySwap, PlySwapResponse

DEFAULT_TTL = 20.0


@dataclass
class _Room:
    core: PlySwap = field(default_factory=PlySwap)
    refreshed: Dict[bytes, float] = field(default_factory=dict)

    def expire(self, now: float, ttl: float) -> None:
        for self_xid, refreshed in list(self.refreshed.items()):
            if now - refreshed >= ttl:
                del self.refreshed[self_xid]
                del self.core.rows[self_xid]


class PlySwapCache:
    """Room-scoped service returning PlySwapResponse, including its room.

    One mutex covers clock sampling, expiry, serve and own-ID refresh.

    Admission, JSON work and socket I/O must happen outside this cache. Sweep is
    a full scan; rooms do not execute concurrently or have bounded sweep budgets.
    """

    def __init__(
        self, ttl: float = DEFAULT_TTL, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if not math.isfinite(ttl) or ttl <= 0:
            raise ValueError("PlySwap TTL must be finite and positive")
        self.ttl = ttl
        self._clock = clock
        self._lock = threading.Lock()
        self._rooms: Dict[bytes, _Room] = {}

    @property
    def room_count(self) -> int:
        with self._lock:
            return len(self._rooms)

    def serve(self, room: bytes, request: Message) -> Optional[PlySwapResponse]:
        with self._lock:
            now = self._clock()
            state = self._rooms.get(room)
            if state is None:
                state = _Room()
            state.expire(now, self.ttl)
            result = state.core.serve(request)
            if result is not None:
                state.refreshed[request.self_xid] = now
            if state.core.rows:
                self._rooms[room] = state
            else:
                self._rooms.pop(room, None)
            return (
                PlySwapResponse.from_message(room, result)
                if result is not None
                else None
            )

    def sweep(self) -> None:
        with self._lock:
            now = self._clock()
            for room, state in list(self._rooms.items()):
                state.expire(now, self.ttl)
                if not state.core.rows:
                    del self._rooms[room]
