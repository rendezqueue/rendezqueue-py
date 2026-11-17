"""Main threaded Rendezqueue API, backed by retained PlySwap state."""

from dataclasses import dataclass
import logging
import math
import secrets
import threading
import time
from typing import Any, Callable, List, Optional

from rendezqueue.continuation import Continuation
from rendezqueue.plyswap_client import ClientState, Mode, Packet
from rendezqueue.plyswap_transport import HttpTransport

logger = logging.getLogger(__name__)


@dataclass(eq=False)
class _PollRun:
    next_poll_time: float
    next_retry_time: float = 0.0


class RendezqueueClient:
    """PlySwap client with the existing start/send/stop/callback interface.

    Endpoint and room are fixed for this retained client's lifetime. stop() stops
    future admission, not owned I/O/callbacks; start() resumes the same state.
    on_complete means local generation completion, not remote application ACK.
    Empty constructor XIDs start fresh; resuming requires a nonempty pair.
    poll_interval_ms paces waiting for peer activity and error retries.
    Locally ready publications and protocol follow-ups run without that delay.
    """

    def __init__(
        self,
        url: str,
        room: str,
        hue: str,
        on_data: Optional[Callable[[List[bytes]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        poll_interval_ms: int = 2000,
        on_complete: Optional[Callable[[], None]] = None,
        *,
        continuable: bool = False,
        self_xid: bytes = b"",
        peer_xid: bytes = b"",
    ) -> None:
        self._url = url
        self._room = room
        self._transport = HttpTransport(url, room.encode("utf-8"))
        self.hue = hue
        self.on_data = on_data
        self.on_error = on_error or (
            lambda error: logger.error("Rendezqueue error: %s", error)
        )
        self.on_complete = on_complete
        self.poll_interval_ms = poll_interval_ms
        # A fresh namespace per object; restarting a stopped object retains it.
        namespace = hue.encode("utf-8") + b"-" + secrets.token_bytes(16)
        self._core = ClientState(
            namespace, continuable=continuable, self_xid=self_xid, peer_xid=peer_xid
        )
        self._finishing = False
        self._failure: Optional[Exception] = None
        self.lock = threading.Lock()
        self._condition = threading.Condition(self.lock)
        self._run: Optional[_PollRun] = None
        self._previous_next = False
        self._ready_mode: Optional[Mode] = None
        self.is_polling = False
        self._poll_owner: Optional[threading.Thread] = None
        self.is_stopped = True
        self.poll_thread: Optional[threading.Thread] = None

    @property
    def url(self) -> str:
        return self._url

    @property
    def room(self) -> str:
        return self._room

    @property
    def poll_interval_ms(self) -> int:
        return self._poll_interval_ms

    @poll_interval_ms.setter
    def poll_interval_ms(self, value: int) -> None:
        if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
            raise ValueError("poll_interval_ms must be finite and positive")
        self._poll_interval_ms = value

    @property
    def outgoing_queue(self) -> List[bytes]:
        """Read-only snapshot of not-yet-locally-completed input."""
        with self.lock:
            return list(self._core.batch or ()) + list(self._core.queued)

    @property
    def current_advertised(self) -> bool:
        """A successful RPC advertised the current successor, not an app ACK."""
        with self.lock:
            return self._core.current_advertised

    @property
    def failure(self) -> Optional[Exception]:
        with self.lock:
            return self._failure

    def finish(self, timeout: float = 30.0) -> Continuation:
        """Freeze new input, certify the peer's progress, and stop atomically.

        Unlike stop(), success waits for owned I/O and callbacks to finish.
        Only a successful return permits dropping this object's retry snapshots.
        Timeout/failure produces no continuation; the old cursor is not reusable.
        """
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("finish timeout must be finite and positive")
        deadline = time.monotonic() + timeout
        with self._condition:
            # start() can replace poll_thread while an old run still owns a
            # callback. Track admission ownership, not the scheduler pointer.
            if threading.current_thread() is self._poll_owner:
                raise RuntimeError("finish cannot run inside a polling callback")
            if not self._core.continuable:
                raise RuntimeError("finish requires continuable=True")
            if self._finishing:
                raise RuntimeError("finish already requested")
            self._finishing = True
            try:
                while True:
                    if self._failure is not None:
                        raise self._failure
                    if not self.is_polling and self._core.can_handoff():
                        assert bool(self._core.peer_xid)
                        return Continuation(self._core.self_xid, self._core.peer_xid)
                    if self.is_stopped and not self.is_polling:
                        raise RuntimeError("client stopped before a safe handoff")
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise TimeoutError(
                            "handoff incomplete; no continuation available"
                        )
                    self._condition.wait(remaining)
            finally:
                self.is_stopped = True
                self._run = None
                self._condition.notify_all()

    def start(self) -> None:
        with self._condition:
            if self._finishing or self._failure is not None:
                raise RuntimeError("client is closed for continuation")
            if not self.is_stopped:
                return
            run = _PollRun(time.monotonic())
            self.is_stopped = False
            self._run = run
            thread = threading.Thread(target=self._poll_loop, args=(run,), daemon=True)
            self.poll_thread = thread
            try:
                thread.start()
            except Exception:
                self.is_stopped = True
                self._run = None
                raise

    def stop(self) -> None:
        with self._condition:
            self.is_stopped = True
            self._run = None
            self._condition.notify_all()

    def send(self, value: Any) -> None:
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif not isinstance(value, bytes):
            value = memoryview(value).tobytes()
        with self._condition:
            if self._finishing or self._failure is not None:
                raise RuntimeError("client is closed for continuation")
            self._core.enqueue(value)
            run = self._run
            if run is not None and not self.is_polling and self._input_ready():
                run.next_poll_time = min(
                    run.next_poll_time, max(time.monotonic(), run.next_retry_time)
                )
                self._condition.notify_all()

    def _input_ready(self) -> bool:
        return (
            bool(self._core.peer_xid)
            and self._core.batch is None
            and bool(self._core.queued)
        )

    def _poll_loop(self, owner: _PollRun) -> None:
        while True:
            with self._condition:
                while True:
                    if self._run is not owner:
                        return
                    if self.is_polling:
                        self._condition.wait()
                        continue
                    delay = owner.next_poll_time - time.monotonic()
                    if delay <= 0:
                        break
                    self._condition.wait(delay)
            self._poll(owner)

    def _report_error(self, error: Exception) -> None:
        try:
            self.on_error(error)
        except Exception:
            logger.exception("Rendezqueue error callback failed")

    def _poll(self, owner: Optional[_PollRun] = None) -> None:
        with self.lock:
            if (
                self.is_polling
                or (self._finishing and self.is_stopped)
                or (owner is not None and self._run is not owner)
            ):
                return
            self.is_polling = True
            self._poll_owner = threading.current_thread()
        packet: Optional[Packet] = None
        successful = False
        try:
            with self.lock:
                generation = self._core.generation
                peer = self._core.peer_xid
                confirmed = self._core.peer_confirmed
                previous_advertised = self._core.previous_advertised
                mode: Mode = self._ready_mode or (
                    "previous"
                    if (
                        not self._input_ready()
                        and self._previous_next
                        and self._core.previous is not None
                        and not (self._core.continuable and confirmed)
                    )
                    else "current"
                )
                self._ready_mode = None
                self._previous_next = mode == "current"
                packet = self._core.send(mode)
            if packet is None:
                return
            reply = self._transport.exchange(packet.request)
            with self.lock:
                if (
                    reply is None
                    and self._core.continuable
                    and not packet.request.is_reference()
                ):
                    self._failure = RuntimeError(
                        "FULL request rejected; continuation may already be used. "
                        "No safe continuation is available."
                    )
                    self.is_stopped = True
                    self._run = None
                    raise self._failure
                completion = self._core.accept(packet, reply)
                successful = True
                if self._core.generation != generation:
                    self._ready_mode = (
                        "current" if self._core.previous_advertised else "previous"
                    )
                elif (not peer and self._core.peer_xid) or (
                    not confirmed and self._core.peer_confirmed
                ):
                    self._ready_mode = "current"
                elif not previous_advertised and self._core.previous_advertised:
                    self._ready_mode = "current"
                elif reply is None and packet.request.is_reference():
                    self._ready_mode = mode
                on_data, on_complete = self.on_data, self.on_complete
            if completion is not None:
                # State commits first; callbacks run outside the state mutex,
                # but inside the admission guard, including reentrant stop/start.
                if on_data is not None:
                    try:
                        on_data(list(completion.values))
                    except Exception as error:
                        successful = False
                        self._report_error(error)
                if on_complete is not None:
                    try:
                        on_complete()
                    except Exception as error:
                        successful = False
                        self._report_error(error)
        except Exception as error:
            successful = False
            with self.lock:
                if packet is not None:
                    self._core.timeout(packet)
            self._report_error(error)
        finally:
            with self._condition:
                self.is_polling = False
                self._poll_owner = None
                if self._run is not None:
                    run = self._run
                    now = time.monotonic()
                    if not successful or run is not owner:
                        run.next_retry_time = now + self.poll_interval_ms / 1000.0
                        run.next_poll_time = run.next_retry_time
                    else:
                        run.next_retry_time = 0.0
                        ready = self._ready_mode is not None or self._input_ready()
                        run.next_poll_time = (
                            now
                            if ready
                            and not (self._finishing and self._core.can_handoff())
                            else now + self.poll_interval_ms / 1000.0
                        )
                self._condition.notify_all()
