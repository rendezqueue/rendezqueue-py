"""Transport-free retained PlySwap client with immutable retry snapshots."""

from dataclasses import dataclass, replace
from typing import List, Literal, Optional, Tuple

from rendezqueue.plyswap import Message

Mode = Literal["current", "previous"]
MAX_COUNTER = (1 << 64) - 1


@dataclass(frozen=True)
class Packet:
    attempt: int
    request: Message


@dataclass(frozen=True)
class Completion:
    """One local generation completion, not proof of remote application receipt."""

    values: Tuple[bytes, ...]
    generation: int


class ClientState:
    def __init__(
        self,
        namespace: bytes,
        *,
        continuable: bool = False,
        self_xid: bytes = b"",
        peer_xid: bytes = b"",
    ) -> None:
        if not isinstance(self_xid, bytes) or not isinstance(peer_xid, bytes):
            raise ValueError("continuation XIDs must be bytes")
        if bool(self_xid) != bool(peer_xid):
            raise ValueError("continuation requires both self_xid and peer_xid")
        if self_xid and (not continuable or self_xid == peer_xid):
            raise ValueError("distinct continuation IDs require continuable=True")
        self.namespace = bytes(namespace)
        self.initial_self_xid = self_xid
        self.continuable = continuable
        self.peer_confirmed = bool(peer_xid)
        self.current_advertised = False
        self.generation = 0
        self.next_attempt = 0
        self.peer_xid = peer_xid
        self.batch: Optional[Tuple[bytes, ...]] = None
        self.queued: List[bytes] = []
        self.previous: Optional[Message] = None
        self.active: Optional[Packet] = None
        self.current_cached = False
        self.previous_cached = False

    def _id(self, generation: int) -> bytes:
        if not 0 <= generation <= MAX_COUNTER:
            raise OverflowError("PlySwap generation exhausted")
        if generation == 0 and bool(self.initial_self_xid):
            return self.initial_self_xid
        return self.namespace + generation.to_bytes(8, "big")

    @property
    def self_xid(self) -> bytes:
        return self._id(self.generation)

    def can_handoff(self) -> bool:
        """The peer consumed our previous batch; current has no frozen input."""
        return (
            self.continuable
            and bool(self.peer_xid)
            and self.peer_confirmed
            and self.batch is None
            and not self.queued
            and self.active is None
        )

    def stored(self) -> Tuple[bytes, ...]:
        return (
            (self.previous.values if self.previous is not None else ())
            + (self.batch or ())
            + tuple(self.queued)
        )

    def enqueue(self, value: bytes) -> None:
        self.queued.append(bytes(value))

    def timeout(self, packet: Optional[Packet] = None) -> None:
        if packet is None or self.active == packet:
            self.active = None

    def _current(self) -> Message:
        values = (self.batch or ()) if bool(self.peer_xid) else ()
        return Message(
            self.self_xid,
            peer_xid=self.peer_xid,
            next_xid=self._id(self.generation + 1) if values else b"",
            values=values,
        )

    def _slot(self, request: Message) -> Optional[Mode]:
        if self.previous == request:
            return "previous"
        if bool(self.peer_xid) and request == self._current():
            return "current"
        return None

    def _hint(self, slot: Mode) -> bool:
        return self.current_cached if slot == "current" else self.previous_cached

    def _set_hint(self, slot: Mode, value: bool) -> None:
        if slot == "current":
            self.current_cached = value
        else:
            self.previous_cached = value

    def send(self, mode: Mode) -> Optional[Packet]:
        if mode not in ("current", "previous"):
            raise ValueError("invalid PlySwap attempt mode")
        if self.active is not None or (mode == "previous" and self.previous is None):
            return None
        if self.next_attempt >= MAX_COUNTER:
            raise OverflowError("PlySwap attempt counter exhausted")
        if mode == "current":
            self._id(self.generation + 1)  # Check before freezing any queued input.
            if (
                bool(self.peer_xid)
                and self.batch is None
                and self.queued
                and (not self.continuable or self.peer_confirmed)
            ):
                self.batch = tuple(self.queued)
                self.queued.clear()
                self.current_cached = False
            # Before binding a successor, observe the exact expected peer row.
            # Binding first would let OPEN echo our own cached peer binding, which is
            # not evidence that the peer has consumed our previous snapshot.
            request = (
                Message(self.self_xid)
                if self.continuable and bool(self.peer_xid) and not self.peer_confirmed
                else self._current()
            )
        else:
            assert self.previous is not None
            request = self.previous
        slot = self._slot(request)
        if (
            slot is not None
            and self._hint(slot)
            and request.values
            and bool(request.peer_xid)
        ):
            request = replace(request, peer_xid=b"", values=())
        packet = Packet(self.next_attempt, request)
        self.next_attempt += 1
        self.active = packet
        return packet

    def _logical(self, request: Message) -> Optional[Message]:
        if request.is_reference():

            def same(snapshot: Optional[Message]) -> bool:
                return (
                    snapshot is not None
                    and snapshot.self_xid == request.self_xid
                    and snapshot.next_xid == request.next_xid
                    and bool(snapshot.peer_xid)
                    and bool(snapshot.values)
                )

            if same(self.previous):
                return self.previous
            current = self._current()
            return current if same(current) else None
        if not request.peer_xid:
            return request if not request.values and not request.next_xid else None
        return request if not request.values or bool(request.next_xid) else None

    def accept(self, packet: Packet, reply: Optional[Message]) -> Optional[Completion]:
        """None reply is exact protocol rejection, never a transport failure.

        Old/duplicate packets cannot clear a newer ACTIVE. Accepted completions
        are returned as events, not accumulated in an unbounded output history.
        """
        if self.active != packet:
            return None
        self.active = None
        request = self._logical(packet.request)
        if request is None:
            return None
        if reply is not None and (
            (bool(reply.next_xid) and bool(reply.peer_xid))
            or (not reply.next_xid and reply.values)
        ):
            return None
        if (
            reply is not None
            and reply.self_xid == self.self_xid
            and request.self_xid == self.self_xid
        ):
            self.current_advertised = True
        if not request.peer_xid:
            if (
                self.continuable
                and bool(self.peer_xid)
                and request == Message(self.self_xid)
                and reply == Message(self.self_xid, peer_xid=self.peer_xid)
            ):
                self.peer_confirmed = True
            elif (
                reply is not None
                and bool(reply.peer_xid)
                and not reply.next_xid
                and self.previous is None
                and not self.peer_xid
                and self.generation == 0
                and request == self._current()
                and reply.self_xid == request.self_xid
                and reply.peer_xid != request.self_xid
            ):
                self.peer_xid = reply.peer_xid
                self.peer_confirmed = True
            return None
        slot = self._slot(request)
        if slot is None:
            return None
        if reply is None:
            if packet.request.is_reference():
                self._set_hint(slot, False)
            return None
        if reply.self_xid != request.self_xid:
            return None
        if packet.request.values:
            self._set_hint(slot, True)
        if slot == "current" and bool(reply.next_xid):
            generation = self.generation + 1
            own_next = self._id(generation)
            if reply.next_xid == own_next:
                return None
            self.generation = generation
            self.peer_xid = reply.next_xid
            self.batch = None
            self.previous = replace(request, next_xid=own_next)
            self.previous_cached = self.current_cached
            self.current_cached = False
            self.peer_confirmed = False
            self.current_advertised = False
            return Completion(reply.values, generation)
        return None
