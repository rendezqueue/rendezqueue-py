"""Clock-free PlySwap core, separate from legacy TrySwap and HTTP encoding."""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple


def _validate_xids(self_xid: bytes, peer_xid: bytes, next_xid: bytes) -> None:
    if not isinstance(self_xid, bytes) or not self_xid:
        raise ValueError("self_xid must be nonempty bytes")
    if not isinstance(peer_xid, bytes) or not isinstance(next_xid, bytes):
        raise ValueError("peer_xid and next_xid must be bytes; empty means absent")


@dataclass(frozen=True)
class Message:
    self_xid: bytes
    peer_xid: bytes = b""
    next_xid: bytes = b""
    values: Tuple[bytes, ...] = ()

    def __post_init__(self) -> None:
        _validate_xids(self.self_xid, self.peer_xid, self.next_xid)

    def is_reference(self) -> bool:
        return not self.peer_xid and not self.values and bool(self.next_xid)


@dataclass(frozen=True)
class PlySwapResponse:
    """Successful room-scoped service result, independent of JSON encoding."""

    room: bytes
    self_xid: bytes
    peer_xid: bytes = b""
    next_xid: bytes = b""
    values: Tuple[bytes, ...] = ()

    def __post_init__(self) -> None:
        _validate_xids(self.self_xid, self.peer_xid, self.next_xid)

    @classmethod
    def from_message(cls, room: bytes, message: Message) -> "PlySwapResponse":
        return cls(
            room, message.self_xid, message.peer_xid, message.next_xid, message.values
        )

    @property
    def message(self) -> Message:
        return Message(self.self_xid, self.peer_xid, self.next_xid, self.values)


@dataclass(frozen=True)
class Record:
    self_xid: bytes
    peer_xid: bytes = b""
    next_xid: bytes = b""
    values: Tuple[bytes, ...] = ()

    def __post_init__(self) -> None:
        _validate_xids(self.self_xid, self.peer_xid, self.next_xid)


class PlySwap:
    """Single-room core. The room-scoped service boundary is PlySwapCache.

    Newest-first discovery. None is rejection, never a pending reply.
    """

    def __init__(self) -> None:
        # Dict insertion order is oldest-first; only put moves a row.
        self.rows: Dict[bytes, Record] = {}

    def _put(self, row: Record) -> None:
        self.rows.pop(row.self_xid, None)
        self.rows[row.self_xid] = row

    def _resolve(self, wire: Message) -> Optional[Message]:
        if wire.is_reference():
            row = self.rows.get(wire.self_xid)
            if (
                row is None
                or not row.peer_xid
                or not row.values
                or row.next_xid != wire.next_xid
            ):
                return None
            return Message(wire.self_xid, row.peer_xid, wire.next_xid, row.values)
        if not wire.peer_xid:
            return wire if not wire.values and not wire.next_xid else None
        return wire if not wire.values or bool(wire.next_xid) else None

    def serve(self, wire: Message) -> Optional[Message]:
        request = self._resolve(wire)
        if request is None:
            return None
        self_xid, peer_xid = request.self_xid, request.peer_xid
        if not peer_xid:
            if self_xid not in self.rows:
                self._put(Record(self_xid))
            bound = self.rows[self_xid].peer_xid
            if not bound:
                bound = next(
                    (
                        row.self_xid
                        for row in reversed(self.rows.values())
                        if row.self_xid != self_xid
                        and (not row.peer_xid or row.peer_xid == self_xid)
                    ),
                    b"",
                )
            return Message(self_xid, peer_xid=bound)

        if self_xid == peer_xid or request.next_xid in (self_xid, peer_xid):
            return None
        old = self.rows.get(self_xid)
        if old is not None and (
            (bool(old.peer_xid) and old.peer_xid != peer_xid)
            or ((old.values or bool(old.next_xid)) and old.values != request.values)
            or (
                bool(old.next_xid)
                and bool(request.next_xid)
                and old.next_xid != request.next_xid
            )
        ):
            return None
        next_xid = (
            old.next_xid if old is not None and bool(old.next_xid) else request.next_xid
        )
        self._put(Record(self_xid, peer_xid, next_xid, request.values))
        other = self.rows.get(peer_xid)
        if other is not None and other.peer_xid == self_xid and bool(other.next_xid):
            return Message(self_xid, next_xid=other.next_xid, values=other.values)
        return Message(self_xid)
