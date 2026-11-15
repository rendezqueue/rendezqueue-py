"""Explicit, single-owner continuation at a completed command's exit barrier."""

import base64
import binascii
from dataclasses import dataclass


def encode_id(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def decode_id(value: str) -> bytes:
    """Read the canonical unpadded base64url IDs printed by a command."""
    try:
        raw = value.encode("ascii")
        decoded = base64.b64decode(
            raw + b"=" * (-len(raw) % 4), altchars=b"-_", validate=True
        )
    except (UnicodeError, ValueError, binascii.Error) as error:
        raise ValueError("expected an unpadded base64url continuation ID") from error
    if not decoded or encode_id(decoded) != value:
        raise ValueError("expected a nonempty canonical base64url continuation ID")
    return decoded


@dataclass(frozen=True)
class Continuation:
    """Use once, with the same endpoint/room, never concurrently or after timeout.

    Only RendezqueueClient.finish() certifies that old payloads can be discarded.
    These two IDs are not a checkpoint for an interrupted command.
    """

    self_xid: bytes
    peer_xid: bytes

    def __post_init__(self) -> None:
        if (
            not isinstance(self.self_xid, bytes)
            or not isinstance(self.peer_xid, bytes)
            or not self.self_xid
            or not self.peer_xid
            or self.self_xid == self.peer_xid
        ):
            raise ValueError("continuation requires distinct nonempty byte XIDs")

    def arguments(self) -> str:
        return f"--self {encode_id(self.self_xid)} --peer {encode_id(self.peer_xid)}"
