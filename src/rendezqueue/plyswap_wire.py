"""Strict PlySwap JSON: room, self, peer, next, values, and optional b64.

Python self_xid, peer_xid, and next_xid are self, peer, and next on the wire.
No old model, SDK, or wire aliases are accepted.
Reply self echoes the requester ID; reply next is the peer's successor.
"""

import base64
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional
from urllib.parse import unquote_plus

from rendezqueue.plyswap import Message, PlySwapResponse

ROUTE = "/"
REQUEST_LIMIT = 1024 * 1024
# Own room + self ID share one admitted request budget; peer fields share another.
# Raw JSON escaping can expand each decoded byte sixfold.
RESPONSE_LIMIT = 12 * REQUEST_LIMIT + 1024
REJECTION = b'{"error":"plyswap-rejected"}'


def encode_bytes(value: bytes, encoded: bool = True) -> str:
    if encoded:
        return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")
    return value.decode("latin1")


def decode_bytes(value: Any, encoded: bool = True) -> bytes:
    if not isinstance(value, str):
        raise ValueError("expected string")
    if not encoded:
        return value.encode("latin1")
    # Accept standard/base64url alphabets and optional correct padding, but not
    # garbage, whitespace or nonzero unused bits. Emit unpadded base64url.
    if re.fullmatch(r"[A-Za-z0-9_+/\-]*={0,2}", value) is None:
        raise ValueError("invalid base64")
    unpadded = value.rstrip("=")
    if "=" in value and (
        len(value) % 4 or len(value) - len(unpadded) != -len(unpadded) % 4
    ):
        raise ValueError("invalid base64 padding")
    decoded = base64.b64decode(
        unpadded.replace("-", "+").replace("_", "/") + "=" * (-len(unpadded) % 4),
        validate=True,
    )
    if encode_bytes(decoded) != unpadded.replace("+", "-").replace("/", "_"):
        raise ValueError("nonzero base64 trailing bits")
    return decoded


def _unique_object(pairs: Any) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("duplicate field")
        result[name] = value
    return result


def _invalid_constant(value: str) -> Any:
    raise ValueError("invalid JSON constant")


def _object(body: bytes, allow_room: bool) -> Dict[str, Any]:
    obj = json.loads(
        body.decode("utf-8"),
        object_pairs_hook=_unique_object,
        parse_constant=_invalid_constant,
    )
    allowed = {"self", "peer", "values", "next", "b64"}
    if allow_room:
        allowed.add("room")
    if not isinstance(obj, dict) or "self" not in obj or obj.keys() - allowed:
        raise ValueError("expected PlySwap object")
    flags = obj.get("b64", 0)
    if type(flags) is not int or not 0 <= flags <= 7:
        raise ValueError("invalid b64 bitmap")
    return obj


def _message(obj: Dict[str, Any]) -> Message:
    flags = obj.get("b64", 0)
    values = obj.get("values", [])
    if not isinstance(values, list):
        raise ValueError("expected values array")
    return Message(
        self_xid=decode_bytes(obj["self"], bool(flags & 2)),
        peer_xid=decode_bytes(obj["peer"], bool(flags & 2)) if "peer" in obj else b"",
        next_xid=decode_bytes(obj["next"], bool(flags & 2)) if "next" in obj else b"",
        values=tuple(decode_bytes(value, bool(flags & 1)) for value in values),
    )


def decode_message(body: bytes) -> Message:
    """Core-only JSON packet/reply; no room field is accepted."""
    return _message(_object(body, allow_room=False))


def _encoded(message: Message, flags: int) -> Dict[str, Any]:
    if type(flags) is not int or not 0 <= flags <= 7:
        raise ValueError("invalid b64 bitmap")
    obj: Dict[str, Any] = {"b64": flags} if flags else {}
    obj["self"] = encode_bytes(message.self_xid, bool(flags & 2))
    if bool(message.peer_xid):
        obj["peer"] = encode_bytes(message.peer_xid, bool(flags & 2))
    if bool(message.next_xid):
        obj["next"] = encode_bytes(message.next_xid, bool(flags & 2))
    if message.values:
        obj["values"] = [
            encode_bytes(value, bool(flags & 1)) for value in message.values
        ]
    return obj


def encode_message(message: Message, flags: int = 0) -> bytes:
    return json.dumps(_encoded(message, flags), separators=(",", ":")).encode("ascii")


def encode_request(message: Message, room: bytes, flags: int = 0) -> bytes:
    fields = _encoded(message, flags)
    obj: Dict[str, Any] = {"b64": flags} if flags else {}
    obj["room"] = encode_bytes(room, bool(flags & 4))
    obj.update(fields)
    return json.dumps(obj, separators=(",", ":")).encode("ascii")


def encode_response(response: PlySwapResponse, flags: int = 0) -> bytes:
    return encode_request(response.message, response.room, flags)


def decode_response(body: bytes) -> PlySwapResponse:
    """Successful service reply: room is required in JSON, never implicit."""
    envelope = decode_request(body)
    message = envelope.message
    if (bool(message.next_xid) and bool(message.peer_xid)) or (
        not message.next_xid and message.values
    ):
        raise ValueError("invalid PlySwap reply shape")
    return PlySwapResponse.from_message(envelope.room, message)


def query_from_target(target: str, endpoint: str = ROUTE) -> Optional[str]:
    path, has_query, query = target.partition("?")
    if path != endpoint or "#" in target:
        raise LookupError("unknown endpoint")
    if not has_query:
        return None
    if not query or "&" in query or re.search(r"%(?![0-9A-Fa-f]{2})", query):
        raise ValueError("expected one query room")
    name, separator, value = query.partition("=")
    if not separator or unquote_plus(name, errors="strict") != "room":
        raise ValueError("unknown query parameter")
    # Raw non-ASCII targets are not a second, platform-dependent URL encoding.
    query.encode("ascii")
    return unquote_plus(value, encoding="utf-8", errors="strict")


@dataclass(frozen=True)
class Envelope:
    room: bytes
    message: Message
    b64: int


def decode_request(body: bytes, query_room: Optional[str] = None) -> Envelope:
    obj = _object(body, allow_room=True)
    if ("room" in obj) == (query_room is not None):
        raise ValueError("require room in JSON or query, not both")
    flags = obj.get("b64", 0)
    room = decode_bytes(obj["room"] if "room" in obj else query_room, bool(flags & 4))
    if not room:
        raise ValueError("room must not be empty")
    return Envelope(room, _message(obj), flags)
