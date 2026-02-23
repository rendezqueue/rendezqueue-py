import json
import base64
import time
from typing import Optional, Union, Any, Dict
from rendezqueue.swapstore import SwapStore, TrySwapResponse

MAX_KEY_BYTES = 100
MAX_ID_BYTES = 100
MAX_VALUE_BYTES = 65536


def btoa(s: str) -> str:
    # URL-safe base64 encoding without padding
    return base64.urlsafe_b64encode(s.encode("latin1")).decode("ascii").rstrip("=")


def atob(s: str) -> str:
    # URL-safe base64 decoding with padding added
    missing_padding = len(s) % 4
    if missing_padding:
        s += "=" * (4 - missing_padding)
    return base64.urlsafe_b64decode(s).decode("latin1")


def inplace_decode_tryswap_message(msg: Dict[str, Any]) -> str:
    if "b64" not in msg:
        msg["b64"] = 0
    elif not isinstance(msg["b64"], int) or msg["b64"] < 0:
        return "b64"

    if "ttl" not in msg:
        msg["ttl"] = 0
    elif not isinstance(msg["ttl"], int) or msg["ttl"] < 0:
        return "ttl"

    if "key" not in msg:
        msg["key"] = ""
    elif not isinstance(msg["key"], str):
        return "key"

    if "sid" not in msg:
        msg["sid"] = ""
    elif not isinstance(msg["sid"], str):
        return "sid"

    if "offset" not in msg:
        msg["offset"] = 0
    elif not isinstance(msg["offset"], int) or msg["offset"] < 0:
        return "offset"

    if "values" not in msg:
        msg["values"] = []
    elif not isinstance(msg["values"], list):
        return "values"

    try:
        if msg["b64"] & 4:
            msg["key"] = atob(msg["key"])
        if msg["b64"] & 2:
            msg["sid"] = atob(msg["sid"])
        if msg["b64"] & 1:
            msg["values"] = [atob(v) for v in msg["values"]]
    except Exception:
        return "b64_decode_error"

    return ""


def inplace_encode_tryswap_message(msg: Dict[str, Any]) -> None:
    if "b64" not in msg:
        msg["b64"] = 0

    if msg["b64"] & 4:
        msg["key"] = btoa(msg["key"])
    if msg["b64"] & 2:
        msg["sid"] = btoa(msg["sid"])
    if msg["b64"] & 1:
        if "values" not in msg or msg["values"] is None:
            msg["b64"] &= ~1
        else:
            msg["values"] = [btoa(v) for v in msg["values"]]

    if msg["b64"] == 0:
        del msg["b64"]
    if "ttl" in msg and msg["ttl"] == 0:
        del msg["ttl"]
    if "offset" in msg and msg["offset"] == 0:
        del msg["offset"]
    if "values" in msg and msg["values"] is None:
        del msg["values"]


class RendezqueueImpl:
    def __init__(self) -> None:
        self.swapstore = SwapStore()

    def tryswap(
        self, msg: Any, now_ms: Optional[float] = None
    ) -> Union[int, TrySwapResponse]:
        if not msg:
            return 400

        if not isinstance(msg, dict):
            return 400

        key = msg.get("key", "")
        sid = msg.get("sid", "")
        offset = msg.get("offset", 0)
        values = msg.get("values", [])

        if len(key) > MAX_KEY_BYTES:
            return 413
        if len(sid) > MAX_ID_BYTES:
            return 413

        total_value_bytes = sum(len(v) for v in values)
        if total_value_bytes > MAX_VALUE_BYTES:
            return 413

        if now_ms is None:
            now_ms = time.time() * 1000
            if now_ms == 0:
                return 500
            now_ms = int(now_ms)

        result = self.swapstore.tryswap(
            key,
            sid,
            offset,
            values,
            now_ms,
            msg.get("ttl", 0),
        )

        return result

    def tryswap_string(self, request_text: str) -> Union[int, str]:
        msg: Optional[Dict[str, Any]] = None
        try:
            msg = json.loads(request_text)
            e = inplace_decode_tryswap_message(msg)
            if e:
                raise Exception(e)
        except Exception:
            msg = None

        result = self.tryswap(msg)
        if isinstance(result, int):
            return result

        # Convert dataclass to dict
        res_dict: Dict[str, Any] = {
            "key": result.key,
            "sid": result.sid,
            "offset": result.offset,
        }
        if result.values is not None:
            res_dict["values"] = result.values
        if result.ttl is not None:
            res_dict["ttl"] = result.ttl

        # Carry over b64 flags from request if present
        if msg and "b64" in msg:
            res_dict["b64"] = msg["b64"]

        inplace_encode_tryswap_message(res_dict)
        return json.dumps(res_dict)
