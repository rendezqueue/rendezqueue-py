"""Fixed-channel synchronous HTTP transport."""

import ssl
import urllib.error
import urllib.request
from typing import Optional
from urllib.parse import urlsplit

from rendezqueue.plyswap import Message
from rendezqueue.plyswap_wire import (
    REJECTION,
    REQUEST_LIMIT,
    RESPONSE_LIMIT,
    decode_response,
    encode_request,
)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class HttpTransport:
    def __init__(self, endpoint: str, room: bytes) -> None:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in ("http", "https")
            or not parsed.hostname
            or parsed.query
            or parsed.fragment
            or "?" in endpoint
            or "#" in endpoint
            or parsed.username is not None
            or parsed.password is not None
        ):
            raise ValueError(
                "absolute HTTP(S) endpoint without query/fragment required"
            )
        if not room:
            raise ValueError("room must not be empty")
        if len(encode_request(Message(b"\x00"), room, 7)) > REQUEST_LIMIT:
            raise ValueError("room exceeds request envelope limit")
        self._endpoint = endpoint
        self._room = bytes(room)
        self._opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            _NoRedirect(),
            urllib.request.HTTPSHandler(context=ssl.create_default_context()),
        )

    def exchange(self, message: Message) -> Optional[Message]:
        body = encode_request(message, self._room, 7)
        if len(body) > REQUEST_LIMIT:
            raise ValueError(
                "not sent: request exceeds 1 MiB; frozen batch remains retained"
            )
        request = urllib.request.Request(
            self._endpoint,
            data=body,
            headers={
                "Content-Type": "application/json",
                "Cache-Control": "no-store",
                "User-Agent": "Rendezqueue-Python-Client/0.0.0",
            },
            method="POST",
        )
        try:
            response = self._opener.open(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            status = response.status
            if status not in (200, 409):
                raise RuntimeError(f"HTTP {status}; delivery may have occurred")
            content_types = response.headers.get_all("Content-Type", [])
            if (
                len(content_types) != 1
                or content_types[0].split(";", 1)[0].strip().lower()
                != "application/json"
                or "Content-Encoding" in response.headers
            ):
                raise ValueError(
                    "invalid response content type/encoding; delivery is uncertain"
                )
            data = response.read(RESPONSE_LIMIT + 1)
            if len(data) > RESPONSE_LIMIT:
                raise ValueError("response exceeds size limit; delivery is uncertain")
            lengths = response.headers.get_all("Content-Length", [])
            if len(lengths) > 1 or (
                lengths
                and (
                    not lengths[0].isascii()
                    or not lengths[0].isdecimal()
                    or int(lengths[0]) != len(data)
                )
            ):
                raise ValueError(
                    "invalid/truncated response length; delivery is uncertain"
                )
        if status == 409:
            if data != REJECTION:
                raise ValueError(
                    "not a PlySwap protocol rejection; delivery is uncertain"
                )
            return None
        response = decode_response(data)
        if response.room != self._room:
            raise ValueError(
                "response room does not match request; delivery is uncertain"
            )
        reply = response.message
        if reply.self_xid != message.self_xid:
            raise ValueError(
                "response self does not match request; delivery is uncertain"
            )
        return reply
