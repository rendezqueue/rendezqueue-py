"""HTTP admission for PlySwap. No socket work takes a cache lock."""

import json
import re
from http.server import BaseHTTPRequestHandler
from typing import Tuple

from rendezqueue.plyswap_wire import (
    REJECTION,
    REQUEST_LIMIT,
    decode_request,
    encode_response,
    query_from_target,
)
from rendezqueue.plyswap_cache import PlySwapCache


def _error(status: int, message: str) -> Tuple[int, bytes]:
    return status, json.dumps({"error": message}, separators=(",", ":")).encode("ascii")


def _dispatch(
    handler: BaseHTTPRequestHandler, cache: PlySwapCache, endpoint: str
) -> Tuple[int, bytes]:
    # self.path loses leading-double-slash aliases in BaseHTTPRequestHandler.
    target = handler.requestline.split()[1]
    try:
        query_room = query_from_target(target, endpoint)
    except LookupError:
        return _error(404, "unknown endpoint")
    except ValueError:
        return _error(400, "invalid query room")
    if handler.command != "POST":
        return _error(405, "POST required")
    content_types = handler.headers.get_all("Content-Type", [])
    if len(content_types) != 1 or (
        content_types[0].split(";", 1)[0].strip().lower() != "application/json"
    ):
        return _error(415, "application/json required")
    if "Content-Encoding" in handler.headers:
        return _error(415, "content encoding not supported")
    lengths = handler.headers.get_all("Content-Length", [])
    if "Transfer-Encoding" in handler.headers or len(lengths) != 1:
        return _error(411, "one Content-Length required; transfer encoding unsupported")
    if "Upgrade" in handler.headers or any(
        token.strip().lower() == "upgrade"
        for value in handler.headers.get_all("Connection", [])
        for token in value.split(",")
    ):
        return _error(400, "connection upgrade unsupported")
    if re.fullmatch(r"[0-9]+", lengths[0]) is None:
        return _error(400, "invalid Content-Length")
    try:
        length = int(lengths[0])
    except ValueError:
        return _error(400, "invalid Content-Length")
    if length + len(target.partition("?")[2]) > REQUEST_LIMIT:
        return _error(413, "request too large")
    try:
        body = handler.rfile.read(length)
    except OSError:
        return _error(400, "body read failed")
    if len(body) != length:
        return _error(400, "incomplete request body")
    try:
        envelope = decode_request(body, query_room)
    except (ValueError, RecursionError):
        return _error(400, "invalid PlySwap JSON")
    reply = cache.serve(envelope.room, envelope.message)
    if reply is None:
        return 409, REJECTION
    return 200, encode_response(reply, envelope.b64)


def handle(handler: BaseHTTPRequestHandler, cache: PlySwapCache, endpoint: str) -> None:
    status, body = _dispatch(handler, cache, endpoint)
    # Close even on early admission errors: unread bytes cannot become another
    # request. A failed response write never rolls back an admitted transition.
    handler.close_connection = True
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Connection", "close")
    handler.end_headers()
    if handler.command != "HEAD":
        handler.wfile.write(body)
