from contextlib import contextmanager
import io
import ssl
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError

import pytest

from rendezqueue import RendezqueueClient
from rendezqueue.plyswap import Message, PlySwapResponse
from rendezqueue.plyswap_transport import HttpTransport
from rendezqueue.plyswap_wire import REJECTION, RESPONSE_LIMIT, encode_response
from client_test import Response, opening, transport as transport
from plyswap_http_test import server as server  # Shared real Python endpoint fixture.


@pytest.mark.parametrize(
    "mutation",
    ["encoding", "duplicate-type", "duplicate-length", "length", "bad-length"],
)
def test_otherwise_valid_response_headers_are_checked(transport, mutation):
    a, ar, ac, errors = transport.client("a")
    b, _, _, _ = transport.client("b")
    a.send("frozen")
    opening(a, b)
    a._poll()
    retained = a._core.stored()

    def corrupt(request, reply):
        if mutation == "encoding":
            reply.headers["Content-Encoding"] = "identity"
        elif mutation == "duplicate-type":
            reply.headers["Content-Type"] = "application/json"
        elif mutation == "duplicate-length":
            reply.headers["Content-Length"] = str(len(reply.body))
        else:
            reply.headers.replace_header(
                "Content-Length",
                "invalid" if mutation == "bad-length" else str(len(reply.body) + 1),
            )
        return reply

    transport.after = corrupt
    a._poll()
    assert len(errors) == 1 and not ar and not ac
    assert a._core.stored() == retained and a._core.current_cached


def test_oversized_otherwise_valid_json_is_bounded_before_decoding(transport):
    a, ar, ac, errors = transport.client("a")
    limits = []

    class OversizedResponse(Response):
        def read(self, limit=-1):
            limits.append(limit)
            return self.body if limit < 0 else self.body[:limit]

    response = OversizedResponse(
        encode_response(PlySwapResponse(b"room", a._core.self_xid)).ljust(
            RESPONSE_LIMIT + 1
        )
    )
    transport.after = lambda request, original: response
    a._poll()
    assert limits == [RESPONSE_LIMIT + 1]
    assert len(errors) == 1 and "size limit" in str(errors[0])
    assert not ar and not ac and response.closed


def test_urllib_http_error_exact_409_is_reference_recovery(transport):
    a, _, _, errors = transport.client("a")
    b, _, _, _ = transport.client("b")
    a.send("frozen")
    opening(a, b)
    a._poll()
    full = transport.requests[-1]
    body = io.BytesIO(REJECTION)
    headers = Response(REJECTION, 409).headers

    def reject(request, reply):
        raise HTTPError(a.url, 409, "conflict", headers, body)

    transport.after = reject
    a._poll()
    assert body.closed and not a._core.current_cached and not errors
    transport.after = lambda request, reply: reply
    a._poll()
    assert transport.requests[-1] == full


def test_tls_verification_is_enabled_not_the_legacy_unverified_context():
    transport = HttpTransport("https://example.test/custom", b"room")
    handler = next(
        handler
        for handler in getattr(transport._opener, "handlers")
        if hasattr(handler, "_context")
    )
    assert handler._context.verify_mode == ssl.CERT_REQUIRED
    assert handler._context.check_hostname


@pytest.mark.parametrize(
    "url",
    [
        "relative",
        "ftp://host/rpc",
        "http:///rpc",
        "http://host/rpc?key=room",
        "http://host/rpc#fragment",
        "http://user:pass@host/rpc",
    ],
)
def test_transport_rejects_unpinnable_endpoints(url):
    with pytest.raises(ValueError):
        HttpTransport(url, b"room")


def test_real_main_api_ignores_environment_proxies_and_uses_plyswap(
    server, monkeypatch
):
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.setenv(name, "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    url = f"http://127.0.0.1:{server.server_port}/plyswap"
    received, completed, errors = [], [], []
    client = RendezqueueClient(
        url,
        "live-main-api",
        "probe",
        received.extend,
        errors.append,
        poll_interval_ms=500,
        on_complete=lambda: completed.append(True),
    )
    # One synchronous OPEN proves SDK routing/proxy bypass; no chat or scheduler
    # is needed to test whether the real HTTP request reaches the right service.
    client._poll()
    assert not received and not completed and not errors
    assert server.plyswap.room_count == 1
    assert client._core.self_xid in server.plyswap._rooms[b"live-main-api"].core.rows


def test_real_protocol_rejection_is_not_an_http_error(server):
    transport = HttpTransport(
        f"http://127.0.0.1:{server.server_port}/plyswap", b"missing"
    )
    assert transport.exchange(Message(b"a", next_xid=b"a1")) is None
    assert server.plyswap.room_count == 0


def test_legacy_endpoint_is_not_a_silent_fallback(server):
    errors, received = [], []
    client = RendezqueueClient(
        f"http://127.0.0.1:{server.server_port}/tryswap",
        "room",
        "main",
        received.append,
        errors.append,
    )
    client.send("retained")
    client._poll()
    assert len(errors) == 1 and not received
    assert client.outgoing_queue == [b"retained"]
    assert client._core.generation == 0 and server.plyswap.room_count == 0


@contextmanager
def stub(action):
    calls = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

        def do_GET(self):
            self.respond()

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", "0")))
            self.respond()

        def respond(self):
            calls.append(self.command)
            status, headers, body = action()
            self.send_response(status)
            for name, value in headers:
                self.send_header(name, value)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    listener = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(
        target=listener.serve_forever, kwargs={"poll_interval": 0.01}
    )
    worker.start()
    try:
        yield f"http://127.0.0.1:{listener.server_port}/custom", calls
    finally:
        listener.shutdown()
        listener.server_close()
        worker.join(5)
        assert not worker.is_alive()


def test_redirect_cannot_move_the_channel_even_if_destination_would_succeed():
    reply = encode_response(PlySwapResponse(b"room", b"own"))
    with stub(lambda: (200, [("Content-Type", "application/json")], reply)) as (
        destination,
        hits,
    ):
        with stub(
            lambda: (
                302,
                [("Location", destination), ("Content-Type", "application/json")],
                reply,
            )
        ) as (origin, requests):
            with pytest.raises(RuntimeError, match="HTTP 302"):
                HttpTransport(origin, b"room").exchange(Message(b"own"))
            assert requests == ["POST"]
            assert not hits
