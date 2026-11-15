import http.client
import json
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

from rendezqueue.impl import RendezqueueImpl
from rendezqueue.plyswap import Message, PlySwapResponse
from rendezqueue.plyswap_wire import (
    REJECTION,
    REQUEST_LIMIT,
    decode_response,
    encode_request,
)
from rendezqueue.plyswap_cache import PlySwapCache
from rendezqueue.server import RendezqueueHandler, RendezqueueServer, run

PATH = "/plyswap"


class QuietHandler(RendezqueueHandler):
    def log_message(self, format, *args):
        pass


@pytest.fixture
def server(request):
    server = RendezqueueServer(
        ("127.0.0.1", 0),
        QuietHandler,
        RendezqueueImpl(),
        http_path=PATH,
        tryswap_http_path=getattr(request, "param", None),
    )
    worker = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}
    )
    worker.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        worker.join(5)
        assert not worker.is_alive()


def rpc(server, message, path=PATH, room=b"room", flags=0):
    status, headers, body = post(
        server, encode_request(message, room, flags), path=path
    )
    assert headers["Cache-Control"] == "no-store"
    assert headers["Content-Type"] == "application/json"
    if status == 200:
        response = decode_response(body)
        assert response.room == room
        return status, response.message
    return status, body


def post(server, body, path=PATH, method="POST", content_type="application/json"):
    conn = http.client.HTTPConnection(*server.server_address[:2], timeout=5)
    try:
        conn.request(method, path, body, {"Content-Type": content_type})
        response = conn.getresponse()
        return response.status, response.headers, response.read()
    finally:
        conn.close()


def raw(server, request):
    with socket.create_connection(server.server_address[:2], timeout=5) as conn:
        conn.sendall(request)
        conn.shutdown(socket.SHUT_WR)
        response = http.client.HTTPResponse(conn)
        response.begin()
        return response.status, response.headers, response.read()


def declared_post(server, path, length):
    return raw(
        server,
        f"POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {length}\r\n\r\n".encode(),
    )


def test_duplex_and_duplicate_reference_over_http(server):
    assert rpc(server, Message(b"a")) == (200, Message(b"a"))
    assert rpc(server, Message(b"b")) == (200, Message(b"b", peer_xid=b"a"))
    assert rpc(server, Message(b"a")) == (200, Message(b"a", peer_xid=b"b"))
    a = Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"", b"\xff\x00"))
    b = Message(b"b", peer_xid=b"a", next_xid=b"b1", values=(b"reply",))
    assert rpc(server, a) == (200, Message(b"a"))
    assert rpc(server, b) == (200, Message(b"b", next_xid=b"a1", values=a.values))
    for _ in range(2):
        assert rpc(server, Message(b"a", next_xid=b"a1")) == (
            200,
            Message(b"a", next_xid=b"b1", values=b.values),
        )
    assert rpc(server, Message(b"a", next_xid=b"wrong")) == (409, REJECTION)


def test_empty_completion_and_full_recovery_over_http(server):
    a = Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"data",))
    assert rpc(server, a) == (200, Message(b"a"))
    assert rpc(server, Message(b"b", peer_xid=b"a")) == (
        200,
        Message(b"b", next_xid=b"a1", values=(b"data",)),
    )
    assert rpc(server, Message(b"a", next_xid=b"a1")) == (200, Message(b"a"))
    assert rpc(server, Message(b"b", peer_xid=b"a", next_xid=b"b1")) == (
        200,
        Message(b"b", next_xid=b"a1", values=(b"data",)),
    )
    assert rpc(server, Message(b"a", next_xid=b"a1")) == (
        200,
        Message(b"a", next_xid=b"b1"),
    )
    # Explicit cache loss. No client state is imported or reconstructed by HTTP.
    server.plyswap = PlySwapCache()
    assert rpc(server, Message(b"a", next_xid=b"a1")) == (409, REJECTION)
    assert server.plyswap.room_count == 0
    assert rpc(server, a) == (200, Message(b"a"))
    assert rpc(server, Message(b"b", peer_xid=b"a", next_xid=b"b1")) == (
        200,
        Message(b"b", next_xid=b"a1", values=(b"data",)),
    )
    assert rpc(server, Message(b"a", next_xid=b"a1")) == (
        200,
        Message(b"a", next_xid=b"b1"),
    )


@pytest.mark.parametrize("server", ["/tryswap"], indirect=True)
def test_room_and_legacy_isolation(server):
    legacy = {"key": "room", "sid": "a", "ack": "b", "values": ["legacy"], "offset": 0}
    assert post(server, json.dumps(legacy), path="/tryswap")[0] == 200
    assert rpc(server, Message(b"a")) == (200, Message(b"a"))
    assert rpc(server, Message(b"b"), room=b"other") == (
        200,
        Message(b"b"),
    )
    assert rpc(server, Message(b"b")) == (200, Message(b"b", peer_xid=b"a"))
    # A legacy-shaped body is never sniffed or downgraded on the new route.
    assert post(server, json.dumps(legacy))[0] == 400
    assert post(server, b"", path="/tryswap", method="OPTIONS")[0] == 204


@pytest.mark.parametrize(
    "method", ["GET", "HEAD", "PUT", "OPTIONS", "DELETE", "PATCH", "WHATEVER"]
)
def test_unsupported_methods(server, method):
    status, headers, body = post(server, b"", method=method)
    assert status == 405
    assert headers["Cache-Control"] == "no-store"
    assert body == (b"" if method == "HEAD" else b'{"error":"POST required"}')
    assert server.plyswap.room_count == 0


@pytest.mark.parametrize(
    "path",
    [
        PATH + "/",
        PATH + "#x",
        PATH + "=",
        "/plyswap/%63m9vbQ",
        "/plyswap/cm9vbR",
        "/plyswap/room",
        "//plyswap/cm9vbQ",
    ],
)
def test_wrong_routes(server, path):
    status, _, _ = post(server, b'{"room":"room","self":"a"}', path=path)
    assert status == 404
    assert server.plyswap.room_count == 0


# Keep the envelope valid so a missing admission check cannot hide behind a
# later JSON/room failure. Duplicate self deliberately mutates only that property.
VALID_REQUEST = b'{"room":"room","self":"a"}'
DUPLICATE_SELF_REQUEST = b'{"room":"room","self":"a","self":"b"}'
JSON_HEADER = b"Content-Type: application/json\r\n"
LENGTH_HEADER = f"Content-Length: {len(VALID_REQUEST)}\r\n".encode()


@pytest.mark.parametrize(
    "headers,body,status",
    [
        pytest.param(
            JSON_HEADER + LENGTH_HEADER, VALID_REQUEST, 200, id="valid-control"
        ),
        pytest.param(
            b"Content-Type: text/plain\r\n" + LENGTH_HEADER,
            VALID_REQUEST,
            415,
            id="wrong-media-type",
        ),
        pytest.param(
            JSON_HEADER + JSON_HEADER + LENGTH_HEADER,
            VALID_REQUEST,
            415,
            id="duplicate-media-type",
        ),
        pytest.param(JSON_HEADER, VALID_REQUEST, 411, id="missing-length"),
        pytest.param(
            JSON_HEADER + LENGTH_HEADER + LENGTH_HEADER,
            VALID_REQUEST,
            411,
            id="duplicate-length",
        ),
        pytest.param(
            JSON_HEADER + LENGTH_HEADER + b"Transfer-Encoding: chunked\r\n",
            VALID_REQUEST,
            411,
            id="transfer-encoding",
        ),
        pytest.param(
            JSON_HEADER + b"Content-Length: -1\r\n",
            VALID_REQUEST,
            400,
            id="negative-length",
        ),
        pytest.param(
            JSON_HEADER + b"Content-Length: nope\r\n",
            VALID_REQUEST,
            400,
            id="nondecimal-length",
        ),
        pytest.param(
            JSON_HEADER + f"Content-Length: +{len(VALID_REQUEST)}\r\n".encode(),
            VALID_REQUEST,
            400,
            id="signed-length",
        ),
        pytest.param(
            JSON_HEADER + b"Content-Length: 2000\r\n",
            VALID_REQUEST,
            400,
            id="truncated-body",
        ),
        pytest.param(
            JSON_HEADER + LENGTH_HEADER + b"Content-Encoding: identity\r\n",
            VALID_REQUEST,
            415,
            id="content-encoding",
        ),
        pytest.param(
            JSON_HEADER + LENGTH_HEADER + b"Upgrade: websocket\r\n",
            VALID_REQUEST,
            400,
            id="upgrade-header",
        ),
        pytest.param(
            JSON_HEADER + LENGTH_HEADER + b"Connection: keep-alive, UpGrade\r\n",
            VALID_REQUEST,
            400,
            id="connection-upgrade-token",
        ),
        # Declaration-only: oversize must be rejected before reading the body.
        pytest.param(
            JSON_HEADER + f"Content-Length: {REQUEST_LIMIT + 1}\r\n".encode(),
            b"",
            413,
            id="oversized-declaration",
        ),
        pytest.param(
            JSON_HEADER + f"Content-Length: {len(DUPLICATE_SELF_REQUEST)}\r\n".encode(),
            DUPLICATE_SELF_REQUEST,
            400,
            id="duplicate-self",
        ),
    ],
)
def test_strict_http_admission(server, headers, body, status):
    actual, response_headers, response = raw(
        server,
        b"POST "
        + PATH.encode()
        + b" HTTP/1.1\r\nHost: localhost\r\n"
        + headers
        + b"\r\n"
        + body,
    )
    assert actual == status
    assert response_headers["Cache-Control"] == "no-store"
    if status == 200:
        assert json.loads(response) == {"room": "room", "self": "a"}
        assert server.plyswap.room_count == 1
    else:
        assert server.plyswap.room_count == 0


def test_content_type_parameters_and_body_limit(server):
    assert (
        post(
            server,
            b'{"room":"room","self":"a"}',
            content_type="Application/JSON; charset=utf-8",
        )[0]
        == 200
    )
    # Whitespace contributes to the envelope limit, even if JSON itself is tiny.
    assert post(server, b'{"room":"room","self":"a"}'.ljust(REQUEST_LIMIT))[0] == 200
    assert declared_post(server, PATH, REQUEST_LIMIT + 1)[0] == 413


def test_malformed_admission_does_not_refresh_a_row(server):
    now = 0
    server.plyswap = PlySwapCache(ttl=20, clock=lambda: now)
    assert (
        rpc(server, Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"frozen",)))[
            0
        ]
        == 200
    )
    now = 19
    for body in [
        b"not JSON",
        b'{"room":"room","self":"a","ttl":20}',
        b'{"room":"room","self":"a","peer":null}',
        b'{"room":"room","self":"a","self":"a"}',
    ]:
        assert post(server, body)[0] == 400
    assert server.plyswap._rooms[b"room"].refreshed == {b"a": 0}
    now = 20
    assert rpc(server, Message(b"a", next_xid=b"a1")) == (409, REJECTION)
    assert server.plyswap.room_count == 0


def test_idle_cleanup_without_requests_even_while_body_is_blocked(server):
    now = 0
    server.plyswap = PlySwapCache(ttl=20, clock=lambda: now)
    assert rpc(server, Message(b"a"))[0] == 200
    body_started = threading.Event()
    body_finished = threading.Event()
    cleaned = threading.Event()
    original_post = QuietHandler.do_POST
    original_sweep = server.plyswap.sweep

    def observed_post(handler):
        original_read = handler.rfile.read

        def observed_read(length):
            body_started.set()
            result = original_read(length)
            body_finished.set()
            return result

        with patch.object(handler.rfile, "read", observed_read):
            original_post(handler)

    def observed_sweep():
        original_sweep()
        if server.plyswap.room_count == 0:
            cleaned.set()

    with (
        patch.object(QuietHandler, "do_POST", observed_post),
        patch.object(server.plyswap, "sweep", observed_sweep),
    ):
        with socket.create_connection(server.server_address[:2], timeout=5) as conn:
            body = b'{"room":"slow","self":"a"}'
            conn.sendall(
                f"POST {PATH} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n".encode()
                + body[:1]
            )
            assert body_started.wait(5)
            now = 20
            assert cleaned.wait(5), "idle cleanup waited for the unfinished body"
            assert not body_finished.is_set(), (
                "body read was not pending during cleanup"
            )
            assert server.plyswap.room_count == 0
            conn.sendall(body[1:])
            response = http.client.HTTPResponse(conn)
            response.begin()
            assert response.status == 200
            response.read()
            assert body_finished.is_set()
    assert server.plyswap.room_count == 1


def test_concurrent_duplicate_requests_and_unexpired_sweep_smoke(server):
    # Nonconflicting HTTP operations: smoke coverage, not a forced lock ordering.
    request = Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"immutable",))
    barrier = threading.Barrier(9)

    def work(index):
        barrier.wait(timeout=5)
        if index == 8:
            server.plyswap.sweep()
            return
        assert rpc(server, request) == (200, Message(b"a"))

    with ThreadPoolExecutor(max_workers=9) as pool:
        list(pool.map(work, range(9)))
    assert len(server.plyswap._rooms[b"room"].core.rows) == 1
    assert rpc(server, Message(b"b", peer_xid=b"a", next_xid=b"b1")) == (
        200,
        Message(b"b", next_xid=b"a1", values=request.values),
    )


def test_cli_wires_two_full_paths_without_appending_suffixes():
    fake = MagicMock()
    fake.server_address = ("127.0.0.1", 1234)
    with (
        patch(
            "sys.argv",
            [
                "rendezqueue-server",
                "--tryswap_http_path",
                "/",
                "--http_path",
                "/chosen/rpc",
            ],
        ),
        patch("rendezqueue.server.RendezqueueServer", return_value=fake) as factory,
    ):
        run()
    assert factory.call_args.kwargs["tryswap_http_path"] == "/"
    assert factory.call_args.kwargs["http_path"] == "/chosen/rpc"
    fake.serve_forever.assert_called_once()
    fake.server_close.assert_called_once()


@pytest.mark.parametrize("path", [None, "/", "/chosen/rpc"])
def test_cli_http_path_selects_plyswap_without_enabling_tryswap(path, capsys):
    fake = MagicMock()
    fake.server_address = ("127.0.0.1", 1234)
    argv = ["rendezqueue-server"]
    if path is not None:
        argv.extend(["--http_path", path])
    with (
        patch("sys.argv", argv),
        patch("rendezqueue.server.RendezqueueServer", return_value=fake) as factory,
    ):
        run()
    selected = "/" if path is None else path
    assert factory.call_args.kwargs["http_path"] == selected
    assert factory.call_args.kwargs["tryswap_http_path"] is None
    assert capsys.readouterr().out == f"PlySwap at http://127.0.0.1:1234{selected}\n"


@pytest.mark.parametrize("method", ["POST", "OPTIONS"])
def test_tryswap_endpoint_is_absent_by_default(server, method):
    assert server.tryswap_http_path is None
    with patch.object(
        server.impl, "tryswap_string", side_effect=AssertionError("TrySwap dispatched")
    ):
        status, headers, _ = post(
            server, b'{"key":"room","sid":"a"}', path="/tryswap", method=method
        )
    assert status == 404
    assert headers.get("Access-Control-Allow-Origin") is None
    assert server.plyswap.room_count == 0
    assert rpc(server, Message(b"a")) == (200, Message(b"a"))


def test_equal_route_configuration():
    with pytest.raises(ValueError, match="must differ"):
        RendezqueueServer(
            ("127.0.0.1", 0),
            QuietHandler,
            RendezqueueImpl(),
            http_path=PATH,
            tryswap_http_path=PATH,
        )


@pytest.mark.parametrize(
    "old,new", [("key", "room"), ("sid", "self"), ("ack", "peer"), ("nxt", "next")]
)
def test_old_wire_fields_reject_before_cache_admission(server, old, new):
    first = {
        "room": "room",
        "self": "a",
        "peer": "b",
        "next": "a1",
        "values": ["first"],
    }
    assert post(server, json.dumps(first).encode())[0] == 200

    def snapshot():
        with server.plyswap._lock:
            return {
                room: (dict(state.core.rows), dict(state.refreshed))
                for room, state in server.plyswap._rooms.items()
            }

    before = snapshot()
    second = {
        "room": "room",
        "self": "b",
        "peer": "a",
        "next": "b1",
        "values": ["second"],
    }
    invalid = dict(second)
    invalid[old] = invalid[new]
    assert post(server, json.dumps(invalid).encode())[0] == 400
    assert snapshot() == before
    # Positive control: the same packet without the old field completes the pair.
    status, _, body = post(server, json.dumps(second).encode())
    assert status == 200
    assert decode_response(body) == PlySwapResponse(
        b"room", b"b", next_xid=b"a1", values=(b"first",)
    )


@pytest.mark.parametrize("body", [b'{"self":"a"}', b'{"room":"room","self":"a"}'])
def test_old_query_room_is_not_an_alias_or_ignored(server, body):
    assert post(server, body, path=PATH + "?key=room")[0] == 400
    assert server.plyswap.room_count == 0


def test_raw_json_and_query_rooms_share_the_same_room(server):
    a = b'{"room":"room","self":"a","peer":"b","values":["hello"],"next":"a1"}'
    assert post(server, a)[0] == 200
    status, _, body = post(
        server, b'{"self":"b","peer":"a","next":"b1"}', path=PATH + "?room=room"
    )
    assert status == 200
    assert json.loads(body) == {
        "room": "room",
        "self": "b",
        "values": ["hello"],
        "next": "a1",
    }
    assert rpc(server, Message(b"a", next_xid=b"a1"), flags=7) == (
        200,
        Message(b"a", next_xid=b"b1"),
    )
    assert server.plyswap.room_count == 1


def test_encoded_and_raw_bytes_share_snapshots_and_response_flags(server):
    full = Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"\x00\xff", b""))
    assert rpc(server, full, room=b"\xff", flags=7) == (200, Message(b"a"))
    # Same snapshot in a different encoding is a replay, not changed input.
    assert rpc(server, full, room=b"\xff", flags=0) == (200, Message(b"a"))
    status, _, body = post(
        server, b'{"self":"b","peer":"a","next":"b1"}', path=PATH + "?room=%C3%BF"
    )
    assert status == 200
    assert json.loads(body) == {
        "room": "\u00ff",
        "self": "b",
        "values": ["\u0000\u00ff", ""],
        "next": "a1",
    }
    status, _, body = post(
        server,
        b'{"self":"Yg","peer":"YQ","next":"YjE","b64":6}',
        path=PATH + "?room=_w",
    )
    assert status == 200
    assert json.loads(body) == {
        "b64": 6,
        "room": "_w",
        "self": "Yg",
        "values": ["\u0000\u00ff", ""],
        "next": "YTE",
    }


@pytest.mark.parametrize(
    "path,body",
    [
        (PATH, b'{"self":"a"}'),
        (PATH, b'{"room":"","self":"a"}'),
        (PATH, b'{"room":null,"self":"a"}'),
        (PATH, b'{"room":"x","room":"x","self":"a"}'),
        (PATH + "?room=room", b'{"room":"room","self":"a"}'),
        (PATH + "?room=", b'{"self":"a"}'),
        (PATH + "?room=x&room=x", b'{"self":"a"}'),
        (PATH + "?room=x&other=y", b'{"self":"a"}'),
        (PATH + "?other=x", b'{"self":"a"}'),
        (PATH + "?room=%FF", b'{"self":"a"}'),
        (PATH + "?room=%ZZ", b'{"self":"a"}'),
        (PATH + "?room=%", b'{"self":"a"}'),
        (PATH + "?room=%C4%80", b'{"self":"a"}'),
        (PATH, b'{"room":"room","self":"a","b64":true}'),
        (PATH, b'{"room":"room","self":"a","b64":8}'),
    ],
)
def test_room_and_bitmap_errors_do_not_create_rooms(server, path, body):
    status, headers, _ = post(server, body, path=path)
    assert status == 400
    assert headers["Cache-Control"] == "no-store"
    assert server.plyswap.room_count == 0


def test_query_bytes_are_charged_to_the_admission_limit(server):
    status, _, _ = declared_post(server, PATH + "?room=room", REQUEST_LIMIT)
    assert status == 413
    assert server.plyswap.room_count == 0


def test_raw_reply_expansion_can_exceed_the_old_two_mib_bound(server):
    payload = b"\x00" * 400_000
    assert (
        rpc(
            server,
            Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(payload,)),
            flags=7,
        )[0]
        == 200
    )
    status, _, body = post(server, b'{"room":"room","self":"b","peer":"a","next":"b1"}')
    assert status == 200
    assert len(body) > 2 * REQUEST_LIMIT + 1024
    assert decode_response(body) == PlySwapResponse(
        b"room", b"b", next_xid=b"a1", values=(payload,)
    )


@pytest.mark.parametrize(
    "trypath,plypath", [(None, "/"), ("/", "/custom/ply"), ("/tryswap", "/plyswap")]
)
def test_two_independent_full_endpoint_paths(trypath, plypath):
    server = RendezqueueServer(
        ("127.0.0.1", 0),
        QuietHandler,
        RendezqueueImpl(),
        http_path=plypath,
        tryswap_http_path=trypath,
    )
    worker = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}
    )
    worker.start()
    try:
        if trypath is not None:
            assert post(server, b'{"key":"room","sid":"a"}', path=trypath)[0] == 200
            assert post(server, b"", path=trypath, method="OPTIONS")[0] == 204
        else:
            assert post(server, b'{"key":"room","sid":"a"}', path="/tryswap")[0] == 404
            assert post(server, b"", path="/tryswap", method="OPTIONS")[0] == 404
        assert rpc(server, Message(b"a"), path=plypath) == (200, Message(b"a"))
        assert (
            post(server, b'{"room":"room","self":"a"}', path=plypath + "/room")[0]
            == 404
        )
        assert server.plyswap.room_count == 1
    finally:
        server.shutdown()
        server.server_close()
        worker.join(5)
        assert not worker.is_alive()


@pytest.mark.parametrize(
    "path", ["relative", "//alias", "/query?x=1", "/fragment#x", "/has space"]
)
@pytest.mark.parametrize("option", ["http_path", "tryswap_http_path"])
def test_invalid_configured_endpoint_paths(path, option):
    with pytest.raises(ValueError, match="full absolute"):
        RendezqueueServer(
            ("127.0.0.1", 0), QuietHandler, RendezqueueImpl(), **{option: path}
        )
