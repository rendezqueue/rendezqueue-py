"""Room-scoped wire envelopes, keyed class results and response admission."""

import json
import threading
from urllib.parse import urlencode

import pytest

from rendezqueue.client import RendezqueueClient
from rendezqueue.plyswap import Message, PlySwapResponse
from rendezqueue.plyswap_cache import PlySwapCache
from rendezqueue.plyswap_wire import (
    decode_response,
    encode_bytes,
    encode_request,
    encode_response,
)
from client_test import Response, opening, transport as transport
from plyswap_http_test import PATH, post, server as server


def test_room_is_returned_by_the_class_without_http():
    service = PlySwapCache(clock=lambda: 0)
    assert service.serve(b"one", Message(b"a")) == PlySwapResponse(b"one", b"a")
    assert service.serve(b"two", Message(b"a")) == PlySwapResponse(b"two", b"a")
    assert service.serve(b"one", Message(b"b")) == PlySwapResponse(
        b"one", b"b", peer_xid=b"a"
    )
    assert service.serve(b"two", Message(b"b")) == PlySwapResponse(
        b"two", b"b", peer_xid=b"a"
    )


@pytest.mark.parametrize("flags", range(8))
def test_response_room_and_all_bitmap_bits_roundtrip(flags):
    room = bytes(range(256))
    response = PlySwapResponse(
        room, bytes(range(255, -1, -1)), next_xid=b"\x00\xff", values=(b"", room)
    )
    body = encode_response(response, flags)
    assert decode_response(body) == response
    obj = json.loads(body)
    assert obj.get("b64", 0) == flags
    assert list(obj) == (["b64"] if flags else []) + ["room", "self", "next", "values"]
    assert obj["room"] == encode_bytes(room, bool(flags & 4))


@pytest.mark.parametrize(
    "body",
    [
        b'{"self":"a"}',
        b'{"room":"","self":"a"}',
        b'{"room":null,"self":"a"}',
        b'{"room":1,"self":"a"}',
        b'{"room":"room","room":"room","self":"a"}',
        b'{"room":"!","self":"a","b64":4}',
        b'{"room":"room","self":"a","b64":8}',
        b'{"room":"room","self":"a","b64":true}',
        b'{"room":"room","self":"a","peer":"b","next":"c"}',
        b'{"room":"room","self":"a","values":["unsealed"]}',
        b'{"room":"room","self":"a","unexpected":1}',
    ],
)
def test_response_requires_a_valid_room_and_reply_shape(body):
    with pytest.raises(ValueError):
        decode_response(body)


@pytest.mark.parametrize("flags", range(8))
@pytest.mark.parametrize("query", [False, True])
def test_http_echoes_json_and_query_rooms_with_the_full_bitmap(server, flags, query):
    room, self_xid = b"\x00\xff\x80", b"\xff\x00"
    body = encode_request(Message(self_xid), room, flags)
    path = PATH
    if query:
        obj = json.loads(body)
        path += "?" + urlencode({"room": obj.pop("room")})
        body = json.dumps(obj).encode()
    status, _, body = post(server, body, path=path)
    assert status == 200
    assert decode_response(body) == PlySwapResponse(room, self_xid)
    obj = json.loads(body)
    assert obj.get("b64", 0) == flags
    assert list(obj) == (["b64"] if flags else []) + ["room", "self"]
    assert obj["room"] == encode_bytes(room, bool(flags & 4))


@pytest.mark.parametrize(
    "mutation, error_text",
    [
        ("wrong", "room does not match"),
        ("missing", "require room"),
        ("null", "expected string"),
        ("duplicate", "duplicate field"),
    ],
)
def test_matching_self_cannot_hide_wrong_or_missing_response_rooms(
    transport, mutation, error_text
):
    client, received, completed, errors = transport.client("a")
    peer, _, _, _ = transport.client("b")
    client.send("retained")
    opening(client, peer)
    client._poll()
    before = client._core.stored()
    assert client._core.current_cached

    def corrupt(request, original):
        obj = {"room": "room", "self": request.self_xid.decode("latin1")}
        if mutation == "missing":
            del obj["room"]
        elif mutation == "null":
            obj["room"] = None
        elif mutation == "wrong":
            obj["room"] = "other"
        body = json.dumps(obj).encode()
        if mutation == "duplicate":
            body = b'{"room":"room",' + body[1:]
        return Response(body)

    transport.after = corrupt
    client._poll()
    assert len(errors) == 1 and error_text in str(errors[0])
    assert not received and not completed
    assert client._core.stored() == before and client._core.current_cached
    assert client._core.active is None


def test_http_cursor_resumes_with_cached_then_lost_server_state(server):
    url = f"http://127.0.0.1:{server.server_address[1]}{PATH}"
    payloads = (b"initial", b"\x00\xff", b"")
    replies = (b"host", b"", b"recovered")
    received, errors = [], []
    all_inputs_received = threading.Event()

    def receive_input(values):
        received.extend(values)
        if len(received) == len(payloads):
            all_inputs_received.set()

    peer = RendezqueueClient(
        url=url,
        room="resume",
        hue="host",
        on_data=receive_input,
        on_error=errors.append,
        poll_interval_ms=500,
    )
    cursor = None
    clients, received_replies = [], []
    try:
        for index, (payload, reply) in enumerate(zip(payloads, replies)):
            if index == 2:
                peer.stop()
                assert peer.poll_thread is not None
                peer.poll_thread.join(12)
                assert not peer.poll_thread.is_alive()
                with server.plyswap._lock:
                    server.plyswap._rooms.clear()
            incoming = []
            received_replies.append(incoming)
            reply_received = threading.Event()

            def receive_reply(values, incoming=incoming, ready=reply_received):
                incoming.extend(values)
                if values:
                    ready.set()

            client = RendezqueueClient(
                url=url,
                room="resume",
                hue="cli",
                continuable=True,
                self_xid=cursor.self_xid if cursor else b"",
                peer_xid=cursor.peer_xid if cursor else b"",
                on_data=receive_reply,
                on_error=errors.append,
                poll_interval_ms=500,
            )
            clients.append(client)
            peer.send(reply)
            client.send(payload)
            peer.start()
            client.start()
            if index < 2:
                # Only these cursors are actually resumed by a later command.
                cursor = client.finish(timeout=15)
            else:
                assert reply_received.wait(10), errors
                assert all_inputs_received.wait(10), errors
            assert incoming == [reply] and received == list(payloads[: index + 1])
    finally:
        for client in [*clients, peer]:
            client.stop()
            if client.poll_thread is not None:
                client.poll_thread.join(12)
                assert not client.poll_thread.is_alive()
    assert received == list(payloads) and not errors
    assert received_replies == [[reply] for reply in replies]


def test_wrong_self_in_otherwise_valid_completed_reply_cannot_deliver(transport):
    client, received, completed, errors = transport.client("a")
    peer, _, _, _ = transport.client("b")
    client.send("retained")
    opening(client, peer)
    client._poll()
    before = client._core.stored()
    assert client._core.current_cached
    obj = {
        "room": "room",
        "self": "wrong",
        "next": "peer-next",
        "values": ["must-not-deliver"],
    }
    transport.after = lambda request, original: Response(json.dumps(obj).encode())
    client._poll()
    assert len(errors) == 1 and "self does not match" in str(errors[0])
    assert not received and not completed
    assert client._core.stored() == before and client._core.current_cached
    assert client._core.active is None
