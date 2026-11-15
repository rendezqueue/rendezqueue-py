"""Main API is PlySwap; original naive client tests live in tryswap_client_test.py."""

from contextlib import AbstractContextManager
from email.message import Message as Headers
import json
from unittest.mock import Mock

import pytest

from rendezqueue import RendezqueueClient
from rendezqueue.plyswap import Message, PlySwapResponse
from rendezqueue.plyswap_cache import PlySwapCache
from rendezqueue.plyswap_wire import (
    REJECTION,
    REQUEST_LIMIT,
    RESPONSE_LIMIT,
    decode_request,
    encode_response,
)


class Response(AbstractContextManager):
    def __init__(self, body, status=200, content_type="application/json"):
        self.body = body
        self.status = status
        self.headers = Headers()
        if content_type is not None:
            self.headers["Content-Type"] = content_type
        self.headers["Content-Length"] = str(len(body))
        self.closed = False

    def read(self, limit):
        return self.body[:limit]

    def __exit__(self, *args):
        self.closed = True
        return False


class Transport:
    def __init__(self, monkeypatch):
        self.monkeypatch = monkeypatch
        self.server = PlySwapCache(clock=lambda: 0)
        self.requests = []
        self.after = lambda request, response: response

    def client(self, hue):
        received, completed, errors = [], [], []
        client = RendezqueueClient(
            "http://unused/plyswap",
            "room",
            hue,
            received.append,
            errors.append,
            on_complete=lambda: completed.append(True),
        )
        self.monkeypatch.setattr(client._transport._opener, "open", self.open)
        return client, received, completed, errors

    def open(self, request, **kwargs):
        assert kwargs["timeout"] == 10
        assert request.full_url == "http://unused/plyswap"
        assert request.get_header("Content-type") == "application/json"
        raw = json.loads(request.data)
        assert raw["b64"] == 7 and "offset" not in raw
        envelope = decode_request(request.data)
        assert envelope.room == b"room"
        self.requests.append(envelope.message)
        reply = self.server.serve(envelope.room, envelope.message)
        response = (
            Response(REJECTION, 409)
            if reply is None
            else Response(encode_response(reply, envelope.b64))
        )
        return self.after(envelope.message, response)


@pytest.fixture
def transport(monkeypatch):
    return Transport(monkeypatch)


def opening(a, b):
    a._poll()
    b._poll()
    a._poll()
    assert a._core.peer_xid == b._core.self_xid
    assert b._core.peer_xid == a._core.self_xid


def finish_round(a, b):
    for _ in range(8):
        a._poll()
        b._poll()


def test_main_import_constructor_and_send_ownership(transport):
    client, received, completed, errors = transport.client("hue")
    assert client.url == "http://unused/plyswap" and client.room == "room"
    assert client.poll_interval_ms == 2000
    backing = bytearray(b"\x00\xff")
    client.send(backing)
    client.send(memoryview(backing))
    client.send("雪")
    client.send(b"")
    backing[:] = b"xx"
    assert client.outgoing_queue == [b"\x00\xff", b"\x00\xff", "雪".encode(), b""]
    client._poll()
    assert transport.requests[-1] == Message(
        client._core.self_xid
    )  # OPEN never uploads.
    assert not received and not completed and not errors
    with pytest.raises(AttributeError):
        client.url = "http://other/plyswap"
    with pytest.raises(AttributeError):
        client.room = "other"


def test_main_data_callbacks_and_local_completion_once_per_generation(transport):
    a, ar, ac, ae = transport.client("a")
    b, br, bc, be = transport.client("b")
    a.send(b"\x00\xff")
    a.send(b"")
    b.send("peer")
    finish_round(a, b)
    assert ar == [[b"peer"]] and br == [[b"\x00\xff", b""]]
    assert ac == bc == [True]
    assert not ae and not be
    assert a.outgoing_queue == b.outgoing_queue == []
    a.send("tail")
    finish_round(a, b)
    assert ar == [[b"peer"], []] and br == [[b"\x00\xff", b""], [b"tail"]]
    assert ac == bc == [True, True]
    assert not ae and not be


def test_callbacks_are_committed_in_order_and_fail_independently(transport):
    a, ar, ac, errors = transport.client("a")
    b, _, _, _ = transport.client("b")
    order = []

    def data(values):
        order.append("data")
        ar.append(values)
        a.send("tail")  # Must run outside state mutex.
        a._poll()  # But nested I/O is not admitted inside callbacks.
        raise RuntimeError("data observer")

    def complete():
        order.append("complete")
        ac.append(True)
        raise RuntimeError("completion observer")

    a.on_data, a.on_complete = data, complete
    a.send("a")
    b.send("b")
    opening(a, b)
    b._poll()
    before = len(transport.requests)
    a._poll()
    assert len(transport.requests) == before + 1
    assert order == ["data", "complete"]
    assert ar == [[b"b"]] and ac == [True]
    assert [str(error) for error in errors] == ["data observer", "completion observer"]
    assert a.outgoing_queue == [b"tail"] and not a.is_polling
    assert a._core.generation == 1


def test_no_observers_and_positional_poll_interval(transport):
    a, _, _, errors = transport.client("a")
    b, _, _, _ = transport.client("b")
    a.on_data = a.on_complete = None
    a.send("a")
    b.send("b")
    finish_round(a, b)
    assert a._core.generation == 1 and not errors
    client = RendezqueueClient("http://unused/plyswap", "key", "hue", None, None, 123)
    assert client.poll_interval_ms == 123 and client.on_complete is None


def test_previous_attempts_continue_without_new_input_and_cannot_redeliver(transport):
    a, ar, ac, ae = transport.client("a")
    b, br, bc, be = transport.client("b")
    a.send("a")
    opening(a, b)
    a._poll()  # DATA pending.
    b._poll()  # EMPTY current receives DATA, but its row is still unsealed.
    assert br == [[b"a"]] and not ar
    a._poll()  # Reference remains pending.
    assert not ac
    previous = b._core.previous
    b._poll()  # Scheduler chooses retained previous EMPTY even without new input.
    assert transport.requests[-1] == previous
    a._poll()
    assert ar == [[]] and ac == bc == [True]
    finish_round(a, b)
    assert ar == [[]] and br == [[b"a"]]
    assert not ae and not be


def test_reference_rejection_restores_frozen_batch_then_delivers_queued_tail(transport):
    a, ar, ac, ae = transport.client("a")
    b, br, bc, be = transport.client("b")
    a.send("frozen")
    b.send("peer")
    opening(a, b)
    a._poll()
    full = transport.requests[-1]
    a.send("tail")
    transport.server = PlySwapCache(clock=lambda: 0)
    a._poll()
    assert transport.requests[-1].is_reference()
    assert not a._core.current_cached and not ae and not be
    a._poll()
    assert transport.requests[-1] == full
    assert a.outgoing_queue == [b"frozen", b"tail"]
    finish_round(a, b)
    assert ar == [[b"peer"], []]
    assert br == [[b"frozen"], [b"tail"]]
    assert ac == bc == [True, True]
    assert not ae and not be
    assert a.outgoing_queue == b.outgoing_queue == []
    # More healthy retries cannot create duplicate callbacks or new completions.
    finish_round(a, b)
    assert ar == [[b"peer"], []]
    assert br == [[b"frozen"], [b"tail"]]
    assert ac == bc == [True, True]
    assert not ae and not be
    assert a.outgoing_queue == b.outgoing_queue == []


@pytest.mark.parametrize(
    "status,body,content_type",
    [
        (404, b"missing", "application/json"),
        (503, b"unavailable", "application/json"),
        (302, b"redirect", "application/json"),
        (409, b"proxy conflict", "application/json"),
        (200, b"not JSON", "application/json"),
        (200, b'{"room":"room","self":"wrong"}', "application/json"),
        (200, b'{"self":"wrong"}', "text/plain"),
        (200, b'{"self":"wrong"}', None),
        (200, b" " * (RESPONSE_LIMIT + 1), "application/json"),
    ],
    ids=[
        "not-found",
        "unavailable",
        "redirect",
        "non-protocol-conflict",
        "invalid-json",
        "wrong-self",
        "wrong-content-type",
        "missing-content-type",
        "oversized-response",
    ],
)
def test_uncertain_errors_retain_hint_and_whole_snapshot(
    transport, status, body, content_type
):
    a, ar, ac, errors = transport.client("a")
    b, _, _, _ = transport.client("b")
    a.send("retained")
    opening(a, b)
    a._poll()  # Successful FULL confirms a reference hint.
    assert a._core.current_cached
    stored = a._core.stored()
    if status == 200 and content_type != "application/json":
        # Otherwise-valid payload/self ID: failure must come from the media gate.
        body = encode_response(PlySwapResponse(b"room", a._core.self_xid))
    response = Response(body, status, content_type)
    transport.after = lambda request, reply: response
    a._poll()
    assert transport.requests[-1].is_reference()
    assert a._core.stored() == stored and a._core.current_cached
    assert a._core.active is None and not a.is_polling
    assert not ar and not ac and len(errors) == 1
    assert response.closed
    transport.after = lambda request, reply: reply
    a._poll()
    assert transport.requests[-1].is_reference()  # Not a fabricated cache MISS.


def test_lost_full_response_never_confirms_upload(transport):
    a, _, _, errors = transport.client("a")
    b, _, _, _ = transport.client("b")
    a.send("snapshot")
    opening(a, b)

    def lose(request, reply):
        transport.after = lambda request, response: response
        raise OSError("response lost after processing")

    transport.after = lose
    a._poll()
    full = transport.requests[-1]
    assert full.values == (b"snapshot",) and not a._core.current_cached
    a._poll()
    assert transport.requests[-1] == full
    assert len(errors) == 1


def test_oversized_batch_is_retained_and_never_sent(transport):
    a, _, _, errors = transport.client("a")
    b, _, _, _ = transport.client("b")
    opening(a, b)
    a.send(b"\xff" * REQUEST_LIMIT)
    before = len(transport.requests)
    a._poll()
    assert len(transport.requests) == before
    assert len(a._core.batch[0]) == REQUEST_LIMIT
    assert len(a.outgoing_queue[0]) == REQUEST_LIMIT
    assert a._core.active is None and not a.is_polling
    assert "not sent" in str(errors[0])


@pytest.mark.parametrize("value", [0, -1, True, float("nan"), float("inf")])
def test_invalid_poll_interval_is_explicit(value):
    with pytest.raises(ValueError, match="positive"):
        RendezqueueClient("http://unused/plyswap", "key", "hue", poll_interval_ms=value)


def test_error_observer_failure_does_not_strand_guard(transport):
    a, _, _, _ = transport.client("a")
    observer = Mock(side_effect=RuntimeError("observer failure"))
    a.on_error = observer
    transport.after = lambda request, reply: Response(b"bad", 503)
    a._poll()
    assert observer.call_count == 1
    assert not a.is_polling and a._core.active is None
