"""Continuation export cannot own its own wait or succeed after a failed cut."""

import threading

import pytest

from rendezqueue.client import RendezqueueClient
from rendezqueue.plyswap import Message


def test_callback_restart_does_not_hide_the_actual_poll_owner(monkeypatch):
    entered, release, done = threading.Event(), threading.Event(), threading.Event()
    errors = []
    client = RendezqueueClient(
        "http://example/", "room", "a", continuable=True, on_error=errors.append
    )
    client._core.peer_xid = b"peer"
    client._core.peer_confirmed = True
    client.send(b"own")

    class Transport:
        def exchange(self, request):
            return Message(
                request.self_xid, next_xid=b"peer-next", values=(b"peer-data",)
            )

    def on_data(values):
        entered.set()
        try:
            assert release.wait(5)
            client.stop()
            client.start()  # Replaces poll_thread, but not this admitted owner.
            with pytest.raises(RuntimeError, match="polling callback"):
                client.finish(timeout=0.1)
        finally:
            client.stop()
            done.set()

    monkeypatch.setattr(client, "_transport", Transport())
    client.on_data = on_data
    client.start()
    owner = client.poll_thread
    assert owner is not None
    try:
        assert entered.wait(5)
        release.set()
        assert done.wait(5)
    finally:
        release.set()
        client.stop()
        owner.join(5)
        replacement = client.poll_thread
        assert replacement is not None
        replacement.join(5)
        assert not owner.is_alive() and not replacement.is_alive()
    assert not errors
    assert not client._finishing  # The callback was rejected before freezing admission.
    assert client._poll_owner is None


def test_timeout_emits_no_continuation_and_prevents_new_admission(monkeypatch):
    client = RendezqueueClient(
        "http://example/", "room", "a", continuable=True, poll_interval_ms=1
    )
    requests = []

    class NoPeer:
        def exchange(self, request):
            requests.append(request)
            return Message(request.self_xid)

    monkeypatch.setattr(client, "_transport", NoPeer())
    client.send(b"still pending")
    client.start()
    try:
        with pytest.raises(TimeoutError, match="no continuation"):
            client.finish(timeout=0.01)
    finally:
        client.stop()
        worker = client.poll_thread
        assert worker is not None
        worker.join(5)
        assert not worker.is_alive()
    before = list(requests)
    client._poll()
    assert requests == before
    assert client.outgoing_queue == [b"still pending"]
    with pytest.raises(RuntimeError, match="closed"):
        client.start()
    with pytest.raises(RuntimeError, match="closed"):
        client.send(b"unsafe new input")
