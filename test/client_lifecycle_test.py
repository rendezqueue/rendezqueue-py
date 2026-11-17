"""Event-controlled ownership and pacing tests for the main PlySwap scheduler."""

import threading
import time

import pytest

from client_test import Response, Transport, opening


class RunningTransport(Transport):
    def __init__(self, monkeypatch):
        super().__init__(monkeypatch)
        self.clients = []
        self.threads = []
        self.releases = []

    def client(self, hue):
        result = super().client(hue)
        client = result[0]
        client.poll_interval_ms = 60_000
        self.clients.append(client)
        idle = threading.Event()
        wait = client._condition.wait

        def observe_wait(timeout=None):
            if timeout is not None:
                idle.set()
            return wait(timeout)

        client._condition.wait = observe_wait
        client.test_idle = idle
        poll_loop = client._poll_loop

        def owned_loop(owner):
            self.threads.append(threading.current_thread())
            poll_loop(owner)

        client._poll_loop = owned_loop
        return result

    def start(self, client):
        client.test_idle.clear()
        client.start()

    def idle(self, client):
        assert client.test_idle.wait(5), "scheduler did not reach its paced wait"
        with client.lock:
            assert not client.is_polling and client._run is not None
            assert client._run.next_poll_time > time.monotonic()

    def tick(self, client):
        with client._condition:
            client.test_idle.clear()
            client._run.next_poll_time = time.monotonic()
            client._condition.notify_all()
        self.idle(client)

    def gate(self):
        release = threading.Event()
        self.releases.append(release)
        return release

    def fork(self, fn):
        worker = threading.Thread(target=fn, daemon=True)
        self.threads.append(worker)
        worker.start()
        return worker

    def close(self):
        for client in self.clients:
            client.stop()
        for release in self.releases:
            release.set()
        for worker in self.threads:
            worker.join(5)
            assert not worker.is_alive(), "poll/test worker leaked"


@pytest.fixture
def running(monkeypatch):
    transport = RunningTransport(monkeypatch)
    try:
        yield transport
    finally:
        transport.close()


def ready_completion(f):
    a = f.client("a")
    b = f.client("b")
    a[0].send("a")
    b[0].send("b")
    opening(a[0], b[0])
    b[0]._poll()  # B's DATA is ready; A's next current attempt completes.
    return a, b


@pytest.mark.parametrize("callback", ["data", "complete"])
@pytest.mark.parametrize("action", ["stop", "restart", "throw"])
def test_callback_reentrancy_keeps_one_guard_and_retained_state(
    running, callback, action
):
    f = running
    (a, ar, ac, errors), _ = ready_completion(f)
    namespace = a._core.namespace

    def react():
        a.send("tail")
        a._poll()  # Nested admission must be rejected without deadlock.
        if action in ("stop", "restart"):
            a.stop()
        if action == "restart":
            f.start(a)
        elif action == "throw":
            raise RuntimeError("observer failed")

    if callback == "data":

        def data(values):
            ar.append(values)
            react()

        a.on_data = data
    else:

        def complete():
            ac.append(True)
            react()

        a.on_complete = complete
    before = len(f.requests)
    f.start(a)
    old_thread = a.poll_thread
    if action == "stop":
        old_thread.join(5)
        assert not old_thread.is_alive()
    else:
        f.idle(a)
    assert len(f.requests) == before + 1
    assert ar == [[b"b"]] and ac == [True]
    assert len(errors) == (1 if action == "throw" else 0)
    assert a.outgoing_queue == [b"tail"]
    assert a._core.generation == 1 and a._core.namespace == namespace
    assert not a.is_polling


@pytest.mark.parametrize("stage", ["read", "decode"])
def test_body_decode_guard_survives_competing_polls_and_stop_restart(
    running, monkeypatch, stage
):
    f = running
    (a, ar, ac, errors), _ = ready_completion(f)
    entered, release = threading.Event(), f.gate()

    def block(fn, *args):
        entered.set()
        assert release.wait(5)
        return fn(*args)

    if stage == "read":

        def held(request, response):
            original = response.read
            response.read = lambda limit: block(original, limit)
            return response

        f.after = held
    else:
        from rendezqueue import plyswap_transport

        decode = plyswap_transport.decode_response
        monkeypatch.setattr(
            plyswap_transport, "decode_response", lambda data: block(decode, data)
        )
    before = len(f.requests)
    f.start(a)
    assert entered.wait(5)
    barrier = threading.Barrier(9)

    def compete():
        barrier.wait(5)
        a._poll()

    workers = [f.fork(compete) for _ in range(8)]
    barrier.wait(5)
    for worker in workers:
        worker.join(5)
        assert not worker.is_alive()
    a.send("tail")
    a.stop()
    f.start(a)
    assert len(f.requests) == before + 1
    release.set()
    f.idle(a)
    assert len(f.requests) == before + 1
    assert ar == [[b"b"]] and ac == [True] and not errors
    assert a.outgoing_queue == [b"tail"]
    f.after = lambda request, response: response
    f.tick(a)
    assert any(request.values == (b"tail",) for request in f.requests[before + 1 :])
    assert ar == [[b"b"]] and ac == [True] and not errors


def test_discovery_receipt_and_callback_reply_drain_before_idle(running):
    f = running
    a, received, _, errors = f.client("a")
    b, _, completed, peer_errors = f.client("b")
    b._poll()
    a._poll()
    a.send(b"request")
    a._poll()
    b.on_data = lambda values: b.send(b"response")
    before = len(f.requests)
    f.start(b)
    f.idle(b)
    assert any(request.values == (b"response",) for request in f.requests[before:])
    for _ in range(4):
        a._poll()
    assert [value for batch in received for value in batch] == [b"response"]
    assert completed == [True] and not errors and not peer_errors


@pytest.mark.parametrize("during_io", [False, True])
def test_new_input_wakes_idle_or_finishing_empty_rpc(running, during_io):
    f = running
    a, _, _, errors = f.client("a")
    b, _, _, _ = f.client("b")
    opening(a, b)
    entered, release = threading.Event(), f.gate()

    def held(request, response):
        entered.set()
        assert release.wait(5)
        return response

    if during_io:
        f.after = held
    f.start(a)
    if during_io:
        assert entered.wait(5)
    else:
        f.idle(a)
        a.test_idle.clear()
    before = len(f.requests)
    a.send(b"fresh input")
    release.set()
    f.idle(a)
    assert any(p.values == (b"fresh input",) for p in f.requests[before:])
    assert not errors
    next_poll_time = a._run.next_poll_time
    a.send(b"behind frozen batch")
    assert a._run.next_poll_time == next_poll_time
    assert a.outgoing_queue == [b"fresh input", b"behind frozen batch"]


def test_new_input_cannot_shorten_error_backoff(running):
    f = running
    a, _, _, errors = f.client("a")
    b, _, _, _ = f.client("b")
    opening(a, b)
    f.after = lambda request, response: Response(b"unavailable", 503)
    f.start(a)
    f.idle(a)
    next_poll_time = a._run.next_poll_time
    before = len(f.requests)
    a.send(b"fresh input")
    assert a._run.next_poll_time == next_poll_time
    assert a._core.batch is None and len(f.requests) == before
    f.after = lambda request, response: response
    f.tick(a)
    assert any(p.values == (b"fresh input",) for p in f.requests[before:])
    assert len(errors) == 1


def test_reference_repair_precedes_input_queued_during_io(running):
    from rendezqueue.plyswap_cache import PlySwapCache

    f = running
    (a, _, _, errors), _ = ready_completion(f)
    f.start(a)
    f.idle(a)
    f.server = PlySwapCache(clock=lambda: 0)
    entered, release = threading.Event(), f.gate()

    def held(request, response):
        if request.is_reference():
            entered.set()
            assert release.wait(5)
        return response

    f.after = held
    before = len(f.requests)
    worker = f.fork(lambda: f.tick(a))
    assert entered.wait(5)
    a.send(b"fresh input")
    release.set()
    worker.join(5)
    assert not worker.is_alive()
    f.idle(a)
    requests = f.requests[before:]
    assert requests[0].is_reference()
    assert requests[1].self_xid == requests[0].self_xid
    assert requests[1].values == (b"a",)
    assert requests[2].values == (b"fresh input",)
    assert not errors


def test_full_completion_publishes_successor_without_resealing_predecessor(running):
    f = running
    (a, received, completed, errors), _ = ready_completion(f)
    predecessor = a._core.self_xid

    def respond(values):
        received.append(values)
        a.send(b"tail")

    a.on_data = respond
    before = len(f.requests)
    f.start(a)
    f.idle(a)
    successor = f.requests[before + 1]
    assert successor.self_xid != predecessor
    assert successor.values == (b"tail",)
    assert received == [[b"b"]] and completed == [True] and not errors


def test_stop_during_owned_io_still_accepts_valid_receipt(running):
    f = running
    (a, ar, ac, errors), _ = ready_completion(f)
    entered, release = threading.Event(), f.gate()

    def held(request, response):
        entered.set()
        assert release.wait(5)
        return response

    f.after = held
    f.start(a)
    assert entered.wait(5)
    a.stop()
    release.set()
    a.poll_thread.join(5)
    assert not a.poll_thread.is_alive()
    assert ar == [[b"b"]] and ac == [True] and not errors
    assert a.is_stopped and not a.is_polling


@pytest.mark.parametrize("mode", ["waiting", "http", "malformed"])
def test_waiting_and_errors_are_paced_not_progress_chained(running, mode):
    f = running
    a, ar, ac, errors = f.client("a")
    if mode == "http":
        f.after = lambda request, response: Response(b"unavailable", 503)
    elif mode == "malformed":
        f.after = lambda request, response: Response(b'{"bad":true}')
    f.start(a)
    a.start()  # Idempotent.
    f.idle(a)
    assert len(f.requests) == 1
    a.send("does not spin/wake")
    assert len(f.requests) == 1
    f.tick(a)
    assert len(f.requests) == 2
    assert not ar and not ac
    assert len(errors) == (0 if mode == "waiting" else 2)


def test_current_previous_alternate_even_with_continuous_input_and_errors(running):
    f = running
    (a, _, _, errors), _ = ready_completion(f)
    f.start(a)
    f.idle(a)
    assert a._core.previous is not None
    old_self_xid = a._core.previous.self_xid
    new_self_xid = a._core.self_xid
    f.after = lambda request, response: Response(b"unavailable", 503)
    a.test_idle.clear()
    a.send("first tail")
    f.idle(a)
    assert f.requests[-1].self_xid == new_self_xid
    observed = []
    for _ in range(6):
        a.send("tail")
        f.tick(a)
        observed.append(f.requests[-1].self_xid)
    assert observed == [old_self_xid, new_self_xid] * 3
    assert len(errors) == 7


def test_error_observer_restart_and_exception_do_not_spin(running):
    f = running
    a, _, _, _ = f.client("a")
    errors = []
    f.after = lambda request, response: Response(b"unavailable", 503)

    def error_observer(error):
        errors.append(error)
        a.send("queued")
        a.stop()
        f.start(a)
        raise RuntimeError("error observer failed")

    a.on_error = error_observer
    f.start(a)
    f.idle(a)
    assert len(f.requests) == len(errors) == 1
    assert a.outgoing_queue == [b"queued"] and not a.is_polling


def test_stop_interrupts_idle_wait_and_restart_preserves_namespace(running):
    f = running
    a, _, _, _ = f.client("a")
    namespace = a._core.namespace
    f.start(a)
    f.idle(a)
    thread = a.poll_thread
    a.stop()
    thread.join(5)
    assert not thread.is_alive()
    f.start(a)
    f.idle(a)
    assert a.poll_thread is not thread
    assert a._core.namespace == namespace
    assert len(f.requests) == 2
