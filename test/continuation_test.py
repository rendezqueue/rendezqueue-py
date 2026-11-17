"""Certified process handoff, not a checkpoint for an interrupted command."""

import random
import threading

import pytest

from rendezqueue.client import RendezqueueClient
from rendezqueue.continuation import Continuation, decode_id, encode_id
from rendezqueue.plyswap import Message, PlySwap
from rendezqueue.plyswap_client import ClientState


def step(core, room, mode="current"):
    packet = core.send(mode)
    assert packet is not None
    return core.accept(packet, room.serve(packet.request))


def test_barrier_requires_actual_peer_progress_and_retains_old_data():
    room = PlySwap()
    a = ClientState(b"a", continuable=True)
    b = ClientState(b"b")
    a.enqueue(b"a-data")
    b.enqueue(b"b-data")
    step(a, room)
    step(b, room)
    step(a, room)
    step(b, room)  # B publishes DATA but has not received A's DATA.
    completion = step(a, room)
    assert completion.values == (b"b-data",)
    previous = a.previous
    assert not a.can_handoff()
    step(a, room)  # OPEN(a1) cannot confirm the nonexistent b1.
    assert not room.rows[a.self_xid].peer_xid
    assert not a.can_handoff() and a.previous == previous
    # Cache loss before B gets the data: A must retain and FULL-retry a0.
    room = PlySwap()
    step(a, room, "previous")  # Missing reference.
    assert not a.previous_cached
    step(a, room, "previous")
    step(b, room)  # Missing reference.
    completion = step(b, room)
    assert completion.values == (b"a-data",)
    step(b, room)  # Only now may B advertise b1.
    step(a, room)
    assert a.can_handoff() and a.previous == previous
    assert not room.rows[a.self_xid].values and not room.rows[a.self_xid].next_xid


def test_wrong_open_peer_cannot_certify_or_rebind_a_successor():
    a = ClientState(b"a", continuable=True, self_xid=b"continued", peer_xid=b"peer")
    a.peer_confirmed = False
    packet = a.send("current")
    assert packet is not None
    assert packet.request == Message(b"continued")
    a.enqueue(b"tail")
    a.accept(packet, Message(b"continued", peer_xid=b"stale"))
    assert a.peer_xid == b"peer" and not a.can_handoff()
    assert a.batch is None and a.queued == [b"tail"]


def test_successor_full_pipelines_without_certifying_handoff_or_losing_predecessor():
    room = PlySwap()
    a = ClientState(b"a", continuable=True)
    b = ClientState(b"b")
    a.enqueue(b"first")
    b.enqueue(b"initial reply")
    step(a, room)
    step(b, room)
    step(a, room)
    step(b, room)
    completion = step(a, room)
    assert completion is not None and completion.values == (b"initial reply",)
    assert not a.peer_confirmed
    assert b.generation == 0  # B has not received the first input yet.
    a.enqueue(b"second")
    packet = a.send("current")
    assert packet is not None and packet.request.values == (b"second",)
    assert packet.request.peer_xid == a.peer_xid
    a.accept(packet, room.serve(packet.request))
    assert a.previous is not None and a.previous.values == (b"first",)
    assert not a.can_handoff()
    # Our own binding can be echoed, but it is not a certified continuation.
    assert room.serve(Message(a.self_xid)) == Message(a.self_xid, peer_xid=a.peer_xid)
    assert not a.can_handoff()

    room = PlySwap()
    received = []
    replies = [b"initial reply"]
    for _ in range(100):
        for core in (a, b):
            for mode in ("previous", "current"):
                if mode == "previous" and core.previous is None:
                    continue
                result = step(core, room, mode)
                if result is not None:
                    if core is b:
                        received.extend(result.values)
                        for value in result.values:
                            b.enqueue(b"reply:" + value)
                    else:
                        replies.extend(result.values)
        missing = [b"first", b"second"][len(received) :]
        if missing:
            assert list(a.stored())[-len(missing) :] == missing
        if (
            received == [b"first", b"second"]
            and replies == [b"initial reply", b"reply:first", b"reply:second"]
            and a.can_handoff()
        ):
            break
    else:
        raise AssertionError("pipelined input did not recover after cache loss")
    assert received == [b"first", b"second"]
    assert replies == [b"initial reply", b"reply:first", b"reply:second"]
    assert a.can_handoff()


@pytest.mark.parametrize("seed", range(40))
def test_fresh_objects_keep_exact_data_across_cache_loss_and_delayed_open(seed):
    rng = random.Random(seed)
    room = PlySwap()
    host = ClientState(b"host")
    command = ClientState(b"first", continuable=True)
    old_open = Message(command.self_xid)
    commands = [b"JOIN", bytes(range(256)), b""]
    inputs, outputs = [], []
    phase = 0
    command.enqueue(commands[phase])
    for _ in range(4000):
        if rng.random() < 0.04:
            room = PlySwap()
        is_command = bool(rng.randrange(2))
        core = command if is_command else host
        mode = "previous" if core.previous and rng.random() < 0.5 else "current"
        packet = core.send(mode)
        assert packet is not None
        reply = room.serve(packet.request)
        if rng.random() < 0.04:
            core.timeout(packet)  # Reply lost after the request took effect.
            continue
        result = core.accept(packet, reply)
        if result is not None:
            if is_command:
                outputs.extend(result.values)
            else:
                inputs.extend(result.values)
                for value in result.values:
                    host.enqueue(b"reply:" + value)
        if command.can_handoff() and len(outputs) == phase + 1:
            assert inputs == commands[: phase + 1]
            assert outputs == [b"reply:" + x for x in commands[: phase + 1]]
            assert bool(command.peer_xid)
            cursor = Continuation(command.self_xid, command.peer_xid)
            phase += 1
            if phase == len(commands):
                break
            # Recreate an expired, delayed predecessor OPEN after a cache loss.
            room = PlySwap()
            room.serve(old_open)
            command = ClientState(
                b"new" + bytes([phase]),
                continuable=True,
                self_xid=cursor.self_xid,
                peer_xid=cursor.peer_xid,
            )
            command.enqueue(commands[phase])
    else:
        raise AssertionError(f"handoff stalled: seed={seed}, phase={phase}")
    assert inputs == commands
    assert outputs == [b"reply:" + x for x in commands]


def test_resume_bypasses_newest_stale_open_instead_of_rediscovering():
    room = PlySwap()
    room.serve(Message(b"peer-next", peer_xid=b"own-next"))
    room.serve(Message(b"expired-predecessor"))
    assert room.serve(Message(b"own-next")) == Message(
        b"own-next", peer_xid=b"expired-predecessor"
    )
    resumed = ClientState(
        b"fresh", continuable=True, self_xid=b"own-next", peer_xid=b"peer-next"
    )
    resumed.enqueue(b"next-command")
    packet = resumed.send("current")
    assert packet is not None
    assert packet.request.peer_xid == b"peer-next"
    assert packet.request.values == (b"next-command",)
    assert room.serve(packet.request) is not None


def test_two_continuable_peers_can_both_advertise_before_binding():
    room = PlySwap()
    a = ClientState(b"a", continuable=True)
    b = ClientState(b"b", continuable=True)
    a.enqueue(b"a")
    b.enqueue(b"b")
    received = [[], []]
    for tick in range(100):
        for index, core in enumerate((a, b)):
            result = step(
                core, room, "previous" if tick % 2 and core.previous else "current"
            )
            if result:
                received[index].extend(result.values)
        if a.can_handoff() and b.can_handoff():
            break
    else:
        raise AssertionError("mutual successor confirmation stalled")
    assert received == [[b"b"], [b"a"]]


def test_finish_waits_for_callbacks_then_permanently_closes_admission(monkeypatch):
    entered, release, done = threading.Event(), threading.Event(), threading.Event()
    errors, result = [], []
    client = RendezqueueClient(
        "http://example/",
        "room",
        "a",
        continuable=True,
        poll_interval_ms=1,
        on_error=errors.append,
    )
    client._core.peer_xid = b"peer0"
    client._core.peer_confirmed = True
    client.send(b"own")

    def data(values):
        assert values == [b"peer"]
        entered.set()
        assert release.wait(5)

    class Transport:
        def exchange(self, request):
            if (
                request == Message(client._core.self_xid)
                and client._core.peer_xid == b"peer1"
            ):
                return Message(request.self_xid, peer_xid=b"peer1")
            if client._core.generation == 0:
                return Message(request.self_xid, next_xid=b"peer1", values=(b"peer",))
            return Message(request.self_xid)

    monkeypatch.setattr(client, "_transport", Transport())
    client.on_data = data
    client.start()
    assert entered.wait(5)

    def finish():
        try:
            result.append(client.finish(5))
        except Exception as error:
            errors.append(error)
        finally:
            done.set()

    finishing = threading.Event()
    original_wait = client._condition.wait

    def observe_wait(timeout=None):
        if client._finishing:
            finishing.set()
        return original_wait(timeout)

    monkeypatch.setattr(client._condition, "wait", observe_wait)
    worker = threading.Thread(target=finish)
    try:
        worker.start()
        assert finishing.wait(5)
        with pytest.raises(RuntimeError, match="closed"):
            client.send(b"during-owned-callback")
        with client._condition:
            assert not done.is_set() and client.is_polling
        release.set()
        assert done.wait(5)
        assert not errors and len(result) == 1
        assert result[0].peer_xid == b"peer1"
        assert client.is_stopped and not client.is_polling
        with pytest.raises(RuntimeError, match="closed"):
            client.send(b"late")
        with pytest.raises(RuntimeError, match="closed"):
            client.start()
    finally:
        release.set()
        client.stop()
        worker.join(5)
        client.poll_thread.join(5)
        assert not worker.is_alive() and not client.poll_thread.is_alive()


def test_rejected_used_continuation_stops_promptly_without_a_new_cursor(monkeypatch):
    client = RendezqueueClient(
        "http://example/",
        "room",
        "a",
        continuable=True,
        self_xid=b"used",
        peer_xid=b"peer",
        on_error=lambda error: None,
    )

    room = PlySwap()
    assert room.serve(
        Message(
            b"used", peer_xid=b"peer", next_xid=b"original-next", values=(b"original",)
        )
    ) == Message(b"used")

    class Rejected:
        def exchange(self, request):
            return room.serve(request)

    monkeypatch.setattr(client, "_transport", Rejected())
    client.send(b"different")
    client._poll()
    assert client.failure is not None and client.is_stopped
    with pytest.raises(RuntimeError, match="already be used"):
        client.finish()


def test_continuation_codec_and_constructor_reject_partial_or_bad_inputs():
    value = bytes(range(256))
    assert decode_id(encode_id(value)) == value
    assert Continuation(b"a", b"b").arguments() == "--self YQ --peer Yg"
    for bad in ("", "a", "YQ==", "YR", "a+", "é"):
        with pytest.raises(ValueError):
            decode_id(bad)
    for kwargs in (
        {"self_xid": b"a"},
        {"peer_xid": b"b"},
        {"self_xid": b"a", "peer_xid": b"a"},
    ):
        with pytest.raises(ValueError):
            ClientState(b"namespace", continuable=True, **kwargs)
