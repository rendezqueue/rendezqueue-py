from dataclasses import replace
import random

import pytest

from rendezqueue.plyswap import Message, PlySwap
from rendezqueue.plyswap_client import ClientState, MAX_COUNTER, Packet


def prepared(core: ClientState) -> Packet:
    packet = core.send("current")
    assert packet is not None
    return packet


class World:
    def __init__(self):
        self.clients = [ClientState(b"a"), ClientState(b"b")]
        self.server = PlySwap()
        self.inputs = [[], []]
        self.outputs = [[], []]
        self.completions = [[], []]
        self.requests = []
        self.replies = []
        self.history = []

    def enqueue(self, side, value):
        self.inputs[side].append(bytes(value))
        self.clients[side].enqueue(value)

    def send(self, side, mode="current"):
        packet = self.clients[side].send(mode)
        if packet is not None:
            self.requests.append((side, packet))
        return packet

    def process(self, index=0):
        if index < len(self.requests):
            side, packet = self.requests.pop(index)
            reply = self.server.serve(packet.request)
            self.replies.append((side, packet, reply))
            self.history.append((side, packet.request, reply))

    def deliver(self, index=0):
        if index < len(self.replies):
            side, packet, reply = self.replies.pop(index)
            completion = self.clients[side].accept(packet, reply)
            if completion is not None:
                self.outputs[side].extend(completion.values)
                self.completions[side].append(completion)
            return completion

    def rpc(self, side, mode="current"):
        packet = self.send(side, mode)
        assert packet is not None
        self.process(len(self.requests) - 1)
        return self.deliver(len(self.replies) - 1)

    def opening(self):
        self.rpc(0)
        self.rpc(1)
        self.rpc(0)
        assert self.clients[0].peer_xid == self.clients[1].self_xid
        assert self.clients[1].peer_xid == self.clients[0].self_xid

    def safety(self):
        for side in (0, 1):
            assert (
                self.outputs[side] == self.inputs[1 - side][: len(self.outputs[side])]
            )
            missing = self.inputs[side][len(self.outputs[1 - side]) :]
            if missing:
                assert list(self.clients[side].stored())[-len(missing) :] == missing


def test_two_literal_rounds_current_and_previous_references():
    world = World()
    world.opening()
    for generation in range(2):
        world.enqueue(0, b"same")
        world.enqueue(1, b"same")
        assert world.rpc(0) is None
        assert world.rpc(1).values == (b"same",)
        assert world.rpc(0).values == (b"same",)
        assert world.history[-1][1].is_reference()
        for side in (0, 1):
            core = world.clients[side]
            assert core.generation == generation + 1
            assert core.previous is not None
            assert core.previous.values == (b"same",)
            assert core.batch is None and not core.queued
            assert world.rpc(side, "previous") is None
            assert world.history[-1][1].is_reference()
            assert len(world.completions[side]) == generation + 1
        world.safety()
    assert world.outputs == [[b"same", b"same"], [b"same", b"same"]]


def test_data_empty_requires_previous_seal_after_input_stops():
    world = World()
    world.opening()
    world.enqueue(0, b"data")
    assert world.rpc(0) is None
    assert world.rpc(1).values == (b"data",)
    assert world.clients[1].previous is not None
    assert world.clients[1].previous.values == ()
    assert world.clients[1].previous.next_xid
    assert world.rpc(0) is None  # Peer row is still provisional EMPTY.
    assert world.clients[0].generation == 0
    assert world.rpc(1, "previous") is None
    assert not world.history[-1][1].is_reference()
    assert world.rpc(0).values == ()
    assert world.clients[0].generation == 1
    assert world.outputs == [[], [b"data"]]
    assert [len(events) for events in world.completions] == [1, 1]
    world.safety()


@pytest.mark.parametrize("mode", ["current", "previous"])
def test_reference_miss_restores_exact_full_snapshot_and_retains_tail(mode):
    world = World()
    world.opening()
    backing = bytearray(b"original")
    world.enqueue(0, memoryview(backing))
    world.enqueue(1, b"peer")
    world.rpc(0)
    full = world.history[-1][1]
    backing[:] = b"mutated!"
    world.enqueue(0, b"tail")
    if mode == "previous":
        world.rpc(1)
        world.rpc(0)
    core = world.clients[0]
    if mode == "current":
        self_xid = core.self_xid
    else:
        assert core.previous is not None
        self_xid = core.previous.self_xid
    world.server.rows.pop(self_xid)
    rows = tuple(world.server.rows.items())
    assert world.rpc(0, mode) is None
    assert world.history[-1][1].is_reference()
    assert world.history[-1][2] is None
    assert tuple(world.server.rows.items()) == rows
    assert core.queued == [b"tail"]
    # Process a recovery FULL but lose its response: no upload confirmation.
    world.send(0, mode)
    assert world.requests[-1][1].request == full
    world.process()
    world.replies.clear()
    core.timeout()
    world.send(0, mode)
    assert world.requests[-1][1].request == full
    assert full.values == (b"original",)
    assert core.queued == [b"tail"]
    world.process()
    world.deliver()
    world.safety()


def test_frozen_whole_batch_and_mutable_input_ownership():
    world = World()
    world.opening()
    backing = bytearray(b"\x00\xff")
    world.enqueue(0, backing)
    world.enqueue(0, b"")
    packet = world.send(0)
    backing[:] = b"xx"
    world.enqueue(0, b"tail")
    assert packet.request.values == (b"\x00\xff", b"")
    assert world.clients[0].batch == (b"\x00\xff", b"")
    world.process()
    world.deliver()
    assert world.rpc(1).values == (b"\x00\xff", b"")
    world.rpc(1, "previous")
    assert world.rpc(0).values == ()
    assert world.clients[0].queued == [b"tail"]
    assert world.clients[0].previous is not None
    assert world.clients[0].previous.values == (b"\x00\xff", b"")
    world.safety()


def test_old_or_altered_packet_cannot_clear_new_active_or_redeliver():
    core = ClientState(b"namespace")
    old = prepared(core)
    core.timeout()
    active = prepared(core)
    before = core.active
    assert core.accept(old, Message(old.request.self_xid, peer_xid=b"peer")) is None
    assert core.active == before
    altered = replace(active, request=replace(active.request, values=(b"forged",)))
    assert core.accept(altered, None) is None
    assert core.active == before
    assert (
        core.accept(active, Message(active.request.self_xid, peer_xid=b"peer")) is None
    )
    assert core.peer_xid == b"peer"
    assert core.active is None
    assert (
        core.accept(active, Message(active.request.self_xid, peer_xid=b"different"))
        is None
    )
    assert core.peer_xid == b"peer"


@pytest.mark.parametrize(
    "reply",
    [
        Message(b"wrong"),
        Message(b"own", values=(b"unsealed",)),
        Message(b"own", peer_xid=b"peer", next_xid=b"next"),
    ],
)
def test_invalid_reply_does_not_confirm_upload_or_drop_real_input(reply):
    core = ClientState(b"namespace")
    packet = prepared(core)
    core.accept(packet, Message(packet.request.self_xid, peer_xid=b"peer"))
    core.enqueue(b"input")
    packet = prepared(core)
    assert (
        core.accept(
            packet,
            replace(
                reply,
                self_xid=reply.self_xid
                if reply.self_xid == b"wrong"
                else packet.request.self_xid,
            ),
        )
        is None
    )
    assert not core.current_cached
    assert core.stored() == (b"input",)
    assert core.generation == 0


def test_disabled_sends_and_exhaustion_do_not_mutate_input():
    core = ClientState(b"n")
    assert core.send("previous") is None
    packet = prepared(core)
    assert core.send("current") is None
    core.timeout(packet)
    core.peer_xid = b"peer"
    core.enqueue(b"retained")
    core.next_attempt = MAX_COUNTER
    with pytest.raises(OverflowError):
        core.send("current")
    assert core.batch is None and core.queued == [b"retained"]
    core.next_attempt = 0
    core.generation = MAX_COUNTER
    with pytest.raises(OverflowError):
        core.send("current")
    assert core.batch is None and core.queued == [b"retained"]
    assert core.active is None


def test_opening_self_peer_and_completion_own_successor_do_not_advance():
    core = ClientState(b"n")
    packet = prepared(core)
    core.accept(
        packet, Message(packet.request.self_xid, peer_xid=packet.request.self_xid)
    )
    assert not core.peer_xid
    packet = prepared(core)
    core.accept(packet, Message(packet.request.self_xid, peer_xid=b""))
    assert core.peer_xid == b"" and not core.peer_confirmed
    core.enqueue(b"data")
    packet = prepared(core)
    assert packet.request == Message(core.self_xid) and core.batch is None
    core.accept(packet, Message(packet.request.self_xid, peer_xid=b"peer"))
    assert core.peer_xid == b"peer" and core.peer_confirmed
    packet = prepared(core)
    assert packet.request.next_xid and packet.request.values == (b"data",)
    assert (
        core.accept(
            packet, Message(packet.request.self_xid, next_xid=packet.request.next_xid)
        )
        is None
    )
    assert core.generation == 0 and core.stored() == (b"data",)


@pytest.mark.parametrize("seed", range(12))
def test_bounded_fault_traces_preserve_fifo_and_real_retention(seed):
    world = World()
    rng = random.Random(seed)
    for _ in range(700):
        side = rng.randrange(2)
        action = rng.randrange(12)
        if action == 0:
            world.enqueue(side, bytes([rng.randrange(3)]))
        elif action in (1, 2):
            world.send(side, "current" if action == 1 else "previous")
        elif action == 3:
            if world.requests:
                world.process(rng.randrange(len(world.requests)))
        elif action == 4:
            if world.replies:
                world.deliver(rng.randrange(len(world.replies)))
        elif action == 5 and world.requests:
            world.requests.append(rng.choice(world.requests))
        elif action == 6 and world.replies:
            world.replies.append(rng.choice(world.replies))
        elif action == 7 and world.requests:
            world.requests.pop(rng.randrange(len(world.requests)))
        elif action == 8 and world.replies:
            world.replies.pop(rng.randrange(len(world.replies)))
        elif action == 9:
            world.clients[side].timeout()
        elif action == 10:
            world.server.rows.clear()
        else:
            world.server.rows.pop(world.clients[side].self_xid, None)
        world.safety()
    # Faults stop; healthy alternating attempts must flush these bounded inputs.
    world.requests.clear()
    world.replies.clear()
    for core in world.clients:
        core.timeout()
    for _ in range(40):
        for side in (0, 1):
            for mode in ("previous", "current"):
                if mode == "previous" and world.clients[side].previous is None:
                    continue
                world.rpc(side, mode)
                world.safety()
    assert world.outputs == [world.inputs[1], world.inputs[0]]
