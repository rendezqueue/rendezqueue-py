"""Empty XIDs mean absence, never an address, binding, or completion seal."""

import json

import pytest

from rendezqueue.continuation import Continuation
from rendezqueue.plyswap import Message, PlySwap, PlySwapResponse
from rendezqueue.plyswap_client import ClientState
from rendezqueue.plyswap_wire import decode_request, decode_response, encode_request
from plyswap_http_test import post, server as server


@pytest.mark.parametrize("flags", [0, 2])
def test_empty_and_omitted_wire_xids_normalize_to_the_same_packet(flags):
    canonical = encode_request(Message(b"a"), b"r", flags)
    obj = json.loads(canonical)
    obj.update(peer="", next="")
    body = json.dumps(obj).encode()
    envelope = decode_request(body)
    assert envelope.message == Message(self_xid=b"a", peer_xid=b"", next_xid=b"")
    assert decode_response(body) == PlySwapResponse(room=b"r", self_xid=b"a")
    assert encode_request(envelope.message, envelope.room, flags) == canonical
    assert "peer" not in json.loads(canonical) and "next" not in json.loads(canonical)


def test_empty_successor_is_unsealed_and_cannot_erase_a_seal():
    core = PlySwap()
    empty = Message(self_xid=b"a", peer_xid=b"b", next_xid=b"")
    assert core.serve(empty) == Message(b"a")
    assert core.rows[b"a"].next_xid == b""
    sealed = Message(self_xid=b"a", peer_xid=b"b", next_xid=b"a1")
    assert core.serve(sealed) == Message(b"a")
    before = dict(core.rows)
    assert core.serve(empty) == Message(b"a")
    assert core.rows == before
    assert core.serve(Message(b"a", peer_xid=b"b", next_xid=b"a2")) is None
    assert core.rows == before
    assert core.serve(
        Message(b"b", peer_xid=b"a", next_xid=b"b1", values=(b"",))
    ) == Message(b"b", next_xid=b"a1")
    assert core.serve(sealed) == Message(b"a", next_xid=b"b1", values=(b"",))


def test_empty_successor_reply_cannot_complete_or_drop_frozen_input():
    state = ClientState(namespace=b"n", continuable=True, self_xid=b"a", peer_xid=b"b")
    state.enqueue(b"")
    packet = state.send("current")
    assert packet is not None and packet.request.next_xid
    assert packet.request.values == (b"",)
    assert state.accept(packet, Message(self_xid=b"a", next_xid=b"")) is None
    assert state.generation == 0 and state.previous is None
    assert state.stored() == (b"",) and not state.can_handoff()


def test_fresh_sdk_state_uses_empty_input_sentinels_but_nonempty_generated_xid():
    default = ClientState(namespace=b"")
    explicit = ClientState(namespace=b"", self_xid=b"", peer_xid=b"")
    assert default.self_xid == explicit.self_xid and explicit.self_xid
    assert default.peer_xid == explicit.peer_xid == b""
    assert not explicit.can_handoff()
    packet = explicit.send("current")
    assert packet is not None and packet.request == Message(explicit.self_xid)
    with pytest.raises(ValueError):
        Continuation(self_xid=explicit.self_xid, peer_xid=explicit.peer_xid)


def test_http_empty_xids_do_not_bind_or_seal_but_empty_payload_is_data(server):
    def rpc(**fields):
        status, _, body = post(server, json.dumps({"room": "r", **fields}).encode())
        assert status == 200
        return json.loads(body)

    assert rpc(self="a", peer="", next="") == {"room": "r", "self": "a"}
    assert rpc(self="b", peer="", next="") == {"room": "r", "self": "b", "peer": "a"}
    assert rpc(self="a", peer="b", next="a1", values=[""]) == {"room": "r", "self": "a"}
    data = {"room": "r", "self": "b", "next": "a1", "values": [""]}
    assert rpc(self="b", peer="a", next="") == data
    assert rpc(self="a", peer="", next="a1") == {"room": "r", "self": "a"}
    assert rpc(self="b", peer="a", next="b1") == data
    assert rpc(self="a", peer="", next="a1") == {"room": "r", "self": "a", "next": "b1"}


def test_http_rejects_empty_self_before_admitting_or_refreshing_a_row(server):
    assert post(server, b'{"room":"r","self":"a"}')[0] == 200
    state = server.plyswap._rooms[b"r"]
    before = dict(state.core.rows), dict(state.refreshed)
    assert post(server, b'{"room":"r","self":""}')[0] == 400
    assert (state.core.rows, state.refreshed) == before
    assert server.plyswap.room_count == 1
