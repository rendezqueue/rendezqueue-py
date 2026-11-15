"""Room-keyed protocol scenarios and clock-controlled cache traces."""

import json
from pathlib import Path

import pytest
import sxpb

from rendezqueue.plyswap import Message, PlySwap, PlySwapResponse, Record
from rendezqueue.plyswap_wire import (
    query_from_target,
    decode_request,
    decode_response,
)
from rendezqueue.plyswap_cache import PlySwapCache

FIXTURES = Path(__file__).parent / "plyswap"


@pytest.mark.parametrize("path", sorted((FIXTURES / "scenario").glob("*.sxpb")))
def test_protocol_scenarios(path):
    cache = PlySwapCache(clock=lambda: 0)
    steps = sxpb.loads(path.read_text())
    assert isinstance(steps, list) and steps
    for index, step in enumerate(steps):
        assert set(step) <= {"req", "res", "reject"}
        envelope = decode_request(json.dumps(step["req"]).encode())
        state = cache._rooms.get(envelope.room)
        before = tuple(state.core.rows.items()) if state else ()
        result = cache.serve(envelope.room, envelope.message)
        if step.get("reject"):
            assert result is None, (path, index)
            state = cache._rooms.get(envelope.room)
            assert (tuple(state.core.rows.items()) if state else ()) == before, (
                path,
                index,
            )
        else:
            assert result == decode_response(json.dumps(step["res"]).encode()), (
                path,
                index,
            )


@pytest.mark.parametrize("path", sorted((FIXTURES / "cache").glob("*.sxpb")))
def test_cache_scenarios(path):
    fixture = sxpb.loads(path.read_text())
    assert isinstance(fixture, dict)
    now = 0
    # Keep fixture times in integer milliseconds to test exact boundaries.
    cache = PlySwapCache(ttl=fixture["ttl_ms"], clock=lambda: now)
    for index, step in enumerate(fixture["steps"]):
        now = step["at_ms"]
        if step.get("sweep"):
            cache.sweep()
        else:
            envelope = decode_request(json.dumps(step["req"]).encode())
            result = cache.serve(envelope.room, envelope.message)
            if step.get("reject"):
                assert result is None, (path, index)
            else:
                assert result == decode_response(json.dumps(step["res"]).encode()), (
                    path,
                    index,
                )
            state = cache._rooms.get(envelope.room)
            assert (list(reversed(state.core.rows)) if state else []) == [
                self_xid.encode("latin1") for self_xid in step["rows"]
            ], (path, index)
        assert cache.room_count == step["room_count"], (path, index)


def test_scenario_packets_are_explicit_and_ordered():
    order = ("b64", "room", "self", "peer", "next", "values")
    for path in FIXTURES.rglob("*.sxpb"):
        source = path.read_text()
        fixture = sxpb.loads(source)
        steps = fixture["steps"] if isinstance(fixture, dict) else fixture
        assert isinstance(steps, list)
        for step in steps:
            if "req" in step:
                assert "room" in step["req"] and "room" not in step
            if "res" in step:
                assert "room" in step["res"]
            for packet in (step[name] for name in ("req", "res") if name in step):
                assert list(packet) == [name for name in order if name in packet], path
        for line in source.splitlines():
            if line.lstrip().startswith("(()"):
                assert line.split(";", 1)[0].strip() in ("(())", "(()"), (path, line)


def test_discovery_is_newest_first_without_reordering_open_polls():
    core = PlySwap()
    core.serve(Message(b"a"))
    core.serve(Message(b"b"))
    core.serve(Message(b"a"))
    assert core.serve(Message(b"c")) == Message(b"c", peer_xid=b"b")
    assert all(not row.peer_xid for row in core.rows.values())
    core.serve(Message(b"bound", peer_xid=b"a"))
    assert core.serve(Message(b"bound")) == Message(b"bound", peer_xid=b"a")


def test_first_eligible_candidate_not_first_pair_minimum():
    core = PlySwap()
    core.rows = {
        b"last": Record(b"last"),
        b"directed": Record(b"directed", peer_xid=b"own"),
        b"first": Record(b"first"),
        b"blocked": Record(b"blocked", peer_xid=b"other"),
    }
    assert core.serve(Message(b"own")) == Message(b"own", peer_xid=b"first")
    core.rows.pop(b"first")
    assert core.serve(Message(b"own")) == Message(b"own", peer_xid=b"directed")


def test_nonempty_zero_xid_and_empty_payload_preserve_empty_sealing():
    with pytest.raises(ValueError, match="self_xid"):
        Message(b"")
    core = PlySwap()
    assert core.serve(Message(b"\x00")) == Message(b"\x00")
    assert core.serve(Message(b"b")) == Message(b"b", peer_xid=b"\x00")
    full = Message(b"\x00", peer_xid=b"b", next_xid=b"a1", values=(b"",))
    assert core.serve(full) == Message(b"\x00")
    # An empty successor is still unsealed; a single empty value is DATA.
    assert core.serve(Message(b"b", peer_xid=b"\x00", next_xid=b"")) == Message(
        b"b", next_xid=b"a1", values=(b"",)
    )
    reference = Message(b"\x00", next_xid=b"a1")
    assert core.serve(reference) == Message(b"\x00")
    assert core.serve(Message(b"b", peer_xid=b"\x00", next_xid=b"b1")) == Message(
        b"b", next_xid=b"a1", values=(b"",)
    )
    assert core.serve(reference) == Message(b"\x00", next_xid=b"b1")


@pytest.mark.parametrize(
    "path",
    [
        "//",
        "/YQ/",
        "/YQ?q=1",
        "/YQ#x",
        "/%59Q",
        "/YQ==",
        "/YR",
        "/+w",
        "//YQ",
    ],
)
def test_room_path_suffixes_reject(path):
    with pytest.raises(LookupError):
        query_from_target(path)


def test_arbitrary_binary_room():
    query = query_from_target("/?room=%00%C3%BF%C2%80")
    assert decode_request(b'{"self":"a"}', query).room == b"\x00\xff\x80"


@pytest.mark.parametrize("ttl", [0, -1, float("nan"), float("inf")])
def test_invalid_cache_ttl(ttl):
    with pytest.raises(ValueError):
        PlySwapCache(ttl)


def test_reject_cannot_create_room_and_identical_self_xids_in_rooms_are_isolated():
    cache = PlySwapCache()
    assert cache.serve(b"missing", Message(b"a", next_xid=b"a1")) is None
    assert cache.room_count == 0
    cache.serve(b"one", Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"one",)))
    cache.serve(b"two", Message(b"a", peer_xid=b"b", next_xid=b"a1", values=(b"two",)))
    assert cache.serve(b"one", Message(b"b", peer_xid=b"a")) == PlySwapResponse(
        b"one", b"b", next_xid=b"a1", values=(b"one",)
    )
    assert cache.serve(b"two", Message(b"b", peer_xid=b"a")) == PlySwapResponse(
        b"two", b"b", next_xid=b"a1", values=(b"two",)
    )
