from copy import deepcopy
import json

import pytest

from rendezqueue.impl import RendezqueueImpl
from rendezqueue.swapstore import SwapStore, TrySwapResponse


def response(result: int | TrySwapResponse) -> TrySwapResponse:
    assert isinstance(result, TrySwapResponse)
    return result


def pair(store: SwapStore) -> None:
    store.tryswap("room", "A", 0, "", ["a1", "a2"], 0, 5)
    store.tryswap("room", "B", 0, "", ["b"], 0, 5)


def test_zero_offset_recovery_returns_pending_and_permits_ack_only_polls() -> None:
    store = SwapStore()
    assert store.tryswap("room", "A", 2, "B", [], 0, 5) == 404
    pending = TrySwapResponse(key="room", sid="A", ack="B", offset=2)
    assert store.tryswap("room", "A", 0, "B", ["a1", "a2"], 0, 5) == pending
    seeded = deepcopy((store.unmatched_offer_map, store.swapped_answer_multimap))
    assert store.tryswap("room", "A", 2, "B", [], 0, 5) == pending
    assert (store.unmatched_offer_map, store.swapped_answer_multimap) == seeded
    assert store.tryswap("room", "B", 1, "A", [], 0, 5) == 404
    assert store.tryswap("room", "B", 0, "A", ["b"], 0, 5) == TrySwapResponse(
        key="room", sid="B", ack="B", offset=1
    )
    assert store.tryswap("room", "A", 2, "B", [], 0, 5) == TrySwapResponse(
        key="room", sid="A", ack="A", offset=2
    )


def test_ack_uses_own_length_and_records_reciprocal_peer_evidence() -> None:
    store = SwapStore()
    pair(store)
    assert store.tryswap("room", "A", 1, "B", [], 0, 5) == 404
    assert store.tryswap("room", "A", 2, "B", [], 0, 5) == TrySwapResponse(
        key="room", sid="A", ack="B", offset=2
    )
    assert store.tryswap("room", "B", 1, "A", [], 0, 5) == TrySwapResponse(
        key="room", sid="B", ack="B", offset=1
    )


@pytest.mark.parametrize("state", ["offer", "recovery", "answer"])
def test_rejected_payloads_leave_cached_state_unchanged(state: str) -> None:
    store = SwapStore()
    store.tryswap(
        "room",
        "A",
        0,
        "B" if state == "recovery" else "",
        ["a1", "a2"],
        0,
        5,
    )
    if state == "answer":
        store.tryswap("room", "B", 0, "", ["b"], 0, 5)
    before = deepcopy((store.unmatched_offer_map, store.swapped_answer_multimap))
    invalid: list[tuple[int, list[str]]] = [
        (0, ["wrong", "a2"]),
        (1, []),
        (0, []),
        (0, ["a1"]),
        (2, ["a1", "a2"]),
        (1, ["a2"]),
    ]
    for ack in ["B", "C"]:
        for offset, values in invalid:
            assert store.tryswap("room", "A", offset, ack, values, 0, 5) == 404
            assert (store.unmatched_offer_map, store.swapped_answer_multimap) == before


def test_mismatched_cached_peer_is_rejected_without_changing_state() -> None:
    store = SwapStore()
    pair(store)
    store.tryswap("room", "X", 0, "", ["x"], 0, 5)
    store.tryswap("room", "Y", 0, "", ["y"], 0, 5)
    before = deepcopy((store.unmatched_offer_map, store.swapped_answer_multimap))
    for values in [[], ["a1", "a2"]]:
        assert store.tryswap("room", "A", 0 if values else 2, "C", values, 0, 5) == 404
        assert (store.unmatched_offer_map, store.swapped_answer_multimap) == before
    # The original pair still needs and can receive both directed ACKs.
    assert store.tryswap("room", "A", 2, "B", [], 0, 5) == TrySwapResponse(
        key="room", sid="A", ack="B", offset=2
    )
    assert store.tryswap("room", "B", 1, "A", [], 0, 5) == TrySwapResponse(
        key="room", sid="B", ack="B", offset=1
    )


@pytest.mark.parametrize("state", ["both", "forward-only", "own-offer"])
def test_recovery_does_not_delete_conflicting_live_pair(state: str) -> None:
    store = SwapStore()
    store.tryswap("room", "B", 0, "", ["b"], 0, 5)
    store.tryswap("room", "C", 0, "", ["c"], 0, 5)
    answers = store.swapped_answer_multimap["room"]
    if state == "forward-only":
        del answers["C"]
    if state == "own-offer":
        store.tryswap("room", "A", 0, "", ["a"], 0, 5)
    before = deepcopy(store.swapped_answer_multimap)
    offer = deepcopy(store.unmatched_offer_map.get("room"))

    assert store.tryswap("room", "A", 0, "B", ["a"], 0, 5) == 404
    assert store.swapped_answer_multimap == before
    if offer is not None and offer.sid is not None:
        assert store.unmatched_offer_map.get("room") == offer
    else:
        assert "room" not in store.unmatched_offer_map


def test_expired_conflicting_records_do_not_block_recovery() -> None:
    store = SwapStore()
    # The live X/Y pair makes the existing best-effort sweep stop before B/C.
    store.tryswap("room", "X", 0, "", ["x"], 0, 20)
    store.tryswap("room", "Y", 0, "", ["y"], 0, 20)
    store.tryswap("room", "B", 0, "", ["b"], 0, 1)
    store.tryswap("room", "C", 0, "", ["c"], 0, 1)
    assert store.tryswap("room", "A", 0, "B", ["a"], 1000, 5) == TrySwapResponse(
        key="room", sid="A", ack="B", offset=1
    )
    assert store.tryswap("room", "B", 0, "A", ["b"], 1000, 5) == TrySwapResponse(
        key="room", sid="B", ack="B", offset=1
    )


def test_recovery_offer_rejects_other_peers_and_legacy_append() -> None:
    store = SwapStore()
    store.tryswap("room", "A", 0, "B", ["a1", "a2"], 0, 5)
    before = deepcopy(store.unmatched_offer_map)
    assert store.tryswap("room", "C", 0, "A", ["c"], 0, 5) == 404
    assert store.tryswap("room", "A", 2, "C", [], 0, 5) == 404
    assert store.tryswap("room", "A", 2, "", ["a3"], 0, 5) == 404
    assert store.unmatched_offer_map == before
    assert store.tryswap("room", "A", 2, "B", [], 0, 5) == TrySwapResponse(
        key="room", sid="A", ack="B", offset=2
    )


def test_empty_payloads_can_recover_and_reach_terminal_evidence() -> None:
    store = SwapStore()
    store.tryswap("room", "A", 0, "B", [], 0, 5)
    assert store.tryswap("room", "B", 0, "A", [], 0, 5) == TrySwapResponse(
        key="room", sid="B", ack="B", offset=0
    )
    assert store.tryswap("room", "A", 0, "B", [], 0, 5) == TrySwapResponse(
        key="room", sid="A", ack="A", offset=0
    )


def test_ack_checks_expiry_when_best_effort_cleanup_stops_early() -> None:
    store = SwapStore()
    pair(store)
    expired = store.swapped_answer_multimap["room"]["A"]
    expired.expiry_ms = 1000
    # B precedes A in the cache, so the existing cleanup stops at live B.
    store.tryswap("room", "B", 1, "A", [], 1000, 5)
    assert not expired.peer_acked
    assert store.tryswap("room", "A", 2, "B", [], 1000, 5) == 404


def test_expired_recovery_offer_cannot_authenticate_or_constrain_ack() -> None:
    store = SwapStore()
    store.tryswap("blocker", "X", 0, "", ["x"], 0, 20)
    store.tryswap("room", "A", 0, "B", ["a"], 0, 1)
    assert store.tryswap("room", "A", 1, "B", [], 1000, 5) == 404
    assert store.tryswap("room", "A", 0, "C", ["a"], 1000, 5) == TrySwapResponse(
        key="room", sid="A", ack="C", offset=1
    )


def test_ack_pairing_and_cached_retry_omit_data_but_non_ack_still_retrieves_it() -> (
    None
):
    store = SwapStore()
    store.tryswap("room", "A", 0, "", ["a1", "a2"], 0, 5)
    for _ in range(2):
        assert store.tryswap("room", "B", 0, "A", ["b"], 0, 5) == TrySwapResponse(
            key="room", sid="B", ack="A", offset=1
        )
    assert store.tryswap("room", "B", 0, "", ["b"], 0, 5) == TrySwapResponse(
        key="room", sid="B", ack="A", offset=1, values=["a1", "a2"]
    )


@pytest.mark.parametrize("b64", [0, 2])
def test_empty_and_omitted_acks_stay_non_ack_requests(b64: int) -> None:
    for ack_fields in ({}, {"ack": ""}):
        impl = RendezqueueImpl()
        a, b = ("QQ", "Qg") if b64 else ("A", "B")

        def request(sid: str, values: list[str], offset: int = 0):
            result = impl.tryswap_string(
                json.dumps(
                    {
                        "key": "room",
                        "sid": sid,
                        "values": values,
                        "offset": offset,
                        "b64": b64,
                        **ack_fields,
                    }
                )
            )
            return json.loads(result) if isinstance(result, str) else result

        probe = request(a, [])
        assert probe["sid"] == a
        assert "ack" not in probe
        assert "room" not in impl.swapstore.unmatched_offer_map
        assert request(a, ["a"])["offset"] == 1
        assert impl.swapstore.unmatched_offer_map["room"].ack == ""
        assert request(a, ["tail"], 1)["offset"] == 2
        assert impl.swapstore.unmatched_offer_map["room"].ack == ""
        assert request(b, ["b"])["values"] == ["a", "tail"]
        cached = request(a, ["a", "tail"])
        assert (cached["ack"], cached["offset"], cached["values"]) == (b, 2, ["b"])
        assert [
            answer.peer_acked
            for answer in impl.swapstore.swapped_answer_multimap["room"].values()
        ] == [False, False]


def test_json_validates_decodes_and_forwards_ack() -> None:
    impl = RendezqueueImpl()

    def request(**msg):
        result = impl.tryswap_string(json.dumps({"key": "room", **msg}))
        return json.loads(result) if isinstance(result, str) else result

    request(sid="A", values=["a"])
    request(sid="B", values=["b"])
    assert request(sid="A", ack=None) == 400
    assert request(sid="A", ack=123) == 400
    assert request(sid="A", ack="x" * 101) == 413
    assert request(b64=2, sid="QQ", ack="Qg", offset=1) == {
        "key": "room",
        "b64": 2,
        "sid": "QQ",
        "ack": "Qg",
        "offset": 1,
    }
    assert request(b64=2, sid="Qg", ack="QQ", offset=1) == {
        "key": "room",
        "b64": 2,
        "sid": "Qg",
        "ack": "Qg",
        "offset": 1,
    }


def test_cached_pair_accepts_omitted_ack_payloads() -> None:
    store = SwapStore()
    store.tryswap("cached", "A", 0, "", ["a"], 0, 5)
    store.tryswap("cached", "B", 0, "", ["b"], 0, 5)
    store.tryswap("cached", "A", 0, "", ["a"], 0, 5)

    alice = response(store.tryswap("cached", "A", 1, "B", [], 0, 5))
    assert (alice.ack, alice.offset, alice.values) == ("B", 1, None)

    bob = response(store.tryswap("cached", "B", 1, "A", [], 0, 5))
    assert (bob.ack, bob.offset, bob.values) == ("B", 1, None)

    alice = response(store.tryswap("cached", "A", 1, "B", [], 0, 5))
    assert (alice.ack, alice.offset, alice.values) == ("A", 1, None)


def test_restart_requires_one_full_resend_from_each_side() -> None:
    store = SwapStore()

    assert store.tryswap("restart", "A", 1, "B", [], 0, 5) == 404
    waiting = response(store.tryswap("restart", "A", 0, "B", ["a"], 0, 5))
    assert (waiting.ack, waiting.offset, waiting.values) == ("B", 1, None)

    assert store.tryswap("restart", "B", 1, "A", [], 0, 5) == 404
    bob = response(store.tryswap("restart", "B", 0, "A", ["b"], 0, 5))
    assert (bob.ack, bob.offset, bob.values) == ("B", 1, None)

    alice = response(store.tryswap("restart", "A", 1, "B", [], 0, 5))
    assert (alice.ack, alice.offset, alice.values) == ("A", 1, None)


def test_one_sided_answer_expiry_uses_the_same_fallback() -> None:
    store = SwapStore()
    store.tryswap("expiry", "A", 0, "", ["a"], 0, 5)
    store.tryswap("expiry", "B", 0, "", ["b"], 0, 5)
    del store.swapped_answer_multimap["expiry"]["A"]

    assert store.tryswap("expiry", "A", 1, "B", [], 0, 5) == 404
    response(store.tryswap("expiry", "A", 0, "B", ["a"], 0, 5))
    assert store.tryswap("expiry", "B", 1, "A", [], 0, 5) == 404

    bob = response(store.tryswap("expiry", "B", 0, "A", ["b"], 0, 5))
    assert (bob.ack, bob.offset) == ("B", 1)
    alice = response(store.tryswap("expiry", "A", 1, "B", [], 0, 5))
    assert (alice.ack, alice.offset) == ("A", 1)


def test_forced_offer_accepts_exact_peer_non_ack_replay() -> None:
    store = SwapStore()

    response(store.tryswap("forced", "A", 0, "B", ["a"], 0, 5))
    bob = response(store.tryswap("forced", "B", 0, "", ["b"], 0, 5))
    assert (bob.ack, bob.offset, bob.values) == ("A", 1, ["a"])

    bob = response(store.tryswap("forced", "B", 1, "A", [], 0, 5))
    assert (bob.ack, bob.offset, bob.values) == ("B", 1, None)
    alice = response(store.tryswap("forced", "A", 1, "B", [], 0, 5))
    assert (alice.ack, alice.offset, alice.values) == ("A", 1, None)


def test_ack_rejects_unrelated_live_offer_then_recovers_after_expiry() -> None:
    store = SwapStore()

    store.tryswap("stale", "X", 0, "", ["x"], 0, 5)
    before = deepcopy(store.unmatched_offer_map)
    assert store.tryswap("stale", "A", 0, "B", ["a"], 0, 5) == 404
    assert store.unmatched_offer_map == before
    response(store.tryswap("stale", "A", 0, "B", ["a"], 5000, 5))
    bob = response(store.tryswap("stale", "B", 0, "A", ["b"], 5000, 5))
    assert (bob.ack, bob.offset) == ("B", 1)
    alice = response(store.tryswap("stale", "A", 1, "B", [], 5000, 5))
    assert (alice.ack, alice.offset) == ("A", 1)


def test_conflicting_third_sid_is_rejected_without_changing_forced_offer() -> None:
    store = SwapStore()

    response(store.tryswap("conflict", "A", 0, "B", ["a"], 0, 5))
    assert store.tryswap("conflict", "C", 0, "", ["c"], 0, 5) == 404
    bob = response(store.tryswap("conflict", "B", 0, "A", ["b"], 0, 5))
    assert (bob.ack, bob.offset) == ("B", 1)


def test_rejected_payload_mismatch_does_not_commit_routed_ack() -> None:
    store = SwapStore()

    store.tryswap("transaction", "A", 0, "", ["a"], 0, 5)
    store.tryswap("transaction", "B", 0, "", ["b"], 0, 5)
    assert store.tryswap("transaction", "B", 0, "A", ["WRONG"], 0, 5) == 404

    alice = response(store.tryswap("transaction", "A", 1, "B", [], 0, 5))
    assert (alice.ack, alice.offset, alice.values) == ("B", 1, None)
    bob = response(store.tryswap("transaction", "B", 1, "A", [], 0, 5))
    assert (bob.ack, bob.offset) == ("B", 1)
    alice = response(store.tryswap("transaction", "A", 1, "B", [], 0, 5))
    assert (alice.ack, alice.offset) == ("A", 1)


def test_exact_recovery_preserves_unrelated_cached_pair() -> None:
    store = SwapStore()

    store.tryswap("unrelated", "X", 0, "", ["x"], 0, 5)
    store.tryswap("unrelated", "Y", 0, "", ["y"], 0, 5)
    store.tryswap("unrelated", "A", 0, "B", ["a"], 0, 5)
    store.tryswap("unrelated", "B", 0, "A", ["b"], 0, 5)

    old = response(store.tryswap("unrelated", "X", 0, "", ["x"], 0, 5))
    assert (old.ack, old.offset, old.values) == ("Y", 1, ["y"])


def test_ack_payload_length_discrimination() -> None:
    store = SwapStore()

    # Nonempty ACK uploads at a nonzero offset are malformed.
    assert store.tryswap("validation", "A", 2, "B", ["a"], 0, 5) == 404
    # Empty values plus positive offset means omitted and requires a cache hit.
    assert store.tryswap("validation", "A", 1, "B", [], 0, 5) == 404
    # Empty values at offset zero are a complete genuinely empty payload.
    waiting = response(store.tryswap("empty", "A", 0, "B", [], 0, 5))
    assert (waiting.ack, waiting.offset, waiting.values) == ("B", 0, None)
