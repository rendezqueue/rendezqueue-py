from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union, cast

MAX_TTL_SECONDS = 20


@dataclass
class UnmatchedOffer:
    expiry_ms: float
    sid: Optional[str] = None
    values: List[str] = field(default_factory=list)
    ack: str = ""


@dataclass
class SwappedAnswer:
    original_values: List[str]
    peer_sid: str
    values: List[str]
    peer_acked: bool
    expiry_ms: float


@dataclass
class TrySwapResponse:
    key: str
    sid: str
    offset: int
    ack: Optional[str] = None
    values: Optional[List[str]] = None
    ttl: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.values:
            self.values = None
        if self.ttl == 0:
            self.ttl = None


class SwapStore:
    def __init__(self) -> None:
        # key -> UnmatchedOffer
        self.unmatched_offer_map: Dict[str, UnmatchedOffer] = {}
        # key -> sid -> SwappedAnswer
        self.swapped_answer_multimap: Dict[str, Dict[str, SwappedAnswer]] = {}
        self.ttl = MAX_TTL_SECONDS

    def expire_swapped_answers(self, key: str, now_ms: float) -> bool:
        answer_map = self.swapped_answer_multimap.get(key)
        if not answer_map:
            return True

        expiring_answers = []
        for sid, v in answer_map.items():
            if v.expiry_ms > now_ms:
                break
            expiring_answers.append(sid)

        if len(expiring_answers) == len(answer_map):
            del self.swapped_answer_multimap[key]
            return True

        for sid in expiring_answers:
            del answer_map[sid]
        return False

    def expire_unmatched_offers(self, now_ms: float) -> None:
        expiring_offers = []
        # Python dicts maintain insertion order, allowing this logic to work like JS Map
        for key, v in self.unmatched_offer_map.items():
            if v.expiry_ms > now_ms:
                break
            if not self.expire_swapped_answers(key, now_ms):
                break
            expiring_offers.append(key)

        for key in expiring_offers:
            del self.unmatched_offer_map[key]

    @staticmethod
    def matches_original(
        original_values: List[str], offset: int, values: List[str]
    ) -> bool:
        if len(original_values) < offset:
            return False
        if len(original_values) > offset + len(values):
            return False

        original_slice = original_values[offset:]

        for i, v in enumerate(original_slice):
            if values[i] != v:
                return False
        return True

    @staticmethod
    def matches_ack_payload(
        original_values: List[str], offset: int, values: List[str]
    ) -> bool:
        return (offset == 0 and values == original_values) or (
            not values and offset == len(original_values)
        )

    def _forget_pair(self, key: str, sid: str, peer_sid: str, now_ms: float) -> bool:
        answer_map = self.swapped_answer_multimap.get(key)
        if answer_map is None:
            return True
        peer = answer_map.get(peer_sid)
        if peer is not None and peer.expiry_ms > now_ms and peer.peer_sid != sid:
            return False
        answer_map.pop(sid, None)
        answer_map.pop(peer_sid, None)
        if not answer_map:
            del self.swapped_answer_multimap[key]
        return True

    def tryswap(
        self,
        key: str,
        sid: str,
        offset: int,
        ack: str,
        values: List[str],
        now_ms: float,
        ttl: int = 0,
    ) -> Union[int, TrySwapResponse]:
        if not isinstance(now_ms, (int, float)):
            return 500

        self.expire_unmatched_offers(now_ms)
        answer_map = self.swapped_answer_multimap.get(key)

        if ttl == 0 or ttl > self.ttl:
            ttl = self.ttl

        if answer_map:
            answer = answer_map.get(sid)
            if answer and answer.expiry_ms <= now_ms:
                del answer_map[sid]
                if not answer_map:
                    del self.swapped_answer_multimap[key]
                    answer_map = None
                answer = None
            if answer:
                if ack == "" and SwapStore.matches_original(
                    answer.original_values, offset, values
                ):
                    return TrySwapResponse(
                        key=key,
                        sid=sid,
                        offset=len(answer.original_values),
                        ack=answer.peer_sid,
                        values=answer.values,
                    )
                if ack != "" and SwapStore.matches_ack_payload(
                    answer.original_values, offset, values
                ):
                    if ack != answer.peer_sid:
                        return 404
                    peer = cast(Dict[str, SwappedAnswer], answer_map)[ack]
                    if peer.peer_sid == sid and peer.expiry_ms > now_ms:
                        peer.peer_acked = True
                    return TrySwapResponse(
                        key=key,
                        sid=sid,
                        offset=len(answer.original_values),
                        ack=sid if answer.peer_acked else answer.peer_sid,
                    )
                return 404

        offer = self.unmatched_offer_map.get(key)
        if offer and offer.expiry_ms <= now_ms:
            del self.unmatched_offer_map[key]
            offer = None
            self.expire_swapped_answers(key, now_ms)

        if not offer:
            if offset != 0:
                return 404
            if values or ack != "":
                if ack != "" and not self._forget_pair(key, sid, ack, now_ms):
                    return 404
                self.unmatched_offer_map[key] = UnmatchedOffer(
                    sid=sid,
                    values=values,
                    ack=ack,
                    expiry_ms=now_ms + ttl * 1000,
                )
            if ack != "":
                return TrySwapResponse(key=key, sid=sid, ack=ack, offset=len(values))
            return TrySwapResponse(
                key=key,
                sid=sid,
                offset=len(values),
                ttl=ttl,
            )

        if offer.sid is None:
            return 500  # Unexpected.

        if offer.sid == sid:
            original_values = offer.values or []
            if ack != "":
                if not SwapStore.matches_ack_payload(original_values, offset, values):
                    return 404
                if offer.ack != "" and offer.ack != ack:
                    return 404
                if not self._forget_pair(key, sid, ack, now_ms):
                    return 404
                offer.ack = ack
                return TrySwapResponse(
                    key=key, sid=sid, ack=ack, offset=len(original_values)
                )
            if offer.ack != "":
                # A legacy replay must not append to an already acknowledged payload.
                if values != original_values:
                    return 404
                return TrySwapResponse(key=key, sid=sid, offset=len(values), ttl=ttl)
            if SwapStore.matches_original(original_values, offset, values):
                new_values = original_values[:offset] + values

                self.unmatched_offer_map[key] = UnmatchedOffer(
                    sid=sid,
                    values=new_values,
                    expiry_ms=now_ms + ttl * 1000,
                )
                return TrySwapResponse(
                    key=key,
                    sid=sid,
                    offset=offset + len(values),
                    ttl=ttl,
                )
            return 404

        if ack != "" and offset != 0:
            return 404
        if offer.ack != "" and offer.ack != sid:
            return 404
        if ack == "" and offset != 0:
            return 404
        if ack != "":
            if ack != offer.sid:
                return 404
            if not self._forget_pair(key, sid, ack, now_ms):
                return 404
            answer_map = self.swapped_answer_multimap.get(key)

        if answer_map is None:
            answer_map = {}
            self.swapped_answer_multimap[key] = answer_map

        answer_map[sid] = SwappedAnswer(
            original_values=values,
            peer_sid=offer.sid,
            values=offer.values or [],
            peer_acked=offer.ack != "" and offer.ack == sid,
            expiry_ms=now_ms + ttl * 1000,
        )

        if offer.sid:
            answer_map[offer.sid] = SwappedAnswer(
                original_values=offer.values or [],
                peer_sid=sid,
                values=values,
                peer_acked=ack != "" and ack == offer.sid,
                expiry_ms=now_ms + ttl * 1000,
            )

        del self.unmatched_offer_map[key]
        self.unmatched_offer_map[key] = UnmatchedOffer(expiry_ms=0)
        if ack != "" and offer.ack != "" and offer.ack == sid:
            return TrySwapResponse(key=key, sid=sid, ack=sid, offset=len(values))

        return TrySwapResponse(
            key=key,
            sid=sid,
            offset=len(values),
            ack=offer.sid,
            values=offer.values if ack == "" else None,
        )
