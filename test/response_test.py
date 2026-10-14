import json
import unittest
from unittest.mock import patch
from typing import Any, Dict

from rendezqueue.impl import RendezqueueImpl, btoa
from rendezqueue.swapstore import TrySwapResponse


class TestResponseNormalization(unittest.TestCase):
    def test_empty_optional_fields(self):
        for values in (None, []):
            for ttl in (None, 0):
                with self.subTest(values=values, ttl=ttl):
                    response = TrySwapResponse(
                        key="room", sid="A", offset=0, values=values, ttl=ttl
                    )
                    self.assertIsNone(response.values)
                    self.assertIsNone(response.ttl)
                    self.assertIsNone(response.ack)
                    self.assertEqual(response.offset, 0)

    def test_nonempty_fields_are_preserved(self):
        values = [""]
        response = TrySwapResponse(
            key="room", sid="A", offset=1, ack="B", values=values, ttl=7
        )
        self.assertIs(response.values, values)
        self.assertEqual(response.ttl, 7)
        self.assertEqual(response.ack, "B")
        self.assertEqual(response.offset, 1)

    def test_empty_ack_is_not_normalized_by_this_prefactor(self):
        response = TrySwapResponse(key="room", sid="A", offset=0, ack="")
        self.assertEqual(response.ack, "")

    def test_raw_and_base64_serialization(self):
        for b64 in (0, 7):
            for values in ([], [""]):
                for ttl in (0, 7):
                    with self.subTest(b64=b64, values=values, ttl=ttl):
                        impl = RendezqueueImpl()
                        response = TrySwapResponse(
                            key="room", sid="A", offset=0, values=values, ttl=ttl
                        )
                        with patch.object(
                            impl.swapstore, "tryswap", return_value=response
                        ):
                            result = impl.tryswap_string(json.dumps({"b64": b64}))
                        assert isinstance(result, str)
                        expected: Dict[str, Any] = {"key": "room", "sid": "A"}
                        if b64:
                            expected = {
                                "key": btoa("room"),
                                "sid": btoa("A"),
                                "b64": 7 if values else 6,
                            }
                        if values:
                            expected["values"] = [""]
                        if ttl:
                            expected["ttl"] = ttl
                        self.assertEqual(json.loads(result), expected)
