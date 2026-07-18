import unittest
import sxpb
import os
from rendezqueue.impl import RendezqueueImpl
from rendezqueue.swapstore import SwapStore, TrySwapResponse
from typing import Any, Dict, List, cast


class TestServerSxPB(unittest.TestCase):
    def test_sxpb_cases(self):
        sxpb_path = os.path.join(os.path.dirname(__file__), "server_test_cases.sxpb")
        with open(sxpb_path, "r") as f:
            data = f.read()

        # sxpb.loads returns Any, but we know it's a list of dicts based on our file format
        test_cases = cast(List[Dict[str, Any]], sxpb.loads(data))

        for case in test_cases:
            case_name = case.get("name", "Unknown")
            print(f"Running test case: {case_name}")

            impl = RendezqueueImpl()
            now_ms = case.get("now_ms", 1000)

            # Setup phase
            setup_block = case.get("setup", {})
            if setup_block:
                for req in cast(List[Dict[str, Any]], setup_block.get("requests", [])):
                    self._run_request(impl, req, now_ms)

            # Request phase
            req = case.get("request")
            expected_resp = case.get("response")
            expected_error = case.get("error_code")

            if req:
                actual_resp = self._run_request(impl, req, now_ms)

                if expected_error:
                    self.assertIsInstance(
                        actual_resp,
                        int,
                        f"Case {case_name}: Expected error code {expected_error}, got response object",
                    )
                    self.assertEqual(
                        actual_resp,
                        expected_error,
                        f"Case {case_name}: Expected error code {expected_error}, got {actual_resp}",
                    )
                elif expected_resp:
                    if isinstance(actual_resp, int):
                        self.fail(
                            f"Case {case_name}: Expected success, got error code {actual_resp}"
                        )

                    # Verify response fields
                    self._verify_response(actual_resp, expected_resp, case_name)

    def _run_request(self, impl: RendezqueueImpl, req: Dict[str, Any], now_ms: float):
        # Construct message dict
        msg = {}
        if "key" in req:
            msg["key"] = req["key"]
        if "sid" in req:
            msg["sid"] = req["sid"]
        if "offset" in req:
            msg["offset"] = req["offset"]
        if "ttl" in req:
            msg["ttl"] = req["ttl"]
        if "b64" in req:
            msg["b64"] = req["b64"]

        if "values" in req:
            msg["values"] = req["values"]

        return impl.tryswap(msg, now_ms=now_ms)

    def _verify_response(self, actual, expected, case_name):
        # actual is TrySwapResponse dataclass
        if "key" in expected:
            self.assertEqual(
                actual.key, expected["key"], f"Case {case_name}: Key mismatch"
            )
        if "sid" in expected:
            self.assertEqual(
                actual.sid, expected["sid"], f"Case {case_name}: SID mismatch"
            )
        if "offset" in expected:
            self.assertEqual(
                actual.offset, expected["offset"], f"Case {case_name}: Offset mismatch"
            )

        if "ttl" in expected:
            self.assertEqual(
                actual.ttl, expected["ttl"], f"Case {case_name}: TTL mismatch"
            )
        else:
            # If ttl not expected, it implies it should be None or not present in JSON output?
            # But dataclass has it. The test logic: "ttl should not be present in response when values are present"
            # This logic is usually enforced during encoding.
            pass

        if "values" in expected:
            self.assertEqual(
                actual.values, expected["values"], f"Case {case_name}: Values mismatch"
            )
        else:
            # Expect no values
            self.assertIsNone(
                actual.values,
                f"Case {case_name}: Expected no values, got {actual.values}",
            )


class TestAccessExpiry(unittest.TestCase):
    def test_expired_offer_is_not_swapped_when_cleanup_is_blocked(self):
        swapstore = SwapStore()

        swapstore.tryswap("blocker", "a", 0, ["live"], 0, 20)
        swapstore.tryswap("target", "b", 0, ["stale"], 0, 1)

        result = swapstore.tryswap("target", "c", 0, ["fresh"], 1000, 1)
        self.assertIsInstance(result, TrySwapResponse)
        assert isinstance(result, TrySwapResponse)
        self.assertEqual(result.offset, 1)
        self.assertEqual(result.ttl, 1)
        self.assertIsNone(result.values)
        self.assertEqual(swapstore.unmatched_offer_map["target"].sid, "c")
        self.assertEqual(swapstore.unmatched_offer_map["target"].values, ["fresh"])

    def test_expired_answer_is_not_returned_when_cleanup_is_blocked(self):
        swapstore = SwapStore()

        swapstore.tryswap("blocker", "a", 0, ["live"], 0, 20)
        swapstore.tryswap("target", "b", 0, ["from-b"], 0, 1)
        swapstore.tryswap("target", "c", 0, ["from-c"], 0, 1)

        result = swapstore.tryswap("target", "b", 1, [], 1000, 1)
        self.assertEqual(result, 404)


if __name__ == "__main__":
    unittest.main()
