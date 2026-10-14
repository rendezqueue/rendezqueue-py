from concurrent.futures import ThreadPoolExecutor
import threading
import unittest
from unittest.mock import patch

from rendezqueue.impl import RendezqueueImpl
from rendezqueue.swapstore import SwappedAnswer, TrySwapResponse


class TestServerSerialization(unittest.TestCase):
    def test_recovery_cannot_interrupt_pairing_or_read_the_clock_early(self):
        impl = RendezqueueImpl()
        first = {"key": "room", "sid": "A", "ack": "B", "values": ["a"]}
        second = {"key": "room", "sid": "B", "values": ["b"]}
        impl.tryswap(first, now_ms=9000)
        pairing_paused = threading.Event()
        resume_pairing = threading.Event()
        second_attempted = threading.Event()

        class PausingAnswers(dict[str, SwappedAnswer]):
            def __setitem__(self, sid: str, answer: SwappedAnswer) -> None:
                super().__setitem__(sid, answer)
                if sid == "B" and not pairing_paused.is_set():
                    pairing_paused.set()
                    if not resume_pairing.wait(5):
                        raise AssertionError("Pairing was not resumed")

        class ObservedLock:
            def __init__(self):
                self.lock = threading.Lock()
                self.attempts = 0

            def __enter__(self):
                self.attempts += 1
                if self.attempts == 2:
                    second_attempted.set()
                self.lock.acquire()

            def __exit__(self, *args):
                self.lock.release()

        # Pause at the half-written pair without deleting any cache entry.
        impl.swapstore.swapped_answer_multimap["room"] = PausingAnswers()
        with (
            patch.object(impl, "_lock", ObservedLock(), create=True),
            patch("rendezqueue.impl.time.monotonic", side_effect=[10.0, 11.0]) as clock,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            pairing = executor.submit(impl.tryswap, second)
            try:
                self.assertTrue(pairing_paused.wait(5))
                recovery = executor.submit(impl.tryswap, first)
                # Without serialization, completion also wakes this wait, so
                # the test fails on the early clock read rather than hanging.
                recovery.add_done_callback(lambda _: second_attempted.set())
                self.assertTrue(second_attempted.wait(5))
                self.assertEqual(clock.call_count, 1)
                self.assertFalse(recovery.done())
            finally:
                resume_pairing.set()
            self.assertIsInstance(pairing.result(timeout=5), TrySwapResponse)
            self.assertIsInstance(recovery.result(timeout=5), TrySwapResponse)
            self.assertEqual(clock.call_count, 2)

        answers = impl.swapstore.swapped_answer_multimap["room"]
        self.assertEqual(set(answers), {"A", "B"})
        self.assertEqual(answers["A"].peer_sid, "B")
        self.assertEqual(answers["B"].peer_sid, "A")
        self.assertTrue(answers["B"].peer_acked)
        self.assertEqual(answers["A"].expiry_ms, 30000)
        self.assertEqual(answers["B"].expiry_ms, 30000)
