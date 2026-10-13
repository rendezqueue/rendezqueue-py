import copy
import json
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

from rendezqueue.impl import RendezqueueImpl
from rendezqueue.server import RendezqueueHandler, RendezqueueServer


class TestValueValidation(unittest.TestCase):
    def test_rejects_non_string_values_without_mutation(self):
        for b64 in (0, 1):
            for value in (None, 1, True, {}, []):
                with self.subTest(b64=b64, value=value):
                    impl = RendezqueueImpl()
                    impl.tryswap_string(
                        json.dumps({"key": "room", "sid": "A", "values": ["seed"]})
                    )
                    offers = copy.deepcopy(impl.swapstore.unmatched_offer_map)
                    answers = copy.deepcopy(impl.swapstore.swapped_answer_multimap)
                    result = impl.tryswap_string(
                        json.dumps(
                            {
                                "key": "room",
                                "sid": "B",
                                "values": ["YQ", value],
                                "b64": b64,
                            }
                        )
                    )
                    self.assertEqual(result, 400)
                    self.assertEqual(impl.swapstore.unmatched_offer_map, offers)
                    self.assertEqual(impl.swapstore.swapped_answer_multimap, answers)

    def test_accepts_empty_batches_and_empty_strings(self):
        for b64 in (0, 1):
            impl = RendezqueueImpl()
            offered = impl.tryswap_string(
                json.dumps({"key": "room", "sid": "A", "values": [""], "b64": b64})
            )
            self.assertIsInstance(offered, str)
            paired = impl.tryswap_string(
                json.dumps({"key": "room", "sid": "B", "values": [], "b64": b64})
            )
            assert isinstance(paired, str)
            self.assertEqual(json.loads(paired)["values"], [""])

    def test_http_rejects_bad_values_and_keeps_serving(self):
        server = RendezqueueServer(
            ("127.0.0.1", 0), RendezqueueHandler, RendezqueueImpl()
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}/"
        try:
            with patch.object(RendezqueueHandler, "log_message"):
                for value in (None, 1, True, {}, []):
                    request = Request(
                        url,
                        data=json.dumps(
                            {"key": "invalid-values", "sid": "A", "values": [value]}
                        ).encode(),
                        headers={"Content-Type": "application/json"},
                    )
                    with self.assertRaises(HTTPError) as caught:
                        urlopen(request, timeout=5)
                    self.assertEqual(caught.exception.code, 400)
                    caught.exception.close()
                request = Request(
                    url,
                    data=json.dumps(
                        {"key": "good", "sid": "B", "values": ["ok"]}
                    ).encode(),
                    headers={"Content-Type": "application/json"},
                )
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 200)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
