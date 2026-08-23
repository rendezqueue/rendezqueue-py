import os
import tempfile
import threading
import time
import unittest
from typing import Dict, List
from unittest.mock import patch

from rendezqueue.server import run
from rendezqueue.swap import swap_once


class SwapTest(unittest.TestCase):
    port = 0
    port_filepath = None

    @classmethod
    def setUpClass(cls):
        cls.port_file = tempfile.NamedTemporaryFile(delete=False)
        cls.port_file.close()
        cls.port_filepath = cls.port_file.name

        cls.server_thread = threading.Thread(target=run, daemon=True)
        with patch(
            "sys.argv",
            [
                "server.py",
                "--http_host",
                "127.0.0.1",
                "--http_port",
                "0",
                "--http_path",
                "/tryswap",
                "--port_filepath",
                cls.port_filepath,
            ],
        ):
            cls.server_thread.start()
            for _ in range(50):
                try:
                    with open(cls.port_filepath) as f:
                        content = f.read().strip()
                        if content:
                            cls.port = int(content)
                            break
                except (FileNotFoundError, ValueError):
                    pass
                time.sleep(0.1)
            else:
                raise Exception("Server failed to start in tool test")

    @classmethod
    def tearDownClass(cls):
        if cls.port_filepath and os.path.exists(cls.port_filepath):
            os.remove(cls.port_filepath)

    def test_swap_once(self):
        url = f"http://127.0.0.1:{self.port}/tryswap"
        key = "swap-test-key"
        results: Dict[str, List[bytes]] = {}

        def a() -> None:
            results["a"] = swap_once(url=url, key=key, value=b"from-a", timeout=5)

        def b() -> None:
            results["b"] = swap_once(url=url, key=key, value=b"from-b", timeout=5)

        ta = threading.Thread(target=a)
        tb = threading.Thread(target=b)
        ta.start()
        tb.start()
        ta.join(timeout=10)
        tb.join(timeout=10)

        self.assertIn("a", results)
        self.assertIn("b", results)
        self.assertEqual(results["a"], [b"from-b"])
        self.assertEqual(results["b"], [b"from-a"])

    def test_swap_once_timeout(self):
        url = f"http://127.0.0.1:{self.port}/tryswap"
        key = "swap-timeout-key"
        with self.assertRaises(TimeoutError):
            swap_once(url=url, key=key, value=b"lonely", timeout=1.0)


if __name__ == "__main__":
    unittest.main()
