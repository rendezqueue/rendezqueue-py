import threading
import time
import unittest
import os
import tempfile
from unittest.mock import patch
from rendezqueue.server import run
from rendezqueue.client import RendezqueueClient


class IntegrationTest(unittest.TestCase):
    port = 0
    server_thread = None
    port_filepath = None

    @classmethod
    def setUpClass(cls):
        # Create a temp file for the port
        cls.port_file = tempfile.NamedTemporaryFile(delete=False)
        cls.port_file.close()
        cls.port_filepath = cls.port_file.name

        cls.server_thread = threading.Thread(target=run, daemon=True)
        # Use patch to pass args to run()
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

            # Wait for server to start
            for _ in range(50):
                try:
                    with open(cls.port_filepath, "r") as f:
                        content = f.read().strip()
                        if content:
                            cls.port = int(content)
                            break
                except (FileNotFoundError, ValueError):
                    pass
                time.sleep(0.1)
            else:
                raise Exception("Server failed to start in integration test")

    @classmethod
    def tearDownClass(cls):
        if cls.port_filepath and os.path.exists(cls.port_filepath):
            os.remove(cls.port_filepath)

    def test_client_swap(self):
        url = f"http://127.0.0.1:{self.port}/tryswap"
        key = "test-key-integration"
        hue = "test-hue"

        received_s1 = []
        received_s2 = []

        def on_data_s1(values):
            received_s1.extend(values)

        def on_data_s2(values):
            received_s2.extend(values)

        client1 = RendezqueueClient(
            url=url, key=key, hue=hue, on_data=on_data_s1, poll_interval_ms=100
        )
        client2 = RendezqueueClient(
            url=url, key=key, hue=hue, on_data=on_data_s2, poll_interval_ms=100
        )

        try:
            client1.start()
            client2.start()

            # Send messages
            client1.send("msg_from_s1")
            client2.send("msg_from_s2")

            # Wait for exchange
            start_time = time.time()
            while time.time() - start_time < 5:
                if received_s1 and received_s2:
                    break
                time.sleep(0.1)

            self.assertEqual(received_s1, [b"msg_from_s2"])
            self.assertEqual(received_s2, [b"msg_from_s1"])

        finally:
            client1.stop()
            client2.stop()


if __name__ == "__main__":
    unittest.main()
