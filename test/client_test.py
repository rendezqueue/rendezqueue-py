import unittest
from unittest.mock import MagicMock, patch
import json
import base64
from rendezqueue import RendezqueueClient


class TestRendezqueueClient(unittest.TestCase):
    def setUp(self):
        self.url = "http://example.com"
        self.key = "test-key"
        self.hue = "test-hue"
        self.on_data = MagicMock()
        self.on_error = MagicMock()
        self.client = RendezqueueClient(
            url=self.url,
            key=self.key,
            hue=self.hue,
            on_data=self.on_data,
            on_error=self.on_error,
            poll_interval_ms=100,
        )

    def test_init(self):
        self.assertEqual(self.client.url, self.url)
        self.assertEqual(self.client.key, self.key)
        self.assertTrue(self.client.sid.startswith(f"{self.hue}-1-"))
        self.assertEqual(self.client.offset, 0)
        self.assertEqual(self.client.outgoing_queue, [])

    def test_send(self):
        self.client.send("hello")
        self.client.send(b"world")
        self.assertEqual(self.client.outgoing_queue, [b"hello", b"world"])

    @patch("urllib.request.urlopen")
    def test_poll_send_and_receive(self, mock_urlopen):
        # Scenario: Client sends "val".
        self.client.send("val")

        # Mock server response: echo back "response" and ack offset.
        # Request offset=0. Client sends 1 item.
        # Response: offset=1 (ack), values=["response_b64"]

        resp_values = [base64.b64encode(b"response").decode("ascii")]
        resp_data = {"offset": 1, "values": resp_values, "b64": 1}

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = json.dumps(resp_data).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        # Trigger poll manually
        self.client._poll()

        # Verify request
        args, kwargs = mock_urlopen.call_args
        req = args[0]
        self.assertEqual(req.full_url, self.url)
        req_body = json.loads(req.data)
        self.assertEqual(req_body["key"], self.key)
        self.assertEqual(req_body["offset"], 0)
        self.assertEqual(len(req_body["values"]), 1)
        self.assertEqual(base64.b64decode(req_body["values"][0]), b"val")

        # Verify state update
        self.assertEqual(self.client.offset, 0)  # Reset due to new session
        self.assertEqual(self.client.outgoing_queue, [])
        self.assertTrue(
            self.client.sid.startswith(f"{self.hue}-2-")
        )  # New session started

        # Verify callback
        self.on_data.assert_called_once_with([b"response"])

    @patch("urllib.request.urlopen")
    def test_poll_partial_ack(self, mock_urlopen):
        # Scenario: Client sends 2 items. Server acks 1.
        self.client.send("msg1")
        self.client.send("msg2")

        # Response: offset=1 (ack 1 msg), no values (session open)
        # Must include ttl to indicate session is still open if offset > 0
        resp_data = {"offset": 1, "b64": 0, "ttl": 60}

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = json.dumps(resp_data).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        self.client._poll()

        # Verify queue updated
        self.assertEqual(len(self.client.outgoing_queue), 1)
        self.assertEqual(self.client.outgoing_queue[0], b"msg2")
        self.assertEqual(self.client.offset, 1)
        self.assertTrue(
            self.client.sid.startswith(f"{self.hue}-1-")
        )  # Session continues
        self.on_data.assert_not_called()

    def test_decode_padding(self):
        # Test missing padding
        encoded = base64.b64encode(b"test padding").decode("ascii").rstrip("=")
        msg = {"values": [encoded], "b64": 1}
        decoded = self.client._decode_response(msg)
        self.assertEqual(decoded["values"][0], b"test padding")

    def test_base64url_decoding(self):
        # Test base64url decoding with missing padding
        encoded = base64.urlsafe_b64encode(b"ab\xbf\x9c").decode("ascii").rstrip("=")
        msg = {"values": [encoded], "b64": 1}
        decoded = self.client._decode_response(msg)
        self.assertEqual(decoded["values"][0], b"ab\xbf\x9c")

    @patch("urllib.request.urlopen")
    def test_defaults(self, mock_urlopen):
        # Initialize client without callbacks
        client = RendezqueueClient(url=self.url, key=self.key, hue=self.hue)
        self.assertIsNone(client.on_data)
        self.assertIsNotNone(client.on_error)  # Should be default logger lambda

        # Simulate receiving data
        resp_values = [base64.b64encode(b"response").decode("ascii")]
        resp_data = {"offset": 1, "values": resp_values, "b64": 1}
        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.read.return_value = json.dumps(resp_data).encode("utf-8")
        mock_resp.__enter__.return_value = mock_resp
        mock_urlopen.return_value = mock_resp

        # Trigger poll - should not crash
        try:
            client._poll()
        except Exception as e:
            self.fail(f"_poll raised exception unexpectedly: {e}")


if __name__ == "__main__":
    unittest.main()
