import base64
import json
import logging
import random
import string
import threading
import time
import urllib.request
import urllib.error
from typing import Callable, Optional, List, Dict, Any

logger = logging.getLogger(__name__)


class RendezqueueClient:
    def __init__(
        self,
        url: str,
        key: str,
        hue: str,
        on_data: Optional[Callable[[List[bytes]], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        poll_interval_ms: int = 2000,
    ) -> None:
        self.url = url
        self.key = key
        self.hue = hue
        self.on_data = on_data
        self.on_error = on_error or (lambda e: logger.error(f"Rendezqueue error: {e}"))
        self.poll_interval_ms = poll_interval_ms

        self.sid_counter = 1
        self.sid = self._generate_sid()
        self.offset = 0
        self.outgoing_queue: List[bytes] = []
        self.lock = threading.Lock()

        self.is_polling = False
        self.is_stopped = True
        self.poll_thread: Optional[threading.Thread] = None

    def _generate_sid(self) -> str:
        random_part = "".join(
            random.choices(string.ascii_lowercase + string.digits, k=7)
        )
        return f"{self.hue}-{self.sid_counter}-{random_part}"

    def start(self) -> None:
        if not self.is_stopped:
            return
        self.is_stopped = False
        self.poll_thread = threading.Thread(target=self._poll_loop, daemon=True)
        self.poll_thread.start()

    def stop(self) -> None:
        if self.is_stopped:
            return
        self.is_stopped = True

    def send(self, value: Any) -> None:
        if isinstance(value, str):
            value = value.encode("utf-8")
        with self.lock:
            self.outgoing_queue.append(value)

    def _start_new_session(self) -> None:
        with self.lock:
            self.sid_counter += 1
            self.sid = self._generate_sid()
            self.offset = 0

    def _poll_loop(self) -> None:
        # Initial poll
        self._poll()
        while not self.is_stopped:
            time.sleep(self.poll_interval_ms / 1000.0)
            if self.is_stopped:
                break
            self._poll()

    def _poll(self) -> None:
        if self.is_polling:
            return
        self.is_polling = True
        try:
            with self.lock:
                req_sid = self.sid
                req_offset = self.offset
                # Take snapshot of queue to send
                snapshot_queue = list(self.outgoing_queue)
                b64_values = [
                    base64.urlsafe_b64encode(v).decode("ascii").rstrip("=")
                    for v in snapshot_queue
                ]

            request_body = {
                "key": self.key,
                "sid": req_sid,
                "offset": req_offset,
                "values": b64_values,
                "b64": 1,
            }

            data = json.dumps(request_body).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "User-Agent": "Rendezqueue-Python-Client/0.0.0",
            }
            req = urllib.request.Request(self.url, data=data, headers=headers)

            try:
                with urllib.request.urlopen(req, timeout=10) as response:
                    if response.status != 200:
                        text = response.read().decode("utf-8")
                        self.on_error(
                            Exception(f"Server error: {response.status} {text}")
                        )
                        return

                    resp_body = response.read().decode("utf-8")
                    msg = json.loads(resp_body)
                    data = self._decode_response(msg)

                    received_values = data.get("values", [])
                    # Session ended if we got values OR (server offset > 0 and no ttl/keepalive)
                    session_has_ended = len(received_values) > 0 or (
                        data.get("offset", 0) > 0 and "ttl" not in data
                    )

                    with self.lock:
                        if self.sid != req_sid:
                            # Session changed, ignore response
                            return

                        if session_has_ended:
                            # Remove sent messages
                            sent_count = len(snapshot_queue)
                            del self.outgoing_queue[:sent_count]

                            # Note: self.offset is reset by _start_new_session anyway
                        else:
                            server_offset = data.get("offset", 0)
                            accepted_count = server_offset - self.offset
                            if accepted_count > 0:
                                del self.outgoing_queue[:accepted_count]
                            self.offset = server_offset

                    if session_has_ended:
                        if self.on_data:
                            self.on_data(received_values)
                        self._start_new_session()

            except urllib.error.HTTPError as e:
                text = e.read().decode("utf-8")
                self.on_error(Exception(f"Server error: {e.code} {text}"))
            except Exception as e:
                self.on_error(e)

        except Exception as e:
            self.on_error(e)
        finally:
            self.is_polling = False

    def _decode_response(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        b64_flags = msg.get("b64", 0)
        if b64_flags & 4:
            msg["key"] = self._b64decode_padded(msg["key"]).decode("utf-8")
        if b64_flags & 2:
            msg["sid"] = self._b64decode_padded(msg["sid"]).decode("utf-8")
        if "values" in msg and (b64_flags & 1):
            msg["values"] = [self._b64decode_padded(v) for v in msg["values"]]
        return msg

    def _b64decode_padded(self, s: str) -> bytes:
        # Support both regular base64 and base64url by standardizing to base64url alphabet
        s = s.replace("+", "-").replace("/", "_")
        missing_padding = len(s) % 4
        if missing_padding:
            s += "=" * (4 - missing_padding)
        return base64.urlsafe_b64decode(s)
