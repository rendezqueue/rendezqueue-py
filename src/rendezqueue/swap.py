"""A low-hassle one-shot value swap over the Rendezqueue client.

This wraps RendezqueueClient so a script or agent can send exactly one value,
wait for the peer's reply, and receive it back, without managing sessions,
offsets, or polling by hand.
"""

import argparse
import json
import sys
import threading
from typing import List, Optional

from .client import RendezqueueClient

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_POLL_INTERVAL_MS = 500


def swap_once(
    url: str,
    key: str,
    value: bytes,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    hue: Optional[str] = None,
    poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS,
) -> List[bytes]:
    """Send one value and block until the peer replies (one value swap).

    Args:
        url: Rendezqueue server URL.
        key: Room key (bearer rendezvous selector, not authentication).
        value: The single value to send.
        timeout: Seconds to wait for a reply.
        hue: Client identifier prefix; a default is used when omitted.
        poll_interval_ms: Poll cadence while waiting.

    Returns:
        The list of peer values received (bytes). Empty only if the peer
        replied with no values, which this function does not treat as a
        completed exchange: it keeps waiting, so a non-empty list is the
        normal success result.

    Raises:
        TimeoutError: No reply arrived within `timeout` seconds. The caller
            owns any retry (the 2026-08-15 ruling: 404 delivery is ambiguous
            and is not auto-replayed).
    """
    reply: List[bytes] = []
    last_error: List[str] = []
    done = threading.Event()

    def on_data(values: List[bytes]) -> None:
        reply.extend(values)
        if reply:
            done.set()

    def on_error(e: Exception) -> None:
        # Record but keep waiting: a transient network error recovers on the
        # next poll. A wedged 404 session simply never delivers, so the
        # timeout fires and the caller can retry.
        last_error.append(str(e))

    client = RendezqueueClient(
        url=url,
        key=key,
        hue=hue or "swap",
        on_data=on_data,
        on_error=on_error,
        poll_interval_ms=poll_interval_ms,
    )

    # Send before start so the first poll already carries the value; the raw
    # client polls once immediately on start(), which would otherwise race
    # ahead of this send with an empty queue.
    client.send(value)
    client.start()
    try:
        if not done.wait(timeout):
            suffix = f"; last error: {last_error[-1]}" if last_error else ""
            raise TimeoutError(f"no reply within {timeout}s{suffix}")
        return reply
    finally:
        client.stop()


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="rendezqueue-swap", description="Rendezqueue one-shot value swap"
    )
    parser.add_argument("--url", required=True, help="URL of the Rendezqueue server")
    parser.add_argument("--key", required=True, help="Room key")
    parser.add_argument(
        "--value",
        default=None,
        help="Value to send; if omitted, the value is read from stdin",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Seconds to wait for a reply (default %(default)s)",
    )
    parser.add_argument(
        "--hue",
        default="swap",
        help="Client identifier prefix (default %(default)s)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the reply as one JSON object instead of raw lines",
    )
    args = parser.parse_args()

    if args.value is None:
        value = sys.stdin.buffer.read()
    else:
        value = args.value.encode("utf-8")

    try:
        reply = swap_once(
            url=args.url,
            key=args.key,
            value=value,
            timeout=args.timeout,
            hue=args.hue,
        )
    except TimeoutError as e:
        print(f"Timeout: {e}", file=sys.stderr)
        sys.exit(1)

    if args.json:
        print(
            json.dumps(
                {
                    "values": [
                        v.decode("utf-8", errors="replace")
                        if isinstance(v, bytes)
                        else str(v)
                        for v in reply
                    ]
                }
            )
        )
    else:
        for v in reply:
            if isinstance(v, bytes):
                sys.stdout.buffer.write(v + b"\n")
            else:
                print(v)
        sys.stdout.flush()


if __name__ == "__main__":
    main()
