"""A low-hassle one-shot value swap over the Rendezqueue client.

This wraps RendezqueueClient so a script or agent can send exactly one value,
wait for the peer's reply, and receive it back, without managing sessions,
protocol generations, retained retries, or polling by hand.
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
    room: str,
    value: bytes,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    hue: Optional[str] = None,
    poll_interval_ms: int = DEFAULT_POLL_INTERVAL_MS,
) -> List[bytes]:
    """Send one value and block until the peer replies (one value swap).

    Returning stops this client, including its retained recovery participation.
    The other peer can retrieve our sealed values while cached, but cache loss
    or expiry after one side exits can prevent the other from finishing.

    Args:
        url: Full PlySwap endpoint URL.
        room: Bearer rendezvous selector, not authentication.
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
            owns any application-level retry; an error or timeout never
            proves nondelivery. Protocol retries retain the original snapshots.
    """
    reply: List[bytes] = []
    last_error: List[str] = []
    done = threading.Event()

    def on_data(values: List[bytes]) -> None:
        reply.extend(values)
        if reply:
            done.set()

    def on_error(e: Exception) -> None:
        # Keep waiting through explicit retained protocol retries. Never
        # convert a 404/timeout into a fresh application-level resend.
        last_error.append(str(e))

    client = RendezqueueClient(
        url=url,
        room=room,
        hue=hue or "swap",
        on_data=on_data,
        on_error=on_error,
        poll_interval_ms=poll_interval_ms,
    )

    # Queue before scheduling. OPEN carries no data; the retained core freezes
    # this batch only after discovery binds a peer. Stopping after our receipt
    # ends recovery participation: the peer may still need its cached answer.
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
    parser.add_argument("--url", required=True, help="Full PlySwap endpoint URL")
    parser.add_argument("--room", required=True, help="Room")
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
            room=args.room,
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
