"""Exchange stdin lines with a peer and write received values to stdout."""

import argparse
import sys
from typing import List, Optional

from .client import RendezqueueClient


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Rendezqueue Client",
        epilog=(
            "Send stdin lines and print incoming peer values until EOF or Ctrl-C. "
            "On exit, finish queued input before stopping. "
            "This does not wait for an application reply."
        ),
    )
    parser.add_argument("--url", required=True, help="Full PlySwap endpoint URL")
    parser.add_argument("--room", required=True, help="Room")
    parser.add_argument("--hue", default="cli", help="Client hue (ID prefix)")
    args = parser.parse_args()

    output_error: Optional[Exception] = None

    def on_data(values: List[bytes]) -> None:
        nonlocal output_error
        try:
            for value in values:
                try:
                    print(value.decode("utf-8"))
                except UnicodeDecodeError:
                    print(value)
            sys.stdout.flush()
        except Exception as error:
            # SDK callback errors do not roll back receipt. Do not silently
            # report success if output was lost. Transport retries stay nonfatal.
            if output_error is None:
                output_error = error

    def on_error(error: Exception) -> None:
        print(f"Error: {error}", file=sys.stderr)

    client = RendezqueueClient(
        url=args.url,
        room=args.room,
        hue=args.hue,
        on_data=on_data,
        on_error=on_error,
        continuable=True,
    )
    try:
        client.start()
        # Polling already has its own worker. Read input on the main thread so
        # EOF and Ctrl-C need no blocked daemon reader or extra shutdown race.
        try:
            for line in sys.stdin:
                client.send(line.rstrip("\n"))
        except KeyboardInterrupt:
            pass  # First Ctrl-C requests graceful shutdown.
        # Reuse the SDK's drain barrier, without exposing process continuation.
        client.finish()
        if output_error is not None:
            raise RuntimeError(
                "received output failed: " + ascii(output_error)
            ) from output_error
        return 0
    except TimeoutError:
        print(
            "Timed out finishing queued input; delivery may have occurred.",
            file=sys.stderr,
        )
        return 1
    except (RuntimeError, OSError, ValueError, KeyboardInterrupt) as error:
        print(f"Client stopped: {error}", file=sys.stderr)
        return 130 if isinstance(error, KeyboardInterrupt) else 1
    finally:
        client.stop()


if __name__ == "__main__":
    raise SystemExit(main())
