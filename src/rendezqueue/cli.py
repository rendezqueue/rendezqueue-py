import argparse
import sys
import threading
import time
from typing import List
from .client import RendezqueueClient


def main() -> None:
    parser = argparse.ArgumentParser(description="Rendezqueue Client")
    parser.add_argument("--url", required=True, help="URL of the Rendezqueue server")
    parser.add_argument("--key", required=True, help="Session key")
    parser.add_argument("--hue", default="cli", help="Client hue (ID prefix)")

    args = parser.parse_args()

    stop_event = threading.Event()

    def on_data(values: List[bytes]) -> None:
        for v in values:
            try:
                # Try to decode as utf-8, fallback to repr or raw bytes
                print(v.decode("utf-8"))
            except UnicodeDecodeError:
                print(v)
        sys.stdout.flush()

    def on_error(e: Exception) -> None:
        print(f"Error: {e}", file=sys.stderr)

    client = RendezqueueClient(
        url=args.url, key=args.key, hue=args.hue, on_data=on_data, on_error=on_error
    )

    try:
        client.start()

        # Reader thread
        def read_stdin() -> None:
            try:
                for line in sys.stdin:
                    if stop_event.is_set():
                        break
                    # Strip newline
                    msg = line.rstrip("\n")
                    client.send(msg)
            except Exception:
                # stdin closed or error
                pass
            finally:
                pass

        input_thread = threading.Thread(target=read_stdin, daemon=True)
        input_thread.start()

        # Main loop to keep the main thread alive and monitor stop_event
        while not stop_event.is_set():
            time.sleep(0.1)

    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        client.stop()


if __name__ == "__main__":
    main()
