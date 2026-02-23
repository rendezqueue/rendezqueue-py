import threading
import time
import sys
from rendezqueue import RendezqueueClient


def manual_test():
    url = "https://rendezqueue.com/tryswap"
    key = "manual-test-key-" + str(int(time.time()))

    received_event1 = threading.Event()
    received_event2 = threading.Event()

    def on_data1(data):
        print(f"Client 1 received data: {data}")
        received_event1.set()

    def on_data2(data):
        print(f"Client 2 received data: {data}")
        received_event2.set()

    def on_error(err):
        print(f"Error: {err}")

    client1 = RendezqueueClient(
        url=url,
        key=key,
        hue="test-hue-1",
        on_data=on_data1,
        on_error=on_error,
        poll_interval_ms=1000,
    )

    client2 = RendezqueueClient(
        url=url,
        key=key,
        hue="test-hue-2",
        on_data=on_data2,
        on_error=on_error,
        poll_interval_ms=1000,
    )

    print(f"Starting clients with key={key}")
    client1.start()
    client2.start()

    time.sleep(1)  # Give them time to start polling

    # Send values
    print("Client 1 sending value...")
    client1.send("Hello from Client 1")

    print("Client 2 sending value...")
    client2.send("Hello from Client 2")

    print("Waiting for exchange...")

    success = True
    if not received_event1.wait(timeout=10):
        print("Client 1 timed out")
        success = False

    if not received_event2.wait(timeout=10):
        print("Client 2 timed out")
        success = False

    client1.stop()
    client2.stop()

    if success:
        print("Manual test passed!")
    else:
        print("Manual test failed!")
        sys.exit(1)


if __name__ == "__main__":
    manual_test()
