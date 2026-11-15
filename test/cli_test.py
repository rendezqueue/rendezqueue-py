"""The example CLI streams stdio; process continuation belongs to other clients."""

import io
import os
from pathlib import Path
import select
import signal
import subprocess
import sys
import threading
import uuid
from typing import Optional

import pytest

from rendezqueue import cli
from rendezqueue.client import RendezqueueClient
from rendezqueue.continuation import Continuation
from plyswap_http_test import PATH, server as server


@pytest.fixture
def fake_client(monkeypatch):
    instances = []

    class Client:
        finish_error: Optional[BaseException] = None
        payload = [b"peer", b"\xff"]
        notify_transport_error = False

        def __init__(self, **kwargs):
            self.options = kwargs
            self.sent = []
            self.stopped = False
            self.started = False
            instances.append(self)

        def start(self):
            self.started = True

        def send(self, value):
            self.sent.append(value)

        def finish(self):
            if self.finish_error is not None:
                raise self.finish_error
            if self.notify_transport_error:
                self.options["on_error"](OSError("retryable transport error"))
            self.options["on_data"](self.payload)
            # The SDK returns a cursor, but the stdio example must not expose it.
            return Continuation(b"next", b"peer-next")

        def stop(self):
            self.stopped = True

    monkeypatch.setattr(cli, "RendezqueueClient", Client)
    return Client, instances


def arguments(monkeypatch, *extra):
    monkeypatch.setattr(
        sys,
        "argv",
        ["rendezqueue-client", "--url", "http://example/", "--room", "room", *extra],
    )


def test_eof_drains_and_only_writes_received_data(fake_client, monkeypatch, capsys):
    _, instances = fake_client
    arguments(monkeypatch)
    monkeypatch.setattr(sys, "stdin", io.StringIO("hello\n\n"))
    assert cli.main() == 0
    client = instances[0]
    assert "self_xid" not in client.options and "peer_xid" not in client.options
    assert client.sent == ["hello", ""] and client.started and client.stopped
    output = capsys.readouterr()
    assert output.out.splitlines() == ["peer", "b'\\xff'"]
    assert not output.err


def test_first_ctrl_c_requests_graceful_shutdown(fake_client, monkeypatch, capsys):
    _, instances = fake_client

    class Interrupted:
        def __iter__(self):
            raise KeyboardInterrupt

    arguments(monkeypatch)
    monkeypatch.setattr(sys, "stdin", Interrupted())
    assert cli.main() == 0
    assert instances[0].stopped
    assert capsys.readouterr().out.splitlines() == ["peer", "b'\\xff'"]


@pytest.mark.parametrize(
    "error,code",
    [
        (TimeoutError("deadline"), 1),
        (RuntimeError("failed request"), 1),
        (KeyboardInterrupt(), 130),
    ],
)
def test_failed_or_interrupted_shutdown_is_not_success(
    fake_client, monkeypatch, capsys, error, code
):
    client_class, instances = fake_client
    client_class.finish_error = error
    arguments(monkeypatch)
    monkeypatch.setattr(sys, "stdin", io.StringIO("hello\n"))
    assert cli.main() == code
    output = capsys.readouterr()
    assert not output.out and output.err
    assert "--self" not in output.err and "--peer" not in output.err
    assert instances[0].stopped


def test_failed_unicode_render_cannot_report_success(fake_client, monkeypatch, capsys):
    client_class, instances = fake_client
    client_class.payload = ["é".encode()]
    sink = io.BytesIO()
    stdout = io.TextIOWrapper(sink, encoding="ascii", write_through=True)
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stdin", io.StringIO("hello\n"))
    arguments(monkeypatch)
    assert cli.main() == 1
    assert not sink.getvalue()
    assert "received output failed" in capsys.readouterr().err
    assert instances[0].stopped


def test_failed_payload_flush_cannot_be_hidden(fake_client, monkeypatch, capsys):
    class FirstFlushFails(io.StringIO):
        failed = False

        def flush(self):
            if not self.failed:
                self.failed = True
                raise OSError("payload flush failed")
            return super().flush()

    stdout = FirstFlushFails()
    monkeypatch.setattr(sys, "stdout", stdout)
    monkeypatch.setattr(sys, "stdin", io.StringIO("hello\n"))
    arguments(monkeypatch)
    assert cli.main() == 1
    assert stdout.getvalue().splitlines() == ["peer", "b'\\xff'"]
    assert "received output failed" in capsys.readouterr().err


def test_retryable_transport_error_is_not_a_rendering_failure(
    fake_client, monkeypatch, capsys
):
    client_class, _ = fake_client
    client_class.notify_transport_error = True
    monkeypatch.setattr(sys, "stdin", io.StringIO("hello\n"))
    arguments(monkeypatch)
    assert cli.main() == 0
    output = capsys.readouterr()
    assert output.out.splitlines() == ["peer", "b'\\xff'"]
    assert "retryable transport error" in output.err


@pytest.mark.parametrize(
    "extra", [["--self", "YQ"], ["--peer", "Yg"], ["--self", "YQ", "--peer", "Yg"]]
)
def test_resume_options_are_not_part_of_the_example_cli(
    fake_client, monkeypatch, extra
):
    _, instances = fake_client
    arguments(monkeypatch, *extra)
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 2
    assert not instances


def test_help_describes_stdio_not_continuation(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["rendezqueue-client", "--help"])
    with pytest.raises(SystemExit) as error:
        cli.main()
    assert error.value.code == 0
    output = capsys.readouterr().out
    assert "stdin" in output and "EOF" in output
    assert "--self" not in output and "--peer" not in output


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGINT and pipe readiness")
@pytest.mark.parametrize("interrupt", [False, True])
def test_real_cli_streams_until_eof_or_sigint(server, interrupt):
    url = f"http://127.0.0.1:{server.server_address[1]}{PATH}"
    room = "cli-" + uuid.uuid4().hex
    received, errors = [], []
    first_input_received = threading.Event()

    def receive(values):
        received.extend(values)
        if received == [b"one"]:
            first_input_received.set()

    poll_interval_ms = 500
    peer = RendezqueueClient(
        url,
        room,
        "host",
        on_data=receive,
        on_error=errors.append,
        poll_interval_ms=poll_interval_ms,
    )
    peer.send(b"host-one")
    peer.start()
    source = Path(__file__).resolve().parents[1] / "src"
    # Pace both sides moderately. Synchronize on data/process events rather than
    # compensating for timing-dependent tests with aggressive polling.
    bootstrap = f"""
import runpy
import rendezqueue.client as sdk

original_client = sdk.RendezqueueClient

def paced_client(*args, **kwargs):
    kwargs["poll_interval_ms"] = {poll_interval_ms}
    return original_client(*args, **kwargs)

sdk.RendezqueueClient = paced_client
runpy.run_module("rendezqueue.cli", run_name="__main__")
"""
    process = subprocess.Popen(
        [sys.executable, "-c", bootstrap, "--url", url, "--room", room],
        env=dict(os.environ, PYTHONPATH=str(source)),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    def send(value):
        assert process.stdin is not None
        process.stdin.write(value)
        process.stdin.flush()

    def read_line(expected):
        assert process.stdout is not None
        ready, _, _ = select.select([process.stdout], [], [], 25)
        assert ready, "client did not receive the peer's data"
        assert process.stdout.readline() == expected
        assert process.poll() is None, "client stopped before EOF"

    try:
        send(b"one\n")
        read_line(b"host-one\n")
        assert first_input_received.wait(10), errors
        if interrupt:
            # One exchange is enough to reach a real blocked stdin read.
            process.send_signal(signal.SIGINT)
            expected_input, expected_output = [b"one"], b""
        else:
            peer.send(b"host-two")
            send(b"two\n")
            # Close without waiting for the second reply; queued input must drain.
            assert process.stdin is not None
            process.stdin.close()
            process.stdin = None
            expected_input, expected_output = [b"one", b"two"], b"host-two\n"
        process.wait(timeout=40)
        output, stderr = process.communicate(timeout=5)
        assert process.returncode == 0, stderr.decode()
        assert output == expected_output and not stderr
        assert received == expected_input and not errors
    finally:
        if process.poll() is None:
            process.terminate()
        process.communicate(timeout=5)
        peer.stop()
        assert peer.poll_thread is not None
        peer.poll_thread.join(12)
        assert not peer.poll_thread.is_alive()
