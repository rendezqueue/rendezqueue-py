# rendezqueue-py

A rendezvous service to exchange queued values.
A Python implementation of the [main JavaScript rendezqueue project](https://github.com/rendezqueue/rendezqueue).

## Setup

### PDM Prereq

Install `pdm` on your machine via `apt install pdm`.
Then install package dependencies for this project, omitting the ones needed for development (see [CONTRIBUTING.md](CONTRIBUTING.md) for that).

```shell
pdm install --prod
```

If `pdm` is not available directly, then try installing it via `uv` like `uv tool install pdm`.
Then you can run `pdm` naturally if `~/.local/bin/` is in your `$PATH`, or you can alternatively run it like `uv tool run pdm`.

## Run

```shell
pdm run rendezqueue-server --http_port 15050
```

This serves PlySwap at `/`; `--http_path` selects a different full path.
To also serve TrySwap, pass `--tryswap_http_path /tryswap`.

```shell
pdm run rendezqueue-swap --url http://127.0.0.1:15050/ --room example --value hello
```

Run a peer with the same room to exchange values.
Client URLs are full endpoints; they are not rewritten automatically.

## Wire fields

PlySwap JSON uses `room`, `self`, `peer`, `next`, and `values`, with optional `b64`.
The room scopes the exchange identified by `(self, peer)`.
Request `next` advertises our successor; reply `self` echoes the requester and reply `next` supplies the peer's successor.
Supply a nonempty `room` exactly once: in JSON or as `?room=...`.
The `b64` bitmap is unchanged: values=1, IDs=2, room=4; unflagged bytes use Latin-1 and flagged bytes use base64.

Python uses `room`, `self_xid`, `peer_xid`, and `next_xid`; the XID fields are `self`, `peer`, and `next` on the wire.
Actual XIDs are nonempty bytes. Empty/omitted `peer` and `next` mean absent and encode by omission; empty bytes inside `values` remain valid data.
Old model, SDK, and wire names are not aliases. Opt-in TrySwap retains its original format.

## Client scheduling

`poll_interval_ms` paces polling for peer changes and retries after failures.

## Commands that continue later

Use `RendezqueueClient(..., continuable=True)` for a command that exits between turns.
After its application result, call `cursor = client.finish()` from the main thread and print `"Continue with: " + cursor.arguments()`.
The next process supplies the decoded `self_xid` and `peer_xid` to the constructor, with `continuable=True` and the same endpoint/room.

`finish()` freezes new input, keeps retrying retained snapshots until the peer advances, and waits for owned callbacks/I/O before returning.
It certifies protocol receipt, not durable application processing.
Use the latest pair only once and never concurrently.
An interrupted command or failed handoff produces no safe continuation; its earlier IDs are not a recovery checkpoint.
`stop()` alone does not certify a handoff.

## Development

```shell
pdm install --dev
pdm run lint
pdm run test
```
