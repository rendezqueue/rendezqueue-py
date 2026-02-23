# rendezqueue-py

A rendezvous service to exchange queued values.
This is meant to be a Python version of the [main JavaScript rendezqueue project](https://github.com/rendezqueue/rendezqueue).

## Setup

### PDM Prereq

Install `pdm` on your machine via `apt install pdm`.
Then install package dependencies for this project, omitting the ones needed for development (see [CONTRIBUTING.md](CONTRIBUTING.md) for that).

```shell
pdm install --prod
```

If `pdm` is not available directly, then try installing it via `uv` like `uv tool install pdm`.
Then you can run `pdm` naturally if `~/.local/bin/` is in your `$PATH`, or you can alternatively run it like `uv tool run pdm`.
