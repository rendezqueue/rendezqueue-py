# Development

## Setup

Install PDM as described in [README.md](README.md).
Then install with `--dev` dependencies rather than just `--prod`.

```shell
pdm install --dev
```

## Lint

This project uses `ruff` and `ty` for linting.

```bash
pdm run lint
```
