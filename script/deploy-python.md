# Python Release

This document covers the manual release flow for the `datalevin` Python wheel.

## Scope

This script builds and optionally uploads the universal wheel. It is useful for
local dry runs and explicit manual releases. The GitHub Actions workflow builds
the same wheel and smoke-tests it on Linux/macOS/Windows, but it does not
publish to PyPI or TestPyPI.

## Prerequisites

- Python 3.10+
- Clojure CLI
- A Python environment with `pip`
- A PyPI token or TestPyPI token

Typical credentials:

```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=...
```

## Dry Run

Build the universal wheel and run the smoke test:

```bash
./script/deploy-python --dry-run
```

The packaging scripts use `PYTHON_BIN` when set, otherwise the existing
`bindings/python/.venv` interpreter, then `python` or `python3` from `PATH`.
Use `--python` to override that selection. Choose an interpreter with a JPype
wheel for your platform; for example, [JPype 1.7.1's release files](https://pypi.org/project/jpype1/1.7.1/#files)
include macOS wheels for Python 3.14 but not 3.15:

```bash
./script/deploy-python --dry-run --python python3.14
```

Skip the smoke test if you only want the wheel build:

```bash
./script/deploy-python --dry-run --skip-smoke
```

## Publish

Upload to PyPI:

```bash
./script/deploy-python
```

Upload to TestPyPI:

```bash
./script/deploy-python --testpypi
```
