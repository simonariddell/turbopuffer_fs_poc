# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a minimal, single-namespace virtual filesystem proof-of-concept built on top of [turbopuffer](https://turbopuffer.com/). It implements filesystem-like operations (`ls`, `cat`, `find`, `grep`, `head`, `tail`, `stat`) that compile to turbopuffer query plans and execute against the remote API.

**Architecture**: Pure Python library (no web framework, no Docker, no CLI entry point). The package lives at the repository root (`/workspace`) with an `__init__.py`.

### Dependencies

- **Python 3.10+** (uses `from __future__ import annotations` union-type syntax)
- **Only external package**: `turbopuffer` (installed via `pip install turbopuffer`)
- **Linting**: `ruff` (installed via `pip install ruff`)

### Importing the package

The workspace root _is_ the package directory. To import it, set `PYTHONPATH=/` so Python can find the `workspace` package:

```bash
PYTHONPATH=/ python3 -c "from workspace import ls_plan, scan_directory"
```

### Running lint

```bash
~/.local/bin/ruff check .
```

The codebase has pre-existing E731 (lambda assignment) and F401 (unused import) warnings. These are intentional style choices by the author; do not auto-fix them.

### Testing

There is no formal test suite. All modules except `live.py` (client construction) and `runtime.py` (query execution against real API) are pure functions testable without network access. To run the full pipeline end-to-end, you need a `TURBOPUFFER_API_KEY` and `TURBOPUFFER_REGION` environment variable.

### Key environment variables

| Variable | Required | Description |
|---|---|---|
| `TURBOPUFFER_API_KEY` | Yes (for live API) | Authentication key for turbopuffer |
| `TURBOPUFFER_REGION` | Yes (for live API) | Region endpoint (e.g. `us-east-1`) |
| `TURBOPUFFER_BASE_URL` | No | Override API base URL |
