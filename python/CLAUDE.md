# CLAUDE.md

This file points Claude Code (and other agents) at the project guidance. The full contributor and agent guide lives in **[AGENTS.md](AGENTS.md)** — read it first.

@AGENTS.md

## Quick reference

Work from `python/`. This subproject uses [uv](https://docs.astral.sh/uv/):

```bash
uv sync --extra dev
uv run pytest -q
uv run ruff check src tests examples
uv run ruff format --check src tests examples
```

Before pushing, run the same four checks CI runs (lint, format, tests, wheel build) across Python 3.9–3.12. See [AGENTS.md](AGENTS.md) for layout, conventions, and gotchas, and [ARCHITECTURE.md](ARCHITECTURE.md) for how a job gets submitted.
