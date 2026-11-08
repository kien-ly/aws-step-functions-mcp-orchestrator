# Python rules

- Use `uv` only. No `pip`, `poetry`, `pipenv`, `conda`, or `python -m venv`.
- One environment: `.venv/` in the project root, managed by uv.
- `pyproject.toml` is the source of truth. Add packages with `uv add` (or `uv add --dev`).
- Python version is pinned in `.python-version`.
- Run everything through uv: `uv run <cmd>`, `uv sync`.
- Never commit `.venv/`.

## Commands

    uv sync                  # create/update .venv from pyproject.toml and uv.lock
    uv add <package>         # add a runtime dependency
    uv add --dev <package>   # add a dev dependency
    uv remove <package>      # remove a dependency
    uv run python <file>     # run a script
    uv run pytest            # run tests
