# Contributing to qbvisor

qbvisor is shared infrastructure for developers who build on Quickbase. Changes should improve reliability, performance, idempotency, or developer experience.

## Requirements

- Python 3.12 or 3.13
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
uv sync --all-groups
```

Run the local checks before opening a pull request:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/qbvisor
uv run pytest
uv build
```

## Compatibility

Existing public method names and call signatures should remain stable when practical. Clearly broken behavior may be corrected when the change includes regression tests and release notes.

Use `QuickbaseClient` for new code. `QuickBaseClient` remains supported as a compatibility name until a documented major release removes it.

## Pull requests

Keep each pull request focused on one reviewable change. Describe:

- The problem being solved
- The direct and downstream effects
- Compatibility or migration concerns
- The checks used to verify the change

Changes require passing automated checks and an approving review before merge.

## Integration testing

Unit tests and HTTP contract tests must not require Quickbase credentials.

Integration tests use a dedicated persistent sandbox application. Destructive tests must verify the configured realm and application ID, use uniquely named resources, and require an explicit opt-in environment variable. Never run integration tests against a production application.
