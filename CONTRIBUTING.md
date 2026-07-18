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

`QuickBaseClient` is the supported high-level client. Preserve its public method names and call signatures unless a documented major release provides a migration path.

## Pull requests

Keep each pull request focused on one reviewable change. Describe:

- The problem being solved
- The direct and downstream effects
- Compatibility or migration concerns
- The checks used to verify the change

Changes require passing automated checks and an approving review before merge.

## Integration testing

Unit tests and HTTP contract tests must not require Quickbase credentials.

Integration tests use a dedicated persistent sandbox application. Configure `QBVISOR_TEST_REALM`, `QBVISOR_TEST_TOKEN`, and `QBVISOR_TEST_APP_ID` locally; these values must never be committed.

Run the established sandbox contract without allowing fixture changes:

```bash
QBVISOR_RUN_INTEGRATION=1 uv run pytest -m integration --no-cov
```

Bootstrap missing persistent fixtures and run mutation contracts only with explicit approval:

```bash
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=1 \
  uv run pytest -m integration --no-cov
```

Mutation tests are outside the default suite. They must verify the configured realm and application ID, use uniquely named temporary resources, and clean up after themselves. Never run integration tests against a production application.

## Quickbase API contract audit

The official Quickbase OAS is cached locally and excluded from version control. Refresh it and update the tracked response manifest with:

```bash
uv run python scripts/audit_quickbase_oas.py --refresh --write
```

Audit the existing cached document without network access with:

```bash
uv run python scripts/audit_quickbase_oas.py
```

Review changes to `docs/api/quickbase-oas-manifest.json` as API contract changes, not generated noise.
