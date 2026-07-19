# qbvisor

qbvisor is a developer-focused Python SDK for using Quickbase as an application backend. It adds
reliable transport behavior, developer-facing field labels, pandas workflows, schema automation,
and verifiable backups on top of the Quickbase JSON API.

Quickbase remains responsible for authentication, permissions, data storage, formula evaluation,
and API limits. qbvisor focuses on the development workflows that are difficult to build safely
from the raw API alone.

> **Project status:** qbvisor `0.3.0` is the first public PyPI release and remains pre-`1.0.0`.
> Review the
> [release and compatibility policy](docs/release-policy.md) before adopting it in production.

## What qbvisor is for

| Developer need | qbvisor workflow |
| --- | --- |
| Analyze or report on application data | Query complete results into pandas DataFrames or run existing reports |
| Move and synchronize records | Upsert by field label with payload-aware batching and explicit partial-failure results |
| Build applications from code | Create resources directly or review and apply a declarative schema plan |
| Protect application data | Capture versioned backups with records, schemas, reports, roles, and attachments |
| Export operational data | Write stable Record ID-ordered CSV exports and download attachments concurrently |
| Diagnose API failures | Use structured exceptions with status, rate-limit, and Quickbase request diagnostics |

The SDK translates developer-facing app, table, and field names to Quickbase IDs at the request
boundary. Quickbase receives stable IDs while application code remains readable. Record reads are
intentionally available as pandas DataFrames because analysis and reporting are primary use cases,
not optional integrations.

## Requirements

- Python 3.12 or later
- A Quickbase realm and user token
- At least one application assigned to that token

CI verifies Python 3.12, 3.13, and 3.14.

## Installation

Install the release from PyPI:

```bash
python -m pip install qbvisor==0.3.0
```

With uv:

```bash
uv add qbvisor==0.3.0
```

For repository development, use [uv](https://docs.astral.sh/uv/) and the committed lockfile:

```bash
git clone https://github.com/ChrisEMetcalf/qbvisor.git
cd qbvisor
uv sync --all-groups
```

Production applications should pin a reviewed version. If a temporary Git dependency is required,
pin its full commit SHA instead of tracking `main`.

## Configuration

Set the Quickbase realm, complete authorization value, and application aliases:

```dotenv
QB_REALM_HOSTNAME=your-realm.quickbase.com
QB_REALM_API_KEY=QB-USER-TOKEN replace-with-your-token
QB_APP_IDS={"Billing":"bp7xxxxxx","Sandbox":"bv75yyyyy"}
```

`QB_APP_IDS` is required. Its keys are readable aliases and its values are Quickbase application
IDs. App aliases are case-insensitive. An application ID can be passed directly only when it is
already present as a value in this mapping. Table names and table IDs are resolved from live
application metadata; field labels are resolved case-insensitively to field IDs.

At import time, qbvisor looks for a `.env` file in its package or project locations and the current
working directory. Existing environment variables are not overwritten. Production deployments
should set secrets through their runtime environment or secret store rather than depending on a
local file.

Tokens should be scoped to the applications needed by the process. Use a dedicated sandbox token
for development and integration tests. Never commit `.env` files, tokens, exported records,
backups, or attachments.

## Quick start

Query a table with field labels and build a Quickbase query without looking up field IDs:

```python
from qbvisor import QueryHelper, QuickBaseClient

with QuickBaseClient() as qb:
    query = QueryHelper(qb, "Billing", "Invoices")
    active = query.and_(
        query.eq("Status", "Active"),
        query.on_or_after("Invoice Date", "2026-01-01"),
    )

    invoices = qb.query_dataframe(
        "Billing",
        "Invoices",
        ["Invoice Number", "Customer", "Amount", "Status"],
        where=active,
    )

print(invoices.head())
```

`query_dataframe()` returns the complete matching result by default. Pass `top` when the caller
requires a hard in-memory limit.

## Common workflows

Run an existing Quickbase report as a DataFrame:

```python
with QuickBaseClient() as qb:
    aging = qb.run_report("Billing", "Invoices", report_id=12)
```

Upsert by a stable unique field so a caller can safely reconcile and retry:

```python
with QuickBaseClient() as qb:
    result = qb.upsert_records(
        "Billing",
        "Invoices",
        [{"Invoice Number": "INV-1042", "Status": "Approved"}],
        merge_field_label="Invoice Number",
        fields_to_return=["Invoice Number", "Status"],
    )

print(result["createdRecordIds"], result["updatedRecordIds"])
```

Create and verify an application backup:

```python
from qbvisor import BackupOptions

with QuickBaseClient() as qb:
    backup = qb.backup_app(
        "Billing",
        "backups",
        options=BackupOptions(attachment_versions="all"),
    )

verification = backup.verify()
print(backup.path, verification.artifact_count)
```

Given an `AppSpec` named `spec`, review its plan and apply that exact plan:

```python
with QuickBaseClient() as qb:
    plan = qb.plan_app(spec)
    print(plan)

    if plan.can_apply:
        result = qb.apply_app(plan)
```

See [Declarative schemas](docs/declarative-schemas.md) for a complete specification, formula-field
behavior, identity rules, state handling, and current non-destructive boundaries.

## Reliability boundaries

- Read-like operations retry temporary connection and gateway failures. Mutations are not replayed
  after an uncertain failure.
- A `429` response is retried only when Quickbase provides a valid `Retry-After` value.
- Complete DataFrame and report reads are materialized in memory. Use `top` for bounded workloads or
  CSV export for larger operational extracts.
- Quickbase does not provide transactional application snapshots. Backups detect record changes
  during capture but cannot guarantee a database-wide point-in-time snapshot.
- Schema apply creates and updates declared resources. It does not delete undeclared Quickbase
  resources.
- Backups and exports can contain sensitive application data even though qbvisor excludes API
  tokens and request headers.

## Logging and errors

Logging is opt-in and uses the standard library logging system:

```python
from qbvisor import LoggingConfigurator

logger = LoggingConfigurator.setup(log_dir="logs", log_level="INFO")
```

API and transport failures raise subclasses of `QuickbaseError`. HTTP failures expose the status,
Quickbase message and description, `Retry-After`, and `qb-api-ray` diagnostic value when available.
Authorization values and request bodies are not included in transport logs or public exception
messages.

## Documentation

- [Documentation site](https://chrisemetcalf.github.io/qbvisor/)
- [Configuration and authentication](docs/configuration.md)
- [Data queries, reports, and record movement](docs/data-workflows.md)
- [Building Quickbase applications](docs/application-building.md)
- [Backups and attachments](docs/backups-and-attachments.md)
- [Logging, errors, and recovery](docs/logging-and-errors.md)
- [Upgrading from 0.2 to 0.3](docs/upgrading-to-0.3.md)
- [Declarative schemas](docs/declarative-schemas.md)
- [Quickbase API coverage and response contract](docs/api/README.md)
- [Changelog](CHANGELOG.md)
- [Release and compatibility policy](docs/release-policy.md)
- [Security policy](SECURITY.md)

## Development

The default quality checks are:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy src/qbvisor
uv run pytest
uv build
uv run twine check --strict dist/*
uv run python scripts/verify_distribution.py dist
```

Live tests require explicit opt-in and a dedicated persistent sandbox. See
[CONTRIBUTING.md](CONTRIBUTING.md) before running them or submitting a change.

qbvisor is licensed under the [MIT License](LICENSE.md). Report vulnerabilities using the private
process in [SECURITY.md](SECURITY.md).
