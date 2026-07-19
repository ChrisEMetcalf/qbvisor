# qbvisor

A Python client for the Quickbase REST API, offering:

- Synchronous HTTP transport with pooled connections, timeouts, and operation-aware retries
- Async data and attachment workflows via `aiohttp`
- In‑memory metadata caching for apps, tables & fields
- High‑level client methods for apps, tables, records, reports, files
- A DSL for building Quickbase formula queries (`QueryHelper`)
- Safe value serialization (`format_query_value`)
- Optional, non‑intrusive logging configuration

qbvisor is developed as public infrastructure for building, querying, moving, and backing up Quickbase applications. The project prioritizes reliability, performance, idempotency, and a direct developer experience.


## Python support

qbvisor supports Python 3.12 and 3.13.

Existing public method names remain stable when practical. Clearly broken behavior may be corrected when the fix is tested and documented.

## Installation

[uv](https://docs.astral.sh/uv/) is used for dependency management and local development.

Clone the repository and install the locked development environment:

```bash
git clone https://github.com/ChrisEMetcalf/qbvisor
cd qbvisor
uv sync --all-groups
```

Install the current package directly from GitHub:

```bash
pip install git+https://github.com/ChrisEMetcalf/qbvisor.git
```

## Configure

Copy `.env.example` to `.env` and set the runtime values:

```dotenv
QB_REALM_HOSTNAME=yourrealm.quickbase.com
QB_REALM_API_KEY=QB-USER-TOKEN xxxxxx_xxxx_x_xxxxxxxxxxxxxxxxxxxxxxxxxxx
QB_APP_IDS={"My App":"bp7xxxxxx","Sandbox":"bpnyyyyyy"}
```


## Quick Start

```python
from qbvisor import QuickBaseClient, QueryHelper
from qbvisor.log_runner import start_logging

start_logging(log_dir="logs", log_level="INFO")

app = "My App"
tbl = "My Table"

with QuickBaseClient() as qb:
    df = qb.query_dataframe(
        app_name=app,
        table_name=tbl,
        select_fields=["Name", "Status", "Date"],
        where="{6.EX.'Active'}",
    )

    query = QueryHelper(qb, app, tbl)
    where = query.and_(
        query.eq("Status", "Active"),
        query.after("Date", "2025-05-13"),
    )
    active_records = qb.query_dataframe(app, tbl, ["Name", "Status"], where)

print(df.head())
print(active_records.head())
```

## Complete DataFrame and report reads

`query_dataframe()` and `run_report()` return the complete requested result by default. They let
Quickbase choose the first response size and continue only when response metadata says more rows
remain. Pass `top` to set an explicit total maximum, or `skip` to begin at a specific offset:

```python
first_500 = qb.query_dataframe(
    "My App",
    "Projects",
    ["Name", "Status"],
    top=500,
)

remaining_report = qb.run_report(
    "My App",
    "Projects",
    report_id=12,
    skip=500,
)
```

Unsorted, ungrouped DataFrame queries use Record ID# continuation so records cannot shift between
fixed offsets. The client selects Record ID# internally and removes it when it was not requested.
Sorted, grouped, explicitly offset, and report requests follow Quickbase's returned `skip`,
`numRecords`, and `totalRecords` metadata because those result shapes cannot use a Record ID
cursor without changing their meaning.

Complete reads are intentionally materialized as pandas DataFrames. Use an explicit `top` when a
bounded in-memory result is required. Reports and sorted or grouped queries do not provide
point-in-time snapshots; changes made while multiple responses are being collected can affect
later offset pages.

## Transport reliability

`QuickBaseTransport` reuses one `requests.Session` and applies a 10-second connect timeout and a 120-second read timeout by default. GET requests and read-like POST operations retry connection failures, timeouts, and temporary gateway responses. Mutations are not replayed after an uncertain failure.

A `429` response is replayed for every operation only when Quickbase provides a valid `Retry-After` value. Missing or invalid guidance raises `QuickbaseRateLimitError` immediately instead of inventing a delay for a mutation.

The transport accepts any valid JSON response. High-level methods enforce the response shape documented for their endpoint. In particular, table, report, and field collection endpoints return top-level lists rather than wrapped objects.

Customize the transport and pass it to the high-level client:

```python
from qbvisor import QuickBaseClient, QuickBaseTransport

with QuickBaseTransport(timeout=(5.0, 60.0), max_attempts=4) as transport:
    qb = QuickBaseClient(transport=transport)
    app = qb.get_app("My App")
```

Failed requests raise qbvisor exceptions instead of raw `requests` errors. HTTP errors expose the status, Quickbase message and description, `Retry-After`, and `qb-api-ray` diagnostic ID when present:

```python
from qbvisor import QuickBaseClient, QuickbaseHTTPError

try:
    with QuickBaseClient() as qb:
        qb.get_app("My App")
except QuickbaseHTTPError as error:
    print(error.status_code, error.qb_api_ray)
```

Request credentials and bodies are not included in transport logs or exception messages.

## Application backups

`backup_app()` creates a portable, versioned snapshot containing application metadata, events,
roles, table schemas, relationships, report definitions, records, and file attachments. The
snapshot is built in a private staging directory and published under the destination only after
every requested artifact and the final manifest have been written.

```python
from qbvisor import BackupOptions, QuickBaseClient

with QuickBaseClient() as qb:
    backup = qb.backup_app(
        "My App",
        "backups",
        options=BackupOptions(
            attachment_versions="all",
            page_size=1000,
            max_attachment_concurrency=4,
        ),
    )

verification = backup.verify()
projects = backup.table_dataframe("Projects")
print(backup.path, verification.artifact_count, projects.head())
```

Records are stored as JSON Lines keyed by stable field IDs. `table_dataframe()` applies the field
labels captured in the same snapshot, preserving the normal analyst-facing workflow without making
pandas the archive format. Attachment paths use table, record, field, and version IDs; the original
filename and Quickbase version metadata remain in each table's `attachments.jsonl` index.

`attachment_versions` accepts `"all"` (the default), `"latest"`, or `"none"`. Every stored file and
metadata artifact has a SHA-256 digest and byte count in `manifest.json`. `verify()` recomputes those
values, checks JSON/JSONL item counts, rejects untracked files and symbolic links, and validates the
attachment index. Open an existing snapshot with `ApplicationBackup.open(path)`.

Quickbase does not expose a transactional application snapshot. qbvisor records the UTC start and
completion times and calls `records_modified_since()` after capture for every table. If changes are
found, the completed manifest is marked `consistent: false` and lists the affected table IDs. Set
`fail_on_changes=True` to discard that run and raise `BackupConsistencyError` instead. This is a
conservative record-change check, not a database transaction: schema changes are not detected, and
deleted-record detection depends on Quickbase's **Index record changes** setting.

Backups contain application data, user/role metadata, and possibly sensitive attachments. They do
not contain the API token or request headers, but the destination still needs access controls and a
retention policy appropriate for the source application. Compression, encryption, retention,
incremental capture, and restore automation are intentionally outside the first backup format.

## Declarative application schemas

Define an application with stable resource keys, review a read-only plan, and apply that exact plan
after it is understood. Display names remain normal Quickbase names; keys such as `projects` and
`project_details` provide the durable identity used for renames and drift detection.

```python
from qbvisor import (
    AppSpec,
    FieldSpec,
    FormulaSpec,
    QuickBaseClient,
    RelationshipSpec,
    SummaryFieldSpec,
    TableSpec,
)

spec = AppSpec(
    key="operations",
    name="Operations",
    tables=[
        TableSpec(
            key="projects",
            name="Projects",
            fields=[
                FieldSpec(key="name", label="Project Name", field_type="text"),
                FieldSpec(key="budget", label="Budget", field_type="currency"),
                FieldSpec(key="tax_rate", label="Tax Rate", field_type="percent"),
                FieldSpec(
                    key="budget_with_tax",
                    label="Budget with Tax",
                    field_type="currency",
                    formula=FormulaSpec(
                        expression="[Budget] * (1 + [Tax Rate])",
                        depends_on=(
                            "tables.projects.fields.budget",
                            "tables.projects.fields.tax_rate",
                        ),
                    ),
                ),
            ],
        ),
        TableSpec(
            key="details",
            name="Project Details",
            fields=[
                FieldSpec(key="hours", label="Hours", field_type="numeric"),
            ],
        ),
    ],
    relationships=[
        RelationshipSpec(
            key="project_details",
            parent_table="projects",
            child_table="details",
            foreign_key_label="Related Project",
            lookup_fields=["name"],
            summary_fields=[
                SummaryFieldSpec(
                    key="total_hours",
                    accumulation_type="SUM",
                    field="hours",
                    label="Total Hours",
                )
            ],
        )
    ],
)

with QuickBaseClient() as qb:
    plan = qb.plan_app(spec)
    print(plan)

    if plan.can_apply:
        result = qb.apply_app(plan)
        print(result.quickbase_change_count, result.state.serial)
```

`plan_app()` makes GET requests but does not change Quickbase or write state. `apply_app()` accepts
the reviewed `SchemaPlan`, checks that neither Quickbase nor local state changed after planning,
applies only the declared differences, verifies convergence, and then atomically publishes
`.qbvisor/state.json`. A second plan should be unchanged.

`FormulaSpec` accepts Quickbase formula syntax directly. `depends_on` uses stable schema addresses
to order formula fields, relationships, lookups, and summaries before mutation. Quickbase remains
responsible for parsing and type-checking the expression. Formula-query functions are identified in
the plan with a performance warning.

Existing resources are imported by a unique case-insensitive name match. After that first apply,
stored Quickbase IDs are authoritative, so display names can be changed safely without replacing
resources. Missing bound IDs, ambiguous imports, and field-type changes are conflicts rather than
implicit recreation. Remote tables and fields not declared in the specification are left alone;
deletion is not part of the current workflow.

The state file contains resource addresses, Quickbase IDs, display names, and limited managed
metadata. It does not contain credentials or record data, but it is operationally important and
should not be casually deleted. `.qbvisor/` is ignored by default; CI and team workflows should
persist the selected state path through an access-controlled artifact or state store appropriate
for the project.

Planning does not query records. It reads the app, the table collection, fields for each declared
existing table, and relationships for each declared existing child table. Its cost therefore grows
with the managed schema, not with record count.

See [Declarative schemas](docs/declarative-schemas.md) for import semantics, managed attributes,
state behavior, conflicts, and operational guidance.

## Application and schema inspection

The client exposes Quickbase's current app-event, app-role, and field-usage endpoints without
changing their documented response shapes:

```python
with QuickBaseClient() as qb:
    events = qb.get_app_events("My App")
    roles = qb.get_app_roles("My App")
    field_usage = qb.get_fields_usage("My App", "Projects", top=100)
    status_usage = qb.get_field_usage("My App", "Projects", "Status")
```

Existing relationships can be extended with parent lookup fields and child summary fields. Labels
are resolved to stable field IDs immediately before the request; numeric IDs remain valid for
automation that already tracks schema identifiers:

```python
from qbvisor import RelationshipSummary

with QuickBaseClient() as qb:
    relationship = qb.update_relationship(
        "My App",
        "Project Details",
        "Related Project",
        lookup_fields=["Project Name", "Owner"],
        summary_fields=[
            RelationshipSummary("SUM", "Hours", label="Total Hours"),
            RelationshipSummary("COUNT", label="Detail Count"),
        ],
    )
```

`COUNT` summaries omit the source field, as required by Quickbase. Other accumulation types require
a child-table field. Successful table, field, and relationship mutations invalidate the affected
metadata so schema changes are visible on the next access without sacrificing repeated label
resolution performance.

## Change tracking and attachment cleanup

Use `records_modified_since()` when incremental synchronization needs to include changes found
through selected field dependencies. The timestamp must be timezone-aware and is normalized to
whole-second ISO-8601 UTC. Fractional seconds are truncated so the comparison remains conservative
without skipping changes:

```python
changes = qb.records_modified_since(
    "My App",
    "Projects",
    "2026-07-01T00:00:00Z",
    field_list=["Owner", "Status"],
    include_details=True,
)
```

`delete_file()` removes one attachment version. A version is always explicit; Quickbase's special
version `0` value selects the latest version. File deletion is a mutation and is not replayed after
an uncertain connection, timeout, or gateway failure.

```python
deleted = qb.delete_file("My App", "Projects", record_id=42, field="Attachment", version_number=2)
```

## Record writes

`upsert_records()` accepts developer-facing field labels and sends Quickbase field IDs. Its result
keeps the existing `success`, `createdRecordIds`, and `totalProcessed` keys while preserving the
complete documented write outcome:

```python
result = qb.upsert_records(
    "My App",
    "Projects",
    [{"Project Key": "migration-42", "Status": "Active"}],
    merge_field_label="Project Key",
    fields_to_return=["Project Key", "Status"],
)

returned_record = result["data"][0]
updated_ids = result["updatedRecordIds"]
```

`data` retains Quickbase's native field-ID cell structure and includes Record ID# when return
fields are requested. `updatedRecordIds` and `unchangedRecordIds` distinguish writes from records
that already held the submitted values. A partial `207` response returns `success=False`,
`partial=True`, and one-based `lineErrors` while retaining every successful outcome from the same
request. Malformed or incomplete success responses raise `QuickbaseResponseError`.

Record mutations are not automatically retried after uncertain connection failures. Use a stable,
unique `merge_field_label` when a caller may retry an operation; retrying append-only writes can
create duplicate records. `upsert_records()` currently sends one request and therefore remains
subject to Quickbase's 40 MB upsert payload limit. Payload-aware batching is intentionally handled
as a separate feature because several requests cannot provide transaction-level atomicity.

## Record exports and concurrent attachments

`download_records_to_csv()` scans records in stable Record ID# order and writes each page to a
temporary CSV before moving the completed export into place. `chunk_size` controls each page and
is capped at Quickbase's 1,000-record maximum. `record_limit` is exact. The existing
`max_concurrency` argument remains accepted for compatibility, but record pages are fetched
sequentially so inserts or deletes cannot cause fixed offsets to skip or duplicate records. Query
pages use the same timeout, rate-limit, retry, exception, and diagnostic rules as other read
operations.

The existing `download_attachments_async()` and `download_table_attachments_async()` names are
preserved for compatibility. They are synchronous entry points that use bounded asynchronous I/O
internally and return a list of result dictionaries. Each result retains `record_id`, `file_name`,
and `saved_path`, and adds a `status`:

Matching records are discovered in stable Record ID# order. `page_size` limits each query but does
not limit the total scan, and a response shorter than the requested page is not treated as
completion when Quickbase reports more records. Each populated file field queues its highest
attachment version, preserving the existing latest-version behavior. Invalid attachment metadata
raises `QuickbaseResponseError` before downloads begin instead of guessing a version.

- `downloaded` includes `bytes_written`.
- `skipped` means the destination already existed and was not overwritten.
- `failed` includes a safe error message in `QuickbaseBatchError.results`.

If one or more files fail, independent downloads finish and the method raises
`QuickbaseBatchError`. Its `results` attribute contains every item outcome, while `errors` retains
the original structured exceptions. Files are written through a temporary path and moved into
place only after the complete response has been saved.

Single-field downloads preserve the existing `recordId_filename` layout. Whole-table downloads
use `recordId_fieldId_filename` so identical filenames in different file fields cannot collide.
Quickbase-provided filenames are sanitized before they are joined to the destination directory.

`download_attachment_base64()` returns `None` only when the requested record or attachment version
does not exist. Transport, authentication, rate-limit, and server failures raise qbvisor
exceptions. The returned string is always base64 encoded from the resolved file bytes.

Because the compatibility methods call `asyncio.run()`, they cannot be invoked from a thread that
already has a running event loop. A native public async interface is intentionally deferred rather
than changing the return type of an existing method.

## Module overview

- `QuickBaseClient`

    All high-level methods for apps, tables, fields, reports, records, attachments.

- `QuickBaseTransport`

    Synchronous session, timeout, retry, JSON and file response parsing, and error handling.

- `QueryHelper`

    Build Quickbase formula queries by field label  → `{fid.OP.val}` strings.

- `LoggingConfigurator` + `get_logger`

    Lightweight logging API, opt-in file & console handlers with rotation.

- `helpers`

    Utility routines (e.g. `sanitize_filenames`, `ensure_temp_dir`, etc.).


## Testing

Run all tests with:

```bash
uv run pytest
```

Live integration tests use a dedicated persistent sandbox and skip unless explicitly enabled. Set `QBVISOR_TEST_REALM`, `QBVISOR_TEST_TOKEN`, and `QBVISOR_TEST_APP_ID` in `.env`, then run the established contract without allowing changes:

```bash
QBVISOR_RUN_INTEGRATION=1 uv run pytest -m integration --no-cov
```

The first bootstrap and the mutation contracts require a second opt-in. They create only the named persistent fixtures and uniquely named temporary resources inside the configured sandbox:

```bash
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=1 \
  uv run pytest -m integration --no-cov
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete development and compatibility policy.
