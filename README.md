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
a child-table field. Successful relationship updates invalidate the affected field metadata so the
new lookup and summary fields are visible on the next access.

## Change tracking and attachment cleanup

Use `records_modified_since()` when incremental synchronization needs to include changes found
through selected field dependencies. The timestamp must be timezone-aware and is normalized to
ISO-8601 UTC:

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

## Concurrent exports and attachments

`download_records_to_csv()` reads records concurrently in Quickbase's maximum 1,000-record
pages. `chunk_size` controls each page and is capped at 1,000 without skipping offsets;
`max_concurrency` bounds the number of requests in flight. Query pages use the same timeout,
rate-limit, retry, exception, and diagnostic rules as other read operations.

The existing `download_attachments_async()` and `download_table_attachments_async()` names are
preserved for compatibility. They are synchronous entry points that use bounded asynchronous I/O
internally and return a list of result dictionaries. Each result retains `record_id`, `file_name`,
and `saved_path`, and adds a `status`:

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
