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

## Module overview

- `QuickBaseClient`

    All high-level methods for apps, tables, fields, reports, records, attachments.

- `QuickBaseTransport`

    Synchronous session, timeout, retry, response parsing, and error handling.

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
