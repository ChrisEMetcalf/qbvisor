# qbvisor

A Python Client for the Quickbase REST API, offering:


- Async HTTP transport with retries (via `aiohttp`)
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

1. **Clone** and **Install**:

```
git clone https://github.com/ChrisEMetcalf/qbvisor
cd qbvisor
uv sync --all-groups
```

2. Install from **GitHub**
```bash
pip install git+https://github.com/ChrisEMetcalf/qbvisor.git
```

## Configure

Create a `.env` in the repo root:

```
QB_REALM_HOSTNAME=yourrealm.quickbase.com
QB_REALM_API_KEY=QB-USER-TOKEN xxxxxx_xxxx_x_xxxxxxxxxxxxxxxxxxxxxxxxxxx
QB_APP_IDS={"My App":"bp7xxxxxx","Sandbox":"bpnyyyyyy"}
```


## Quick Start

```python
from qbvisor import QuickBaseClient, QueryHelper
from qbvisor.log_runner import start_logging, get_logger

# (Optional) configure logging in your script:
start_logging(
    log_dir="logs",
    log_level="DEBUG"
)

log = get_logger(__name__)

# Instantiate client
qb = QuickBaseClient()

app = "My App"
tbl = "My Table"

#1. Query into a DataFrame
df = qb.query_dataframe(
    app_name=app,
    table_name=tbl,
    select_fields=["Name", "Status", "Date"],
    where="{6.EX.'Active'}"
)
print(df.head())

# 2. Build a formula with QueryHelper
q = QueryHelper(qb, app, tbl)
where = q.and_(
    q.eq("Status", "Active"),
    q.after("Date", "2025-05-13")
)
query_df = qb.query_dataframe(app, tbl, ["Name", "Status"], where)
print(query_df)

# 3. Upsert one record
res = qb.upsert_records(
    app_name=app,
    table_name=tbl,
    records=[{"Name": "John Doe", "Status": "Active"}],
    merge_field_label="Name"
)
print(res)

# 4. Export all records to CSV
out = qb.download_records_to_csv(
    app_name=app,
    table_name=tbl,
    where=where,
    output_dir="data/exports"
)
print("CSV saved to", out)
```

## Module Overview

* `QuickBaseClient`

    All high-level methods for apps, tables, fields, reports, records, attachments.

* `QueryHelper`

    Build Quickbase formula queries by field label  → `{fid.OP.val}` strings.

* `LoggingConfigurator` + `get_logger`

    Lightweight logging API, opt-in file & console handlers with rotation.

* `helpers`

    Utility routines (e.g. `sanitize_filenames`, `ensure_temp_dir`, etc.).


## Testing
Run all tests with:

```
uv run pytest
```
Coverage is located in `tests/` and includes:

* Value serialization
* QueryHelper expressions
* (Mocked) transport/metadata behaviors

See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete development and compatibility policy.
