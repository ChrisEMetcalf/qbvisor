# Logging, errors, and recovery

qbvisor uses standard Python logging and a structured exception hierarchy. Logging is opt-in; an
application decides where records are written and which level is enabled.

## Configure logging

```python
from qbvisor import LoggingConfigurator

logger = LoggingConfigurator.setup(
    logger_name="qbvisor",
    log_dir="logs",
    log_name="qbvisor.log",
    log_level="INFO",
    max_bytes=5 * 1024 * 1024,
    backup_count=5,
)
```

Setup adds a rotating UTF-8 file handler and a console handler. `QBVISOR_ROOT` selects the base
directory; otherwise the current working directory is used. When `log_name` is omitted, the file is
named after that base directory. `LOG_LEVEL` is used when `log_level` is omitted; the current final
fallback is `DEBUG`.

Call setup once near the application entry point. Repeated calls return the configured logger
without rebuilding handlers. Libraries that consume qbvisor should normally configure their own
logging tree and leave qbvisor unconfigured.

Use `get_logger()` to retrieve a logger without installing handlers:

```python
from qbvisor import get_logger

logger = get_logger("qbvisor.integration")
```

## Exception hierarchy

Catch the narrowest exception that supports the required recovery decision:

| Exception | Meaning | Typical response |
| --- | --- | --- |
| `QuickbaseConfigurationError` | Realm or authorization configuration is missing | Stop before making a request |
| `QuickbaseConnectionError` | A connection failed outside a safe retry path | Check network state and operation certainty |
| `QuickbaseTimeoutError` | Connect or read timeout | Reconcile mutations before retrying |
| `QuickbaseHTTPError` | Quickbase returned a non-success status | Inspect status, description, and request ID |
| `QuickbaseRateLimitError` | A `429` response could not be safely delayed and replayed | Retry later under caller policy |
| `QuickbaseResponseError` | A success response did not match its endpoint contract | Preserve diagnostics and investigate API drift |
| `QuickbaseBatchError` | Some work completed before another item or request failed | Reconcile individual result statuses |
| `QuickbaseSchemaConflictError` | Desired and observed schema cannot be reconciled safely | Resolve the conflict and plan again |
| `QuickbaseSchemaStalePlanError` | Remote or local state changed after planning | Review a new plan |
| `QuickbaseSchemaLockError` | Another apply owns the state lock | Wait for or investigate the active apply |
| `BackupConsistencyError` | Records changed during a strict backup | Retry during a quieter window |
| `BackupIntegrityError` | Stored backup contents failed verification | Quarantine the backup and investigate |

All SDK transport exceptions inherit from `QuickbaseError`. Input-name resolution still has some
legacy exception behavior; validate configured aliases and use the documented client methods rather
than depending on a particular exception class for an unknown app, table, or field.

## HTTP diagnostics

```python
from qbvisor import QuickBaseClient, QuickbaseHTTPError

try:
    with QuickBaseClient() as qb:
        qb.get_app("Billing")
except QuickbaseHTTPError as error:
    print(error.status_code, error.message, error.description, error.qb_api_ray)
```

HTTP exceptions retain the request method and path, status code, Quickbase message and description,
`Retry-After`, and `qb-api-ray` when present. Authorization headers and request bodies are excluded
from the exception message and transport logs.

The `qb-api-ray` value is the most useful identifier when a Quickbase support request needs to trace
a failed API call.

## Retry and mutation certainty

GET requests and read-like POST operations retry configured connection failures, timeouts, and
temporary gateway responses. Mutations are not replayed after those failures because the server may
have committed the operation before the connection failed.

A `429` response is different: qbvisor retries any operation only when Quickbase provides a valid
`Retry-After` value. Missing or malformed guidance raises `QuickbaseRateLimitError` immediately.

When a mutation fails uncertainly:

1. Do not assume the request failed.
2. Read the affected resource or records using a stable identifier.
3. Compare the observed state with the intended state.
4. Retry only the work that did not commit.

Use unique merge fields for upserts and reviewed state for schema apply so reconciliation is
possible.

## Batch failures

`QuickbaseBatchError.results` contains an outcome for every completed item or request range. Its
`errors` collection retains the original exceptions. A result marked `uncertain` requires a read
before retry; a result marked `failed` represents a definitive rejection.

For attachment downloads, independent transfers finish before the batch error is raised. For
payload-batched upserts, completed ranges remain committed even when a later request fails.

## Safe debugging

- Log statuses, resource IDs, input positions, and `qb-api-ray`, not tokens or complete payloads.
- Reproduce mutations in a dedicated sandbox with non-sensitive fixtures.
- Keep logging destinations outside source control.
- Treat logs from consuming applications as potentially sensitive even though qbvisor redacts its
  own transport details.
