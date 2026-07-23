# Compatibility helper ledger

`QuickBaseClient` is the supported high-level API. The helpers on this page are
**compatibility-retained**: they remain public for existing applications, but new code should use
the preferred workflow when it fits. Compatibility-retained does not mean deprecated; none of the
helpers inventoried here has a removal schedule.

A **compatibility-only parameter** remains in a public signature so existing calls keep working,
but it does not control the current implementation. Passing a non-default value emits a
`UserWarning` so the ignored setting cannot silently look effective.

## Inventory

### `download_attachments_async`

- **Signature:** `download_attachments_async(self, app_name: str, table_name: str,
  file_field_label: str, target_dir: str, where: str | None = None,
  max_concurrency: int = 4, page_size: int = 1000) -> list[dict[str, Any]]`
- **Classification:** Compatibility-retained synchronous helper with a historical async name.
- **Behavior:** Scans matching records in stable Record ID# order, selects the latest version in
  one file field, and performs bounded concurrent file transfers. It returns per-file outcomes and
  raises `QuickbaseBatchError` after independent transfers finish if any transfer failed.
- **Side effects:** Reads table and field metadata, queries records, creates `target_dir`, writes
  temporary and completed attachment files, and logs outcomes. It creates and closes an internal
  event loop.
- **Event-loop boundary:** A call made on a thread with an active event loop raises `RuntimeError`
  before metadata, directory, or network side effects. Async applications can run the synchronous
  helper in a worker thread with `await asyncio.to_thread(...)`.
- **Preferred alternative:** Use `backup_app()` with
  `BackupOptions(attachment_versions="latest")` for a durable, indexed capture, or
  `download_attachment_base64()` when one known record is needed.

### `download_table_attachments_async`

- **Signature:** `download_table_attachments_async(self, app_name: str, table_name: str,
  target_dir: str, where: str | None = None, max_concurrency: int = 4,
  page_size: int = 1000) -> list[dict[str, Any]]`
- **Classification:** Compatibility-retained synchronous helper with a historical async name.
- **Behavior:** Scans every file field on matching records, selects each latest attachment, and
  performs bounded concurrent transfers. Whole-table filenames include the field ID to avoid
  collisions.
- **Side effects:** Reads table and field metadata, queries records, creates `target_dir`, writes
  temporary and completed attachment files, and logs outcomes. It creates and closes an internal
  event loop.
- **Event-loop boundary:** A call made on a thread with an active event loop raises `RuntimeError`
  before metadata, directory, or network side effects. Async applications can use
  `await asyncio.to_thread(...)`.
- **Preferred alternative:** Use `backup_app()` with
  `BackupOptions(attachment_versions="latest")` for a verifiable table-inclusive application
  capture.

### `get_field_id`

- **Signature:** `get_field_id(self, app_id: str, table_id: str, field_label: str) -> int`
- **Classification:** Compatibility-retained metadata helper.
- **Behavior:** Resolves a case-insensitive field label to its numeric Quickbase field ID.
- **Side effects:** May fetch and cache table and field metadata. Lookup and transport failures
  propagate as qbvisor exceptions.
- **Preferred alternative:** Pass field labels directly to high-level client methods such as
  `query_dataframe()`, `upsert_records()`, and `get_field_usage()`.

### `get_table_id`

- **Signature:** `get_table_id(self, app_id: str, table_id: str) -> str`
- **Classification:** Compatibility-retained metadata helper.
- **Behavior:** Resolves a configured table name or an already-known table ID to a Quickbase table
  ID.
- **Side effects:** May fetch and cache the application's table catalog and selected table
  metadata. Lookup and transport failures propagate as qbvisor exceptions.
- **Preferred alternative:** Pass the configured table name directly to high-level client methods.

### `get_field`

- **Signature:** `get_field(self, app_id, table_id, field_id)`
- **Classification:** Compatibility-retained metadata helper.
- **Behavior:** Resolves the table name or ID, then returns the Quickbase response for one numeric
  field ID.
- **Side effects:** May fetch and cache table metadata and performs a field network request.
  Lookup, response, and transport failures propagate.
- **Preferred alternative:** Use `get_fields_for_table()` for explicit supported field discovery,
  then select the needed field from the returned list.

### `summarize_config`

- **Signature:** `summarize_config(self, show_fields: bool = False)`
- **Classification:** Compatibility-retained diagnostic helper.
- **Behavior:** Logs a human-readable summary of metadata already present in the client's cache.
  `show_fields=True` includes cached fields. An empty cache logs that no config is available.
- **Side effects:** Emits log records only. It does not make network requests or populate missing
  metadata.
- **Preferred alternative:** Use `get_tables_for_app()` and `get_fields_for_table()` when code
  needs current, structured metadata instead of diagnostic log text.

### `dump_full_config`

- **Signature:** `dump_full_config(self)`
- **Classification:** Compatibility-retained diagnostic helper.
- **Behavior:** Serializes the in-memory metadata cache as indented JSON and logs it. Serialization
  failures are logged and not raised.
- **Side effects:** Emits log records that can expose application schema details. It does not make
  network requests or populate missing metadata.
- **Preferred alternative:** Use `get_tables_for_app()` and `get_fields_for_table()` for current,
  structured metadata and log only the fields appropriate for the application's data policy.

### `download_records_to_csv(max_concurrency=...)`

- **Signature:** `download_records_to_csv(self, app_name: str, table_name: str, output_dir: str,
  where: str = "{3.GT.'0'}", chunk_size: int = 1000, record_limit: int | None = None,
  max_concurrency: int = 4) -> str`
- **Classification:** `download_records_to_csv()` is supported; `max_concurrency` is a
  compatibility-only parameter.
- **Behavior:** CSV pages are always fetched sequentially in stable Record ID# order.
  `max_concurrency` is validated but ignored. Passing a non-default value emits `UserWarning`;
  calls using the default remain silent because the retained signature cannot distinguish omission
  from an explicitly passed default.
- **Side effects:** The supported method reads metadata and records, creates `output_dir`, writes a
  temporary CSV, and atomically replaces the dated final CSV after success. On failure it removes
  the temporary file.
- **Preferred alternative:** Omit `max_concurrency`. Use `chunk_size` and `record_limit` to bound
  the supported sequential export.

## Beginner examples

Use a retained helper when a direct latest-only attachment download is the intended outcome:

```python
from qbvisor import QuickBaseClient

with QuickBaseClient() as qb:
    results = qb.download_attachments_async(
        "Billing",
        "Invoices",
        "Source PDF",
        "downloads/invoices",
        max_concurrency=4,
    )
```

For a durable attachment capture, prefer the supported backup workflow:

```python
from qbvisor import BackupOptions, QuickBaseClient

with QuickBaseClient() as qb:
    backup = qb.backup_app(
        "Billing",
        "backups",
        options=BackupOptions(attachment_versions="latest"),
    )
```

Pass labels to supported client methods instead of resolving IDs first:

```python
from qbvisor import QuickBaseClient

with QuickBaseClient() as qb:
    active = qb.query_dataframe(
        "Billing",
        "Invoices",
        ["Invoice Number", "Status"],
        where="{7.EX.'Active'}",
    )
```

For structured discovery, use the supported collection methods:

```python
from qbvisor import QuickBaseClient

with QuickBaseClient() as qb:
    tables = qb.get_tables_for_app("Billing")
    fields = qb.get_fields_for_table("Billing", "Invoices")
```

Use the supported sequential CSV export without the compatibility-only argument:

```python
from qbvisor import QuickBaseClient

with QuickBaseClient() as qb:
    path = qb.download_records_to_csv(
        "Billing",
        "Invoices",
        "exports",
        chunk_size=1000,
        record_limit=10_000,
    )
```
