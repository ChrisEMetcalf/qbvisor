# Backups and attachments

Quickbase does not provide a transactional application snapshot. qbvisor therefore treats backup
creation as a versioned, verifiable capture with explicit consistency limits.

## Create a backup

```python
from qbvisor import BackupOptions, QuickBaseClient

with QuickBaseClient() as qb:
    backup = qb.backup_app(
        "Billing",
        "backups",
        options=BackupOptions(
            attachment_versions="all",
            page_size=1000,
            max_attachment_concurrency=4,
            fail_on_changes=False,
        ),
    )
```

The backup is assembled in a private staging directory and moved under the destination only after
all requested artifacts and the final manifest are written. An incomplete capture is not published
as a completed backup.

`attachment_versions` accepts:

- `"all"` to preserve every reported attachment version;
- `"latest"` to preserve only the newest version;
- `"none"` to exclude attachment bodies.

## Contents and format

A backup contains application metadata, events, roles, tables, fields, relationships, reports,
records, and the requested attachment versions. Records are JSON Lines keyed by stable field IDs.
Attachment paths use table, record, field, and version IDs; original filenames and Quickbase
metadata remain in each table's attachment index.

The manifest records the format version, capture time range, options, application identity,
artifact paths, SHA-256 digests, byte counts, and item counts. The format is designed for reliable
verification and later tooling rather than direct editing.

## Verify and read

```python
from qbvisor import ApplicationBackup

backup = ApplicationBackup.open("/path/to/completed-backup")
verification = backup.verify()
invoices = backup.table_dataframe("Invoices")
```

`verify()` recalculates digests and sizes, checks JSON and JSON Lines item counts, rejects untracked
files and symbolic links, and validates the attachment index. `table_dataframe()` applies the
field labels captured in the same snapshot while keeping JSON Lines as the archive format.

## Consistency

qbvisor records capture start and completion times and calls `records_modified_since()` for each
table after capture. A detected change marks the completed manifest `consistent: false` and lists
affected table IDs.

Set `fail_on_changes=True` to discard that run and raise `BackupConsistencyError`. This check is
conservative, not transactional:

- schema changes during capture are not detected;
- deleted-record detection depends on Quickbase's **Index record changes** setting;
- changes can still occur between individual table and attachment requests.

Compression, encryption, retention, incremental capture, and restore automation are not part of
the current backup format. Apply those controls outside qbvisor according to the source data's
classification.

## Direct attachment downloads

The compatibility methods named `download_attachments_async()` and
`download_table_attachments_async()` are synchronous entry points. They scan matching records
sequentially in stable Record ID order and use bounded asynchronous I/O for the file transfers.

Download the latest attachment from one file field:

```python
results = qb.download_attachments_async(
    "Billing",
    "Invoices",
    "Source PDF",
    "downloads/invoices",
    where="{7.EX.'Approved'}",
    max_concurrency=4,
    page_size=1000,
)
```

Download the latest attachment from every file field in a table:

```python
results = qb.download_table_attachments_async(
    "Billing",
    "Invoices",
    "downloads/invoices",
    max_concurrency=4,
)
```

Each successful result includes `record_id`, `file_name`, `saved_path`, and `status`:

- `downloaded` also includes `bytes_written`;
- `skipped` means an existing destination was preserved;
- `failed` is included in `QuickbaseBatchError.results` with a safe error message.

Independent downloads finish before a batch error is raised. `QuickbaseBatchError.errors` retains
the original structured exceptions. Files are written through temporary paths and moved into place
only after a complete response is saved.

Single-field filenames use `recordId_filename`. Whole-table downloads include the field ID to avoid
collisions between two file fields. Quickbase filenames are sanitized before joining the target
directory.

These compatibility methods call `asyncio.run()` and cannot be called from a thread that already
has a running event loop. A native public async client is not currently supported.

## Base64 and deletion

`download_attachment_base64()` returns the latest attachment as an ASCII base64 string. It returns
`None` only when the record or attachment does not exist; transport, authorization, rate-limit, and
server failures raise qbvisor exceptions.

```python
encoded = qb.download_attachment_base64(
    "Billing",
    "Invoices",
    record_id=1042,
    file_field_label="Source PDF",
)
```

`delete_file()` deletes one explicit attachment version. Quickbase version `0` selects the latest
version. The deletion is a mutation and is not replayed after an uncertain failure.

## Data handling

Backups, exports, and downloaded attachments can contain customer data, user and role metadata, and
confidential documents. qbvisor does not include the API token or request headers, but destinations
still need access control, encryption, retention, and deletion policies appropriate for the source
application.
