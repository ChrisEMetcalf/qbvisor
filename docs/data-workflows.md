# Data queries, reports, and record movement

qbvisor provides two record-reading levels:

- `query_records()` returns one native Quickbase JSON response and defaults to 1,000 records.
- `query_dataframe()` and `run_report()` return complete pandas DataFrames unless `top` limits the
  total result.

Choose the native response when field-ID cells or response metadata are part of the integration
contract. Choose a DataFrame for analysis, transformation, reporting, and bulk export preparation.

## Build queries with field labels

`QueryHelper` resolves field labels once and builds Quickbase query expressions with the correct
field IDs and value serialization:

```python
from qbvisor import QueryHelper, QuickBaseClient

with QuickBaseClient() as qb:
    query = QueryHelper(qb, "Billing", "Invoices")
    where = query.and_(
        query.eq("Status", "Approved"),
        query.greater_than("Amount", 0),
        query.on_or_after("Invoice Date", "2026-01-01"),
    )

    invoices = qb.query_dataframe(
        "Billing",
        "Invoices",
        ["Invoice Number", "Invoice Date", "Customer", "Amount"],
        where=where,
    )
```

Convenience methods cover equality, containment, list membership, prefix, comparison, and date
operators. `expr(field_label, operator, value)` accepts a supported Quickbase operator code when a
named helper is not available. qbvisor builds query syntax; Quickbase remains responsible for
evaluating field types and formula semantics.

## DataFrame queries

An unsorted, ungrouped query starting at zero uses Record ID# continuation. qbvisor selects Record
ID# internally when needed and removes it when the caller did not request it. This avoids fixed
offsets skipping or duplicating records when other records are inserted or deleted during a scan.

```python
first_500 = qb.query_dataframe(
    "Billing",
    "Invoices",
    ["Invoice Number", "Status", "Amount"],
    top=500,
)
```

Sorted, grouped, or explicitly offset queries use Quickbase's returned `skip`, `numRecords`, and
`totalRecords` metadata because adding a Record ID cursor would change their meaning. Those result
sets are not point-in-time snapshots; concurrent changes can affect later pages.

Complete reads are materialized in memory. Use `top` for bounded interactive work or
`download_records_to_csv()` for large operational extracts.

## Reports

`run_report()` preserves the established method signature and returns the complete report by
default:

```python
with QuickBaseClient() as qb:
    report = qb.run_report(
        "Billing",
        "Invoices",
        report_id=12,
        skip=0,
        top=None,
    )
```

Quickbase limits report responses by payload and returned metadata rather than the normal record
query page contract. qbvisor continues only when that metadata reports remaining records. Report
pages use offsets and do not provide snapshot isolation.

## Native record responses

Use `query_records()` when the caller needs Quickbase's native field-ID cell structure:

```python
with QuickBaseClient() as qb:
    response = qb.query_records(
        "Billing",
        "Invoices",
        select_fields=["Invoice Number", "Status"],
        where="{3.GT.'0'}",
        top=1000,
    )
```

This method performs one request. It does not promise a complete table scan and intentionally keeps
its existing default `top=1000` behavior.

## Idempotent upserts

Use a stable, unique merge field whenever a write may be retried:

```python
with QuickBaseClient() as qb:
    result = qb.upsert_records(
        "Billing",
        "Invoices",
        [
            {"Invoice Number": "INV-1042", "Status": "Approved", "Amount": 425.00},
            {"Invoice Number": "INV-1043", "Status": "Draft", "Amount": 180.00},
        ],
        merge_field_label="Invoice Number",
        fields_to_return=["Invoice Number", "Status", "Amount"],
    )
```

qbvisor resolves labels to IDs, serializes every record, and plans the complete batch before the
first mutation. Inputs larger than Quickbase's 40 MB request limit are divided into the fewest
contiguous sequential requests that fit.

The aggregated result retains Quickbase's native returned data and includes:

- `createdRecordIds`, `updatedRecordIds`, and `unchangedRecordIds`;
- `totalProcessed`, `success`, and `partial`;
- one-based `lineErrors` rebased to the original input positions.

A normal `207` partial response does not prevent later planned batches from running because
Quickbase completed that request. If a later request fails, `QuickbaseBatchError.results` identifies
completed ranges and the failed or uncertain range. Timeouts, connection failures, server errors,
and invalid success responses are uncertain because the server may have committed the request.

Append-only writes without a merge field are not idempotent. A caller retry can create duplicates.

## Deletes and incremental changes

Record deletion accepts a Quickbase query or explicit record IDs and returns the reported deletion
count:

```python
deleted = qb.delete_records("Billing", "Import Staging", where=[101, 102, 103])
```

This is a destructive mutation and is not replayed after uncertain transport failure.

Use `records_modified_since()` for incremental workflows that depend on Quickbase's change index:

```python
changes = qb.records_modified_since(
    "Billing",
    "Invoices",
    "2026-07-01T00:00:00Z",
    field_list=["Status", "Amount"],
    include_details=True,
)
```

The timestamp must include a timezone. qbvisor normalizes it to conservative whole-second UTC so
fractional-second conversion cannot skip a change. Deleted-record visibility depends on the
application's **Index record changes** setting.

## Stable CSV exports

`download_records_to_csv()` reads pages sequentially in Record ID# order, writes a temporary file,
and moves the completed export into place only after success:

```python
path = qb.download_records_to_csv(
    "Billing",
    "Invoices",
    "exports",
    where="{3.GT.'0'}",
    chunk_size=1000,
    record_limit=None,
)
```

`chunk_size` is capped at 1,000. `record_limit` is exact. `max_concurrency` is a
compatibility-only parameter: record pages remain sequential to avoid unstable fixed offsets, and
passing a valid value explicitly emits `UserWarning`, including `max_concurrency=4`; invalid values
are rejected. Omit it in new code. The [compatibility helper
ledger](compatibility-helpers.md#download_records_to_csvmax_concurrency) records the retained
signature and side effects.
