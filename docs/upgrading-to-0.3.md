# Upgrading from 0.2 to 0.3

qbvisor `0.3.0` was released on July 19, 2026. This guide covers the compatibility and operational
changes from `0.2.x`.

qbvisor preserves established `QuickBaseClient` method names and the public signatures of its most
used query, report, export, attachment, and upsert methods. The release intentionally changes some
observable behavior where the previous result was incomplete, unsafe, or inconsistent with the
documented Quickbase API.

## Python and installation

- Python 3.12 is now the minimum supported version.
- Python 3.12, 3.13, and 3.14 are tested.
- The package uses a `src/` layout and modern `pyproject.toml` metadata.
- The PyPI package exposes inline typing and `qbvisor.__version__`.

Install the reviewed release with `python -m pip install qbvisor==0.3.0` or
`uv add qbvisor==0.3.0`.

## Transport and exceptions

- Requests share a pooled session and use explicit connect and read timeouts.
- Safe reads retry temporary connection and gateway failures.
- Mutations are not retried after uncertain connection failures.
- A `429` response is retried only with a valid Quickbase `Retry-After` value.
- Transport failures now raise structured qbvisor exceptions instead of raw `requests` errors.
- Successful responses are checked against each endpoint's documented JSON shape.

Review broad `except requests.RequestException` handlers. They should normally catch an appropriate
`QuickbaseError` subclass instead.

## Queries and reports

`query_dataframe()` and `run_report()` now return the complete requested result by default. Their
existing `top` arguments remain optional and can enforce a total limit.

This can increase memory use and request count for large tables or reports. Add an explicit `top`
where the existing caller expects a bounded result. Use `query_records()` for one native response
or `download_records_to_csv()` for a stable large extract.

`query_records()` retains its one-request behavior and default `top=1000` contract.

## Record exports

CSV exports now scan in stable Record ID# order and publish the output only after all pages succeed.
The existing `max_concurrency` argument remains accepted, but pages are fetched sequentially to
avoid records shifting between offsets.

## Upserts

`upsert_records()` preserves the established call signature and existing result keys while adding
complete outcomes:

- created, updated, and unchanged record IDs;
- returned data;
- total processed counts;
- partial status and one-based line errors.

Payloads are measured before the first mutation and split at the Quickbase 40 MB limit. A later
batch failure raises `QuickbaseBatchError` with completed and failed or uncertain input ranges.

Callers that previously treated any returned dictionary as complete success should check `success`
and `partial`. Callers retrying writes should use a stable unique merge field.

## Attachments

The existing methods ending in `_async` remain synchronous entry points. They now scan all matching
records, use bounded asynchronous file transfers, preserve raw bytes correctly, and report
`downloaded`, `skipped`, or `failed` outcomes.

One or more failed files raise `QuickbaseBatchError` after independent downloads finish. Code that
previously expected a partial list without an exception should catch the batch error and inspect
`results`.

Whole-table downloads include field IDs in filenames to prevent collisions. Direct downloads use
the latest reported attachment version. Backup workflows can preserve every version.

## Metadata and endpoint responses

Table and field metadata are cached and invalidated after successful schema mutations. Collection
endpoints now preserve their documented top-level list responses rather than assuming an object
wrapper.

If consuming code depended on an undocumented wrapper or stale cache, update it to use the native
list and perform a new lookup after mutation.

## New workflows

The release adds:

- versioned, verifiable application backups;
- declarative app, table, field, formula, and relationship management;
- app events, app roles, field usage, relationship lookup and summary operations;
- record-change inspection;
- public package metadata and inline typing.

These additions do not enable destructive declarative deletion or transactional Quickbase
snapshots. Review their documented boundaries before operational use.

## Upgrade checklist

1. Run the existing application test suite on Python 3.12 or later.
2. Add explicit `top` values where a complete DataFrame or report would be too large.
3. Update exception handling to use qbvisor exceptions.
4. Verify upsert callers check partial outcomes and use merge fields where retries are possible.
5. Verify attachment callers handle `QuickbaseBatchError` and the expanded result status.
6. Run critical workflows against a dedicated Quickbase sandbox.
7. Pin the final `0.3.0` release only after those checks pass.

See the [0.3.0 release notes](releases/0.3.0.md) for the release summary and the
[changelog](https://github.com/ChrisEMetcalf/qbvisor/blob/main/CHANGELOG.md) for the complete change
list.
