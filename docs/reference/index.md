# Public API

The package root is the stable import surface. Prefer imports such as:

```python
from qbvisor import AppSpec, BackupOptions, QueryHelper, QuickBaseClient
```

## Support levels

| Level | Intended use | Stability |
| --- | --- | --- |
| Supported | `QuickBaseClient`, declarative schema models, backup models, `QueryHelper`, documented exceptions, logging, and transport configuration | Maintained under the release policy |
| Compatibility-retained | Historical helpers on `QuickBaseClient` retained for existing applications | Names and call signatures remain stable when practical; new code should prefer the supported workflow in the compatibility ledger |
| Compatibility-only parameter | An argument retained only to preserve an established call signature | The current implementation ignores it and warns whenever it is explicitly passed |
| Internal | Underscore-prefixed modules, resource services, metadata-cache internals, and the internal async transport | May change between minor pre-1.0 releases |

## Compatibility-retained methods

The following methods remain public because existing applications rely on them. They are not
currently deprecated:

- `download_attachments_async()` and `download_table_attachments_async()` are synchronous entry
  points despite their historical names. They use async I/O internally and cannot run on a thread
  that already has an active event loop.
- `get_field_id()`, `get_table_id()`, and `get_field()` are compatibility-retained metadata
  helpers. New workflows normally pass configured labels directly to client methods.
- `summarize_config()` and `dump_full_config()` inspect metadata already loaded into the client.
  They are debugging helpers, not application-discovery APIs.

The [`download_records_to_csv(max_concurrency=...)` compatibility ledger
entry](../compatibility-helpers.md#download_records_to_csvmax_concurrency) records why the parameter
is ignored and when it warns. The [complete compatibility helper
ledger](../compatibility-helpers.md) captures each retained signature, behavior, side effect,
event-loop boundary, and preferred alternative.

## Lower-level transport

`QuickBaseTransport` is supported when an application needs explicit session ownership, timeout
control, or retry timing. `QuickBaseClient` remains the recommended facade. qbvisor does not yet
provide a public native-async client; the internal async transport is not part of the compatibility
contract.

See the [release policy](../release-policy.md) for versioning and deprecation rules.
