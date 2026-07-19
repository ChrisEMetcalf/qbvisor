# Public API

The package root is the stable import surface. Prefer imports such as:

```python
from qbvisor import AppSpec, BackupOptions, QueryHelper, QuickBaseClient
```

## Support levels

| Level | Intended use | Stability |
| --- | --- | --- |
| Supported | `QuickBaseClient`, declarative schema models, backup models, `QueryHelper`, documented exceptions, logging, and transport configuration | Maintained under the release policy |
| Compatibility | Historical helpers on `QuickBaseClient` retained for existing applications | Names and call signatures remain stable when practical; new code should prefer the supported workflow noted below |
| Internal | Underscore-prefixed modules, resource services, metadata-cache internals, and the internal async transport | May change between minor pre-1.0 releases |

## Compatibility methods

The following methods remain public because existing applications rely on them:

- `download_attachments_async()` and `download_table_attachments_async()` are synchronous entry
  points despite their historical names. They use async I/O internally and cannot run on a thread
  that already has an active event loop.
- `get_field_id()`, `get_table_id()`, and `get_field()` expose the legacy metadata-resolution
  interface. New workflows normally pass configured labels directly to client methods.
- `summarize_config()` and `dump_full_config()` inspect metadata already loaded into the client.
  They are debugging helpers, not application-discovery APIs.

## Lower-level transport

`QuickBaseTransport` is supported when an application needs explicit session ownership, timeout
control, or retry timing. `QuickBaseClient` remains the recommended facade. qbvisor does not yet
provide a public native-async client; the internal async transport is not part of the compatibility
contract.

See the [release policy](../release-policy.md) for versioning and deprecation rules.
