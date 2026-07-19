# Configuration and authentication

qbvisor reads connection settings from environment variables. Keep credentials outside source code
and create the client only after the process environment is ready.

## Runtime settings

| Variable | Required | Purpose |
| --- | --- | --- |
| `QB_REALM_HOSTNAME` | Yes | Quickbase realm hostname, such as `example.quickbase.com` |
| `QB_REALM_API_KEY` | Yes | Complete authorization header value, including the `QB-USER-TOKEN` prefix |
| `QB_APP_IDS` | Yes | JSON object mapping readable aliases to Quickbase application IDs |
| `QBVISOR_ROOT` | No | Base directory used by qbvisor's optional file logger |
| `LOG_LEVEL` | No | Logging level used when `LoggingConfigurator.setup()` receives no explicit level |

Example:

```dotenv
QB_REALM_HOSTNAME=example.quickbase.com
QB_REALM_API_KEY=QB-USER-TOKEN replace-with-your-token
QB_APP_IDS={"Billing":"bp7xxxxxx","Development Sandbox":"bv75yyyyy"}
```

After environment or `.env` parsing, the value must be valid JSON containing one object whose keys
and values are strings.

## Application and resource resolution

`QB_APP_IDS` defines the applications that one client process may address:

```python
with QuickBaseClient() as qb:
    by_alias = qb.get_app("billing")
    by_id = qb.get_app("bp7xxxxxx")
```

Aliases are case-insensitive. Passing an application ID directly works only when that ID is already
one of the configured values. This prevents an unexpected string from silently becoming an
unreviewed application target.

After the application is resolved, table names and IDs come from live Quickbase metadata. Table
names and field labels are matched case-insensitively. qbvisor sends their stable IDs to Quickbase
and caches the resolved metadata for later operations. Successful schema mutations invalidate the
affected cache before another lookup.

The application mapping is loaded when `QuickBaseClient` is created. If code creates a new app with
`create_app()`, add the returned app ID to `QB_APP_IDS` and create a new client before using
name-resolved operations against that app. Declarative schema apply can create an unbound app and
its child resources in one reviewed operation; see [Building applications](application-building.md).

## `.env` discovery

Importing `qbvisor` looks for the first `.env` file in its package or project locations and the
current working directory. `QuickBaseClient` also asks `python-dotenv` to load the normal current
project file. Existing process variables are not overwritten.

This behavior is convenient for local scripts, but deployed applications should set environment
variables through their runtime, container, CI system, or secret manager. That makes the effective
configuration explicit and avoids depending on the process working directory.

## Token scope

- Assign a development token only to dedicated sandbox applications.
- Give production jobs access only to the applications they need.
- Keep `.env`, backups, record exports, and downloaded attachments out of Git.
- Rotate a token immediately if it appears in a commit, issue, log, test result, or support request.
- Do not print the client transport headers while debugging.

qbvisor excludes authorization values and request bodies from its transport logs and structured
exceptions. Consuming applications remain responsible for not logging their own environment or
record payloads.

## Custom transport settings

Pass a configured transport when an application needs different timeouts or retry attempts:

```python
from qbvisor import QuickBaseClient, QuickBaseTransport

with QuickBaseTransport(timeout=(5.0, 60.0), max_attempts=4) as transport:
    with QuickBaseClient(transport=transport) as qb:
        app = qb.get_app("Billing")
```

The client does not close a transport supplied by the caller. The outer context manager owns it.
Mutation replay policy does not become more permissive when `max_attempts` is increased.

## Integration-test settings

Repository integration tests use separate variables so runtime aliases cannot accidentally select
the test target:

```dotenv
QBVISOR_TEST_REALM=example.quickbase.com
QBVISOR_TEST_TOKEN=QB-USER-TOKEN replace-with-your-sandbox-token
QBVISOR_TEST_APP_ID=replace-with-your-sandbox-app-id
QBVISOR_RUN_INTEGRATION=0
QBVISOR_ALLOW_SANDBOX_MUTATIONS=0
```

Read-only sandbox tests require `QBVISOR_RUN_INTEGRATION=1`. Fixture creation and mutation tests
also require `QBVISOR_ALLOW_SANDBOX_MUTATIONS=1`. Review
[Contributing](https://github.com/ChrisEMetcalf/qbvisor/blob/main/CONTRIBUTING.md) before
enabling either setting.
