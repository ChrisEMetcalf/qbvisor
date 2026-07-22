# Sandbox stabilization workloads

This first workload slice answers a practical release question: can qbvisor complete the same
sequence of operations that a small developer automation performs against real Quickbase data?

It complements the focused integration tests. One run creates uniquely prefixed records, replays
them to distinguish updates from unchanged rows, queries them through friendly field labels,
converts them to a DataFrame, exports paged CSV, verifies an application backup, and deletes the
temporary records.

## Safety model

Workloads run only when all three safety switches are enabled:

```dotenv
QBVISOR_RUN_INTEGRATION=1
QBVISOR_ALLOW_SANDBOX_MUTATIONS=1
QBVISOR_RUN_WORKLOADS=1
```

Use a dedicated sandbox token and a bare application ID in `QBVISOR_TEST_APP_ID`. Do not put the
`QB_APP_IDS` JSON mapping in that variable. Every run receives a random `qbvisor-workload-...`
prefix, and cleanup targets only that exact prefix. Pytest evaluates all three switches before it
initializes the live sandbox fixtures.

An interrupted Python process may stop before cleanup. Search the contract table's **Fixture Key**
field for `qbvisor-workload-` before and after larger runs. Review the matching run prefix before
removing any leftover records through the Quickbase UI.

## Profiles

| Profile | Records | Export page | Backup page | Use |
| --- | ---: | ---: | ---: | --- |
| `smoke` | 30 | 10 | 10 | First sandbox run and pull-request verification |
| `standard` | 300 | 100 | 100 | Routine release-candidate stabilization |
| `scale` | 1,500 | 250 | 250 | Intentional pre-release scale exercise |

Profiles change volume and page sizes, not expected behavior. The contract does not enforce timing
thresholds; the recorded durations are diagnostic evidence, not performance benchmarks.

CSV export uses qbvisor's current sequential keyset pagination. The compatibility-only
`max_concurrency` argument is intentionally not presented as a workload scaling control.

## Stabilization coverage

Issue #23 covers more than the scaled record path. The release evidence combines this workload
with the established persistent-sandbox and local regression contracts:

| Target | Scaled workload | Supporting contract | Current boundary |
| --- | --- | --- | --- |
| Upserts and result shapes | Create, update, and unchanged 200 outcomes without `lineErrors` | Unit batch and live 200/207 contracts | Partial errors remain a focused 207 contract |
| Queries and DataFrames | Complete label-based query with value checks | QueryHelper and pagination regression tests | Generated workload rows only |
| CSV export | Multi-page keyset export with value checks | Export pagination regression tests | Sequential transport today |
| Attachments | Included in the application backup | Direct download, paging, and deletion live contracts | No scaled attachment churn |
| Declarative schemas | Not volume-scaled | Live idempotency, relationship, and formula contracts | Run separately from record workloads |
| Backups | Integrity and record-value verification | Persistent attachment round trip | Entire sandbox application; no resource scoping |
| Compatibility | Public calls exercised without signature changes | Client compatibility regression suite | Existing deprecated behavior is unchanged |

The workload is the new volume layer, not a replacement for those focused tests. Issue #23 should
close only after the pull request records both the full live contract and a reviewed workload
summary. Scoped table, record, and attachment backups are intentionally future work rather than a
hidden part of this stabilization change.

## Compatibility and migration

This workload scaffold does not change a public qbvisor method, signature, or return shape. Existing
applications require no migration. The new `QBVISOR_WORKLOAD_*` variables affect repository tests
only and default to disabled. Sequential CSV pagination and whole-application backup scope are
documented limitations, not new runtime behavior.

## Whole-application backup boundary

Only the generated records and page settings are bounded by the selected profile. `backup_app()`
captures the entire configured application, including every table, record, and selected attachment
already present in the sandbox. Run the workload only while that dedicated application is quiet.

The workload sets `fail_on_changes=True`; any table change detected during capture fails the run
instead of publishing an internally inconsistent result. The summary reports total application
records, attachments, artifact bytes, and consistency alongside the generated-row count so backup
timings retain their denominator and context.

## Run the smoke profile

First run the normal integration contract. It proves the token, application, and persistent fixture
are correct before the workload creates records.

### PowerShell

```powershell
$env:QBVISOR_RUN_INTEGRATION = "1"
$env:QBVISOR_ALLOW_SANDBOX_MUTATIONS = "1"
uv run pytest -m integration --no-cov
```

### Bash

```bash
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=1 \
  uv run pytest -m integration --no-cov
```

Then enable the workload and select its profile.

### PowerShell

```powershell
$env:QBVISOR_RUN_WORKLOADS = "1"
$env:QBVISOR_WORKLOAD_PROFILE = "smoke"
uv run pytest -m workload --no-cov -s
```

### Bash

```bash
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=1 \
  QBVISOR_RUN_WORKLOADS=1 QBVISOR_WORKLOAD_PROFILE=smoke \
  uv run pytest -m workload --no-cov -s
```

Move to `standard` only after `smoke` passes. Reserve `scale` for an intentional pre-release run
when no other developer or automation is changing records, files, or schema in the persistent
sandbox.

## Read the result

Each run writes one JSON file to `.qbvisor/workloads` by default. The directory is excluded from
Git. Set `QBVISOR_WORKLOAD_RESULTS` to another local directory when a release process collects
artifacts.

The summary contains the qbvisor and Python versions, UTC start and completion times, total and
per-operation durations, profile, record outcomes, whole-application backup totals, final status,
failure phase, and cleanup result. It does not contain the realm, token, application ID, request
headers, or record payloads.

A successful run has `status` set to `passed`, identical created, queried, exported, backed-up, and
deleted record counts, and an update plus unchanged outcome totaling the selected profile size.
`backupConsistent` must also be `true`. Keep the summary with the release-candidate checks when a
failure needs comparison across runs. If cleanup fails, use the recorded run prefix to inspect and
remove only those leftover rows through the Quickbase UI.
