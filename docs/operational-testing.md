# Continuous operational testing

The operational suite answers a narrow question on a schedule: do the documented qbvisor read,
upsert, attachment, backup, and declarative-plan paths still work against the real Quickbase
service? It complements unit tests and the larger [stabilization workloads](development-workloads.md);
it is a five-check canary, not a performance or load test.

## Sandbox fixture strategy

Use one application dedicated to qbvisor tests. It must not contain production data or accept
production traffic. The suite reuses the persistent `qbvisor_sdk_contract_records` and
`qbvisor_sdk_contract_details` fixture tables established by the integration contract. Stable
`qbvisor-alpha`, `qbvisor-beta`, and `qbvisor-gamma` records provide read, attachment, backup, and
schema-plan assertions.

Configure the `quickbase-sandbox` GitHub environment with these secrets:

| Secret | Required form |
| --- | --- |
| `QBVISOR_TEST_REALM` | Bare hostname such as `example.quickbase.com`; no scheme or path |
| `QBVISOR_TEST_TOKEN` | Least-privilege user token scoped only to the sandbox |
| `QBVISOR_TEST_APP_ID` | One bare application ID; never a `QB_APP_IDS` JSON mapping |

Do not add the token to repository variables, workflow inputs, artifacts, or command lines. The
workflow has read-only repository permission, exposes the three sandbox secrets only to the pytest
step, and serializes every run through one concurrency group so two operational mutations cannot
overlap.

Each mutation owns one record whose unique **Fixture Key** starts with
`qbvisor-operational-<run-id>-`. The test verifies the create or file round trip, deletes by that
exact key in a `finally`-equivalent context, queries again to prove absence, and checks the delete
count. It never deletes persistent fixture records, stabilization workload records, or arbitrary
application rows.

Before any smoke check starts, recovery deletes records with the reserved
`qbvisor-operational-` prefix and proves that no matching rows remain. This makes process
termination and failed cleanup recoverable on the next serialized run. Do not use that prefix for
manual data.

## Scheduled coverage

`.github/workflows/operational.yml` runs at 07:17 UTC every Monday against the current `main`
commit. Its five explicit smoke tests cover:

1. Application and label-based record reads.
2. A create, verified read, update, and exact-record cleanup.
3. A base64 attachment upload, download byte comparison, and parent-record cleanup.
4. A consistent whole-application backup with manifest and persistent-data verification.
5. A read-only schema plan against the persistent tables and relationship.

The workflow uses the locked dependencies from the tested commit and uploads a JSON summary plus
JUnit output for 14 days. Normal pull-request CI does not need Quickbase credentials and skips the
operational tests unless all explicit opt-ins are present.

## Diagnostics and secret boundary

`.qbvisor/operational/summary.json` contains only an opaque hash derived from the validated run ID,
a tightly constrained candidate label, UTC timestamps, phase names, durations, pass/fail state,
cleanup counts, exception class, HTTP status, and Quickbase diagnostic ray when available. It
deliberately excludes exception messages, realm, token, application ID, request headers, request
bodies, record values, and attachment bytes.

Start with the failed phase and exception class. For an HTTP failure, use the status and
`qbApiRay` to correlate the request with Quickbase support. For a cleanup failure, search only for
the reserved operational prefix in the dedicated sandbox before rerunning. Rotate the sandbox
token immediately if it ever appears in a log or artifact despite these controls.

Configuration errors fail before an HTTP client is created. In particular,
`QBVISOR_TEST_APP_ID` reports a value-free explanation when it contains a JSON mapping, and
`QBVISOR_TEST_REALM` rejects a URL rather than printing it.

## Prove recovery after a failed cleanup

Use a quiet sandbox and a fixed, non-secret run ID. The first command intentionally leaves only
the upsert smoke record and must fail with `IntentionalCleanupFailure`:

```bash
QBVISOR_RUN_INTEGRATION=1 QBVISOR_ALLOW_SANDBOX_MUTATIONS=1 \
  QBVISOR_RUN_OPERATIONAL=1 QBVISOR_OPERATIONAL_RUN_ID=recovery-proof \
  QBVISOR_OPERATIONAL_FAIL_CLEANUP_ONCE=1 \
  uv run pytest -m operational --no-cov -ra
```

Unset the injection flag and repeat the same command. Recovery must report at least one deleted
record, all five checks must pass, each mutating check must report one deleted record, and a third
ordinary run must also pass. Never configure `QBVISOR_OPERATIONAL_FAIL_CLEANUP_ONCE` in GitHub.

## Verify a release candidate manually

Create and push an existing `v`-prefixed semantic-version tag on a commit already contained in
`main`, for example `v0.3.1-rc.1`. Open **Actions → Quickbase operational smoke → Run workflow**
and enter that exact tag as `candidate_tag`.

The credential-free resolver job rejects missing tags, malformed names, and commits outside
`main`. Only its resolved commit SHA is checked out by the environment-scoped job. Approve the
candidate only when the workflow passes, its summary names the expected tag, recovery is clean or
explained, and all five check statuses are `passed`.
