# Risk-driven testing

qbvisor uses coverage to locate unexamined behavior and detect broad regressions. Coverage is not a
substitute for a test objective. A test earns its maintenance cost when it protects a credible
developer or operational failure.

## Test contract

Before writing a test, describe five things:

1. **Failure story:** the realistic defect or external condition being protected against.
2. **Observable contract:** the public result, exception, diagnostic, or persisted state that
   defines success.
3. **Forbidden effect:** the request, retry, partial file, state publication, or resource leak that
   must not occur.
4. **Boundary:** why the selected unit, stateful fake, filesystem fixture, or sandbox contract is
   the narrowest boundary that can prove the behavior.
5. **Defect proof:** a plausible mutation or broken implementation that the test detects.

Test names should describe the protected behavior. Prefer one coherent failure story over several
tests that each execute one branch without establishing a useful invariant.

## Choosing a boundary

Use deterministic unit and contract tests for behavior that does not require Quickbase:

- Exercise client behavior through `QuickBaseClient` when the facade contract matters.
- Put transport doubles at the HTTP-session boundary. Record requests and control responses,
  exceptions, retries, and cancellation without opening sockets or sleeping.
- Use stateful Quickbase fakes for schema planning and apply behavior. Assert both remote
  observations and local state publication.
- Use real files below `tmp_path` for backups and exports. Recompute hashes and manifests when the
  test needs to distinguish structural integrity from semantic validity.

Use the persistent sandbox only for behavior whose correctness depends on the live Quickbase
service, such as endpoint response shapes, mutation cleanup, or cross-request behavior. Sandbox
tests remain explicitly opted in and do not contribute to the default coverage gate.

## Assertions that carry value

For successful behavior, assert the complete documented result shape when it is stable. For
failure behavior, assert the public exception category and the diagnostic fields callers need.
Always assert important negative effects:

- no retry after cancellation or an unsafe uncertain mutation;
- no Quickbase mutation from a conflicted or stale schema plan;
- no published state when schema application fails to converge;
- no completed backup from malformed or incomplete artifacts;
- no replacement of a completed export by a partial file; and
- no closing of a session owned by the caller.

Exact request paths, parameters, and bodies are appropriate at the Quickbase API boundary. Avoid
asserting private call order when it is not part of that contract.

## Correctness-critical targets

These are behavioral targets, not per-module percentage gates:

| Surface | Required scenarios | Deferred or lower-value coverage |
| --- | --- | --- |
| Async transport | Safe versus unsafe replay, server-directed rate-limit retry, cancellation propagation, and session ownership | Assertion-only impossible return types and real-time backoff timing |
| Schema planner and apply | Stable identity, ambiguity and drift conflicts, descendant blocking, dependency order, local state atomicity, and recoverable partial remote mutation | Exhaustive malformed response permutations and broad plan snapshots |
| Backup reader | Cryptographic and semantic integrity, JSONL validity, table-local one-to-one attachment indexes, and aggregated missing/untracked diagnostics | OS-specific filesystem failures that cannot be deterministic across supported platforms |
| Compatibility surfaces | Stable signatures, exact result/error contracts, warnings, and side-effect timing | Repeating delegation tests already protected by the compatibility ledger |
| Low-risk helpers and logging | Regression tests when their public behavior changes | Tests added only because these modules offer inexpensive uncovered lines |

When a target is intentionally deferred, explain why its production risk is lower than the
maintenance or flake cost of the proposed test.

## Coverage policy

The default suite measures branches and enforces a staged repository-wide floor. CI reports missing
branch destinations by file and line so contributors can investigate relevant control flow.

The floor must never be raised to the exact observed result. Supported Python versions can differ
slightly, and a zero-margin threshold turns harmless interpreter differences into failures. Raise
the floor only after correctness-driven tests create stable headroom across every supported Python
version.

Do not add a test merely because a report names a line. First translate the missing branch into a
failure story. If no meaningful contract depends on it, document the rationale and leave it
uncovered.

## Mutation evidence

For correctness-critical changes, demonstrate that representative defects make the intended tests
fail. Suitable mutations include:

- reversing a retry-policy predicate;
- accepting the first ambiguous resource identity;
- publishing candidate state after failed verification;
- bypassing an attachment-index containment or uniqueness check; or
- narrowing cleanup from `BaseException` to `Exception`.

Mutation verification may be targeted and temporary. Record the exact mutation, the named failing
test, and the command in the pull request. Restore the source, confirm a clean diff, and run the
complete suite afterward. A mutation score is not a release objective.

## Review checklist

A reviewer should be able to answer yes to each question:

- Would this test fail for a plausible production defect?
- Does it exercise the behavior rather than a mock of the behavior?
- Does it assert the public outcome and important forbidden effects?
- Is it deterministic and credential-free unless the live API is essential?
- Does its name and failure output identify the broken contract?
- Is it materially different from existing coverage?
