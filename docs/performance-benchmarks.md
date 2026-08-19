# Reproducible local performance benchmarks

qbvisor's local benchmark suite provides repeatable evidence for performance work without a
Quickbase realm, token, or application. It runs deterministic fakes and local artifacts through
the real workflow code, records the workload shape and machine context, and preserves every sample
needed to judge repeat variance.

The suite is comparison evidence, not a universal speed claim. The committed `0.3.1` result is a
machine-context baseline, and the comparison signal is intentionally informational. Neither a
percentage signal nor a live Quickbase timing is a CI pass/fail threshold.

## Run locally

From the repository root, run every scenario with the `small`, `medium`, and `large` profiles:

```bash
uv run python -m benchmarks.run \
  --profile small --profile medium --profile large \
  --warmups 1 --repeats 5 \
  --min-sample-seconds 0.100 --max-operations-per-sample 10000 \
  --output .qbvisor/benchmarks/current.json
```

The command is credential-free: do not set `QB_REALM_HOSTNAME`, `QB_REALM_API_KEY`,
`QB_APP_IDS`, or the sandbox test variables. With no `--profile` or `--scenario` arguments, the
runner selects all profiles and all eight scenarios across seven surfaces. Both selection
arguments are repeatable; for
a faster diagnostic run, select one pair:

```bash
uv run python -m benchmarks.run \
  --scenario record-keyset-pagination --profile small \
  --warmups 1 --repeats 5 \
  --min-sample-seconds 0.100 --max-operations-per-sample 10000
```

The default output is `.qbvisor/benchmarks/result-<UTC timestamp>.json`, with microseconds in the
timestamp so separate runs receive separate names. `.qbvisor` is ignored by Git so ordinary local
results do not become source artifacts. Reports are published atomically, and the runner refuses
to replace an existing path; choose a new `--output` name when repeating an explicit command.
The duration settings shown are the defaults. `--min-sample-seconds` sets the accumulated measured
`run()` duration sought for each logical sample, while `--max-operations-per-sample` bounds the
number of fresh operations used to reach it. Each configured warmup is one fresh, unrecorded
operation; it is not duration-batched. `--warmups 0` disables warmups. The five repeats in this
command mean five logical samples, not necessarily five operation invocations.

## Deterministic profiles

Profiles change data volume and shape, not expected behavior. Payload contents, fake responses,
archive files, attachment bytes, and schema metadata are deterministically constructed for every
iteration.

| Profile | Records | Page size | Fields | Attachments × bytes | Schema tables × fields | Upsert records × value bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `small` | 100 | 25 | 6 | 8 × 4 KiB | 2 × 8 | 100 × 128 B |
| `medium` | 1,000 | 100 | 12 | 32 × 16 KiB | 6 × 20 | 1,000 × 1 KiB |
| `large` | 10,000 | 250 | 24 | 128 × 64 KiB | 12 × 40 | 48 × 1 MiB |

The large upsert profile deliberately uses fewer, much larger values. Its serialized payload
crosses Quickbase's 40,000,000-byte request limit so batching behavior is measured without sending
a mutation.

Every result repeats this complete definition in `profile_shape`. The scenario-specific `shape`
records the effective counts, byte sizes, page or request boundaries, concurrency, and metadata
behavior used by that operation. A profile name alone is not enough to establish a comparable
workload; compare those recorded shapes as well.

## Surfaces and measured work

The registry spans all seven issue #24 surfaces. DataFrame conversion has separate cold- and
warm-metadata scenarios, so the registry contains eight entries:

| Surface | Scenario | Timed contract | Throughput unit |
| --- | --- | --- | --- |
| pagination | `record-keyset-pagination` | Consume every deterministic page using Record ID# keyset continuation. | records |
| DataFrame conversion | `dataframe-materialization-cold-metadata` | Produce the complete label-based pandas DataFrame while resolving metadata from an empty cache. | records |
| DataFrame conversion | `dataframe-materialization-warm-metadata` | Produce the same complete DataFrame after priming metadata outside the timed interval. | records |
| CSV export | `csv-sequential-export` | Fetch keyset pages and atomically publish the completed CSV. | records |
| upsert batching | `upsert-batch-planning` | Serialize records and plan contiguous requests against the real 40,000,000-byte boundary. | records |
| attachment transfer | `attachment-concurrent-transfer` | Transfer deterministic bytes with the real bound of four concurrent tasks and atomic local file publication. | attachments |
| backup reads | `backup-read-verify-dataframe` | Open and verify a deterministic local archive, then read its table into a DataFrame. | records |
| schema planning | `schema-readonly-planning` | Plan a matching deterministic application from metadata without mutations or state publication. | schema resources |

Scenario setup builds fakes and input files before timing. Validation checks the complete result,
pagination cursors, request boundaries, output contents, temporary-file cleanup, concurrency bound,
or read-only effects after timing. The elapsed sample therefore measures the named workflow rather
than fixture construction or assertion cost.

Metadata cache state is part of the workload shape, not an inference from `--warmups`. A benchmark
warmup is merely an unrecorded isolated iteration. Both DataFrame scenarios use the real metadata
cache with a deterministic in-process transport. The cold scenario begins empty and performs
exactly three timed metadata GETs: the table catalog, table detail, and fields. The warm scenario
performs those same three requests during preparation and exactly zero during the timed query.
Their `shape` values record `metadata_cache` as `cold` or `warm` and
`timed_metadata_requests` as `3` or `0`. Compare cold results with cold results and warm results
with warm results; do not label the second process-level repeat "warm" when the scenario creates a
fresh client.

## Metrics and repeat variance

Each repeat produces one duration-backed logical sample. Inside it, the runner performs this
sequence one or more times:

1. create a fresh prepared scenario;
2. start the monotonic `perf_counter` immediately before `run()` and stop it immediately after
   `run()` returns;
3. validate the result and read deterministic workload observations; and
4. clean up that prepared scenario.

Only the intervals around `run()` are accumulated. Preparation, validation, observation, cleanup,
and the gaps between fresh operations do not contribute to measured duration. The runner repeats
that full fresh-operation sequence until accumulated `run()` time reaches `min_sample_seconds` or
the operation count reaches `max_operations_per_sample`.

Each raw sample contains:

- `elapsed_seconds`: mean latency per operation, calculated as
  `total_measured_seconds / operation_count`;
- `throughput_units_per_second`: `unit_count * operation_count / total_measured_seconds`;
- `total_measured_seconds`: the sum of only the measured `run()` intervals;
- `operation_count`: the number of fresh latency operations in the logical sample;
- `sampling_cap_reached`: whether the operation cap was reached before the duration target;
- `peak_traced_memory_bytes`: the separate memory run's peak traced allocation, or `null` when the
  scenario does not track it;
- `workload_observation_totals`: each deterministic per-operation observation multiplied by
  `operation_count`; and
- `workload_observation_rates_per_second`: those totals divided by `total_measured_seconds`.

The per-operation latency is therefore an average inside one logical sample; `summary.median` is
the median of those averages across repeats. If `sampling_cap_reached` is true, the sample did not
reach the requested timing floor. Inspect it directly and use `sampling_cap_reached_count` in the
summary before relying on a very fast workload's comparison.

For scenarios that track peak memory, each logical sample has one additional fresh
prepare/run/validate/observe/cleanup operation with `tracemalloc` enabled. Its elapsed time is
discarded, and it does not contribute to `operation_count`, workload totals, or workload rates.
Only its `peak_traced_memory_bytes` is paired with the logical sample. Peak traced memory measures
Python allocations attributable to that one `run()` interval; it is not process resident set size
and does not include every native-library allocation. This `paired_fresh_iteration` method keeps
allocation tracing overhead from distorting latency. The runner refuses to begin latency sampling
when `tracemalloc` is already active; stop external allocation tracing before running the suite. It
does not accept contaminated latency or stop a trace it does not own.

### Secondary workload observations

Workload observations describe units that the validated operation actually processed:

| Scenario | Per-operation observations |
| --- | --- |
| `record-keyset-pagination` | `records`, `pages` |
| both DataFrame scenarios | `records`, `cells`, `pages`, `metadata_requests` |
| `csv-sequential-export` | `records`, `pages`, `published_bytes` |
| `upsert-batch-planning` | `records`, `batches`, `serialized_bytes` |
| `attachment-concurrent-transfer` | `attachments`, `bytes` |
| `backup-read-verify-dataframe` | `records`, `artifacts`, `artifact_bytes` |
| `schema-readonly-planning` | `resources`, `metadata_requests` |

The stable map is stored as `workload_observations_per_operation`. The raw totals and derived rates
are secondary context, not independently timed metrics or additional trials. Every rate uses the
same `total_measured_seconds` denominator as the primary throughput sample. For example, pages per
second can explain a pagination result, but it is mathematically derived from that result's page
count and duration; do not present it as separate corroborating evidence.

For latency, throughput, total measured duration, operation count, observation rates, and any
measured peak memory, `summary` reports `median`, `min`, `max`, sample standard deviation (`stdev`),
and `coefficient_of_variation`. It separately records `sampling_cap_reached_count`. `unit_count`
and `unit_label` retain the primary throughput denominator, while `shape` supplies additional size
and request context. Use latency, primary throughput, or peak memory as the comparison metric, then
inspect raw samples, ranges, coefficients of variation, operation counts, cap state, and secondary
observations. A percentage change from a noisy run is not a reliable regression. Repeat the suite
on an otherwise idle machine when variance is unexpectedly high or current and baseline spreads
substantially overlap.

## Result and baseline contract

The committed baseline is:

```text
benchmarks/baselines/qbvisor-0.3.1.json
```

It uses benchmark schema `1.1` and is a reviewable record of qbvisor `0.3.1` on the environment
identified inside that artifact. It is not a portable expectation for every contributor machine.

The top-level JSON object contains:

| Key | Meaning |
| --- | --- |
| `schema_version` | Version of the benchmark artifact contract. |
| `generated_at_utc` | UTC creation timestamp; context only, not a comparison key. |
| `environment` | Environment identity and performance-clock details. |
| `configuration` | Warmup and logical-sample counts, duration target, operation cap, memory method, and selected scenario and profile names. |
| `results` | One result for every selected scenario/profile pair. |
| `comparison` | Optional signal-only comparison created when `--baseline` is supplied. |

Each result contains `scenario`, `description`, `profile`, `profile_shape`, `shape`, `unit_count`,
`unit_label`, `workload_observations_per_operation`, `memory_measurement`, raw `samples`, and
`summary`. Configuration records `min_sample_seconds` and `max_operations_per_sample` alongside
warmups and repeats. The configuration and each memory-measured result identify the memory method
as `paired_fresh_iteration`.

An optional comparison uses comparison schema `1.1`; its contract is versioned independently of
the benchmark artifact schema. For each metric, `baseline` and `current` are the medians;
`delta_percent` records
their change; `baseline_range` and `current_range` record the inclusive minimum and maximum raw
samples; and `threshold_crossed` plus `sample_ranges_overlap` expose the evidence used to assign
`stable`, `regression_signal`, `improvement_signal`, or `unavailable`. Each pair includes
`unavailable_reasons`. The comparison also includes `environment_compatible` and
`environment_differences`, `report_compatible` and `report_differences`, `qbvisor_versions`, and
`source_states`, `source_reproducible`, and `source_warnings`. Latency and memory are better when
lower; throughput is better when higher. `comparison.signal_only` is always `true`.

`environment` records the qbvisor version; Python implementation and version; the installed
versions of aiofiles, aiohttp, pandas, python-dotenv, and requests; operating-system, release,
machine architecture, processor identity, and logical CPU count; and the `perf_counter`
implementation, monotonic and adjustable flags, and resolution. `source` adds the 40-character Git
revision and whether the working tree was dirty when measurement began. It deliberately omits the
branch and checkout path, hostnames, user names, environment variables, realm and application
identities, credentials, and data values.

## Comparison rules

Generate the same full result with the published baseline attached:

```bash
uv run python -m benchmarks.run \
  --profile small --profile medium --profile large \
  --warmups 1 --repeats 5 \
  --min-sample-seconds 0.100 --max-operations-per-sample 10000 \
  --output .qbvisor/benchmarks/current-vs-0.3.1.json \
  --baseline benchmarks/baselines/qbvisor-0.3.1.json
```

The default `--regression-percent 15` makes a median change at or beyond 15 percent necessary, but
not sufficient, for a signal. A `regression_signal` or `improvement_signal` requires both that
threshold crossing and non-overlapping raw sample ranges in the same quality direction. Range
comparison is inclusive: ranges that touch at an endpoint overlap. A threshold-crossing median
with overlapping ranges is classified `stable`, while `threshold_crossed: true` and
`sample_ranges_overlap: true` preserve the reason for inspection. This avoids promoting a noisy
median-only movement to a signal.

`stable` means **no qualified signal under this rule**. It is not proof of equal or equivalent
performance, and it does not rule out a smaller or noisier change. Inspect the medians, raw ranges,
individual samples, and coefficients of variation, then rerun on the same idle environment when
the distinction matters. Changing `--regression-percent` changes only the annotations in
`comparison`; a regression signal never changes the command's exit status.

The runner marks a pair `unavailable` unless all of these identities form a matched measurement
protocol:

1. Both reports use benchmark schema `1.1` and the same warmup count, logical-sample repeat count,
   `min_sample_seconds`, `max_operations_per_sample`, and `paired_fresh_iteration` memory method.
2. The scenario/profile pair has identical `profile_shape`, scenario `shape`, `unit_count`,
   `unit_label`, `workload_observations_per_operation`, and result `memory_measurement`, with the
   configured number of raw samples.
3. Python implementation and full version; every recorded dependency version; operating system
   and release; machine architecture, processor identity, and logical CPU count; and the complete
   performance-clock identity match exactly.

`environment_compatible` and `report_compatible` summarize the first report-wide checks. Each
ineligible pair carries its specific environment, report, or workload explanations in
`unavailable_reasons`.

The realized `operation_count` may differ between otherwise compatible samples because each one
adapts to the shared duration target. The runner validates that every sample's per-operation
latency, total duration, operation count, cap state, observation totals, and observation rates are
internally consistent. Comparison signals still use only the per-operation latency, primary
throughput, and peak-memory samples. Observation rates remain explanatory context.

The qbvisor version and source revision may differ because the code change is normally the intended
independent variable. If Python, pandas, another dependency, the operating system, or the machine
is also being changed intentionally, treat that as a separate experiment and create a baseline in
that same environment. The runner retains metric values from incompatible artifacts for
inspection, but classifies the comparison as unavailable and explains every environment, report,
or workload difference.

Source revision and dirty state are experimental context, not environment-compatibility gates.
`source_states` exposes both report identities, while `source_reproducible` and `source_warnings`
make missing revisions and uncommitted changes explicit. A dirty-tree result may therefore compare
when its machine, runtime, workload, and measurement method match. Review `source.revision` and
`source.dirty` before drawing a conclusion: a dirty result is less exactly reconstructible because
the revision does not identify its uncommitted changes.

Initial comparisons report evidence only. Do not make the signal percentage a CI threshold or
discard a result solely because its median crossed that line. Review repeat variance, the actual
code path, and a same-environment rerun first.

## Interpretation boundaries

The suite protects current documented behavior. It must not turn a deliberate contract into a
performance defect:

- **Complete DataFrames materialize** all requested rows in memory. Peak memory is expected to
  scale with result shape; callers use `top` or CSV export when they need a bounded workflow.
- **Keyset CSV pages are sequential** in stable Record ID# order. The benchmark validates cursor
  progress and does not pass the compatibility-only `max_concurrency` argument. A lack of
  concurrent page requests is not a regression.
- **Backup capture remains whole-application**, but the credential-free local benchmark reads a
  deterministic archive. It times public open, integrity verification, and DataFrame reading; it
  does not pretend that a scoped live backup exists.
- **Schema planning reads metadata only**. The scenario permits metadata reads and rejects record
  requests, schema mutations, and local state publication.
- **Mutations are not replayed** after uncertain failures. Upsert benchmarking stops at real
  serialization and batch planning; it never sends or replays a mutation merely to obtain timing
  data.

Attachment timing uses deterministic in-process transfer behavior and local atomic file writes.
It does not establish a Quickbase download-service target or alter the retained synchronous public
entry points.

## Evidence layers

The local suite, sandbox stabilization workload, and operational canary answer different
questions:

| Layer | Data and credentials | Purpose | Timing interpretation |
| --- | --- | --- | --- |
| Local benchmark | Deterministic fakes and files; no credentials | Repeatable implementation comparison across all seven surfaces | Compare compatible environment identities and repeat variance; signal only |
| Sandbox stabilization workload | Generated rows plus the entire dedicated sandbox application | Validate representative volume, results, backup consistency, and cleanup | Live diagnostics with service, network, and application-size variability; never CI thresholds |
| Operational canary | Persistent dedicated-sandbox fixtures and narrowly owned mutations | Detect whether five supported workflows still function against Quickbase | Live diagnostic smoke with a whole-job safety timeout, not a benchmark or latency target |

Do not substitute local benchmark numbers for live-service evidence, and do not use a live
Quickbase duration as a machine-comparable benchmark. Quickbase load, rate limiting, network path,
application size, formula evaluation, and concurrent application changes can all alter a live
timing. The [sandbox stabilization workload](development-workloads.md) and [operational
canary](operational-testing.md) retain those denominators and safety boundaries for diagnosis.
