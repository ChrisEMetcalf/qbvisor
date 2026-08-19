from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
GUIDE_PATH = PROJECT_ROOT / "docs" / "performance-benchmarks.md"

SURFACES = (
    "pagination",
    "DataFrame conversion",
    "CSV export",
    "upsert batching",
    "attachment transfer",
    "backup reads",
    "schema planning",
)
SCENARIOS = (
    "attachment-concurrent-transfer",
    "backup-read-verify-dataframe",
    "csv-sequential-export",
    "dataframe-materialization-cold-metadata",
    "dataframe-materialization-warm-metadata",
    "record-keyset-pagination",
    "schema-readonly-planning",
    "upsert-batch-planning",
)


def _section(document: str, heading: str) -> str:
    start = document.index(heading)
    next_heading = document.find("\n## ", start + len(heading))
    return document[start:] if next_heading == -1 else document[start:next_heading]


def test_performance_guide_is_linked_from_the_site_and_readme():
    navigation = (PROJECT_ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")

    assert "Local performance benchmarks: performance-benchmarks.md" in navigation
    assert "[Reproducible local performance benchmarks](docs/performance-benchmarks.md)" in readme


def test_performance_guide_covers_every_issue_24_surface_and_baseline_contract():
    guide = GUIDE_PATH.read_text(encoding="utf-8")
    gitignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")

    for surface in SURFACES:
        assert surface in guide
    for scenario in SCENARIOS:
        assert scenario in guide
    assert "uv run python -m benchmarks.run" in guide
    assert "--min-sample-seconds 0.100" in guide
    assert "--max-operations-per-sample 10000" in guide
    assert "credential-free" in guide
    assert "benchmarks/baselines/qbvisor-0.3.1.json" in guide
    assert ".qbvisor/" in gitignore
    assert "!benchmarks/baselines/*.json" in gitignore
    for result_key in (
        "schema_version",
        "generated_at_utc",
        "environment",
        "configuration",
        "results",
        "comparison",
        "memory_measurement",
        "workload_observations_per_operation",
    ):
        assert f"`{result_key}`" in guide
    assert "benchmark schema `1.1`" in guide
    assert "environment identity" in guide.lower()
    assert "repeat variance" in guide.lower()


def test_performance_guide_preserves_intentional_runtime_boundaries():
    boundaries = _section(
        GUIDE_PATH.read_text(encoding="utf-8"), "## Interpretation boundaries"
    ).lower()

    for distinction in (
        "complete dataframes materialize",
        "keyset csv pages are sequential",
        "backup capture remains whole-application",
        "schema planning reads metadata only",
        "mutations are not replayed",
    ):
        assert distinction in boundaries


def test_performance_guide_keeps_live_evidence_out_of_local_comparisons():
    evidence = _section(GUIDE_PATH.read_text(encoding="utf-8"), "## Evidence layers").lower()

    assert "sandbox stabilization workload" in evidence
    assert "operational canary" in evidence
    assert "service, network, and application-size variability" in evidence
    assert "never ci thresholds" in evidence


def test_performance_guide_requires_compatible_comparison_identity():
    comparison = " ".join(
        _section(GUIDE_PATH.read_text(encoding="utf-8"), "## Comparison rules").lower().split()
    )

    for contract in (
        "profile_shape",
        "paired_fresh_iteration",
        "processor identity",
        "source revision and dirty state",
        "classifies the comparison as unavailable",
        "environment_compatible",
        "report_compatible",
        "source_reproducible",
        "unavailable_reasons",
        "min_sample_seconds",
        "max_operations_per_sample",
        "workload_observations_per_operation",
        "matched measurement protocol",
    ):
        assert contract in comparison


def test_performance_guide_explains_signal_qualification_without_claiming_equivalence():
    guide = GUIDE_PATH.read_text(encoding="utf-8")
    result_contract = _section(guide, "## Result and baseline contract").lower()
    comparison = " ".join(_section(guide, "## Comparison rules").lower().split())

    for evidence_field in (
        "baseline_range",
        "current_range",
        "sample_ranges_overlap",
        "threshold_crossed",
    ):
        assert f"`{evidence_field}`" in result_contract
    assert "comparison schema `1.1`" in result_contract
    assert "ranges that touch at an endpoint overlap" in comparison
    assert "no qualified signal under this rule" in comparison
    assert "not proof of equal or equivalent performance" in comparison


def test_performance_guide_distinguishes_duration_samples_and_secondary_observations():
    metrics = " ".join(
        _section(GUIDE_PATH.read_text(encoding="utf-8"), "## Metrics and repeat variance")
        .lower()
        .split()
    )

    for sample_field in (
        "total_measured_seconds",
        "operation_count",
        "sampling_cap_reached",
        "sampling_cap_reached_count",
        "workload_observation_totals",
        "workload_observation_rates_per_second",
    ):
        assert f"`{sample_field}`" in metrics
    assert "start the monotonic `perf_counter` immediately before `run()`" in metrics
    assert "validation, observation, cleanup, and the gaps" in metrics
    assert "do not contribute to measured duration" in metrics
    assert "one additional fresh prepare/run/validate/observe/cleanup operation" in metrics
    assert "does not contribute to `operation_count`, workload totals, or workload rates" in metrics
    assert "refuses to begin latency sampling when `tracemalloc` is already active" in metrics
    assert "not independently timed metrics or additional trials" in metrics
    assert "same `total_measured_seconds` denominator" in metrics
