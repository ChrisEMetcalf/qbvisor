from __future__ import annotations

import json
import tracemalloc
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from benchmarks import core
from benchmarks import run as benchmark_run
from benchmarks.core import BenchmarkContractError
from benchmarks.profiles import PROFILES, BenchmarkProfile, get_profile


@dataclass
class Prepared:
    events: list[str]
    shape: dict[str, int] = field(default_factory=lambda: {"records": 8})
    unit_count: int = 8
    unit_label: str = "records"
    fail_at: str | None = None
    observation_count: int | float | None = None

    def run(self) -> object:
        self.events.append("run")
        if self.fail_at == "run":
            raise RuntimeError("run failed")
        return list(range(self.unit_count))

    def validate(self, result: object) -> None:
        self.events.append("validate")
        if self.fail_at == "validate":
            raise RuntimeError("validation failed")
        assert len(result) == self.unit_count  # type: ignore[arg-type]

    def workload_observations(self, result: object) -> dict[str, int | float]:
        self.events.append("observe")
        return {
            "items": self.unit_count if self.observation_count is None else self.observation_count,
            "zero_events": 0,
        }

    def cleanup(self) -> None:
        self.events.append("cleanup")
        if self.fail_at == "cleanup":
            raise RuntimeError("cleanup failed")


@dataclass
class Scenario:
    events: list[str] = field(default_factory=list)
    name: str = "example"
    description: str = "Deterministic example"
    track_peak_memory: bool = False
    fail_at: str | None = None
    identities: list[int] = field(default_factory=list)
    observation_counts: list[int | float] = field(default_factory=list)

    def prepare(self, profile: BenchmarkProfile) -> core.PreparedScenario:
        self.events.append(f"prepare:{profile.name}")
        identity = self.identities.pop(0) if self.identities else 8
        observation_count = self.observation_counts.pop(0) if self.observation_counts else identity
        return Prepared(
            self.events,
            shape={"records": identity},
            unit_count=identity,
            fail_at=self.fail_at,
            observation_count=observation_count,
        )


def _result_with_medians(
    latency: float,
    _throughput: float,
    memory: float | None,
    *,
    shape_records: int = 8,
    latency_samples: tuple[float, float] | None = None,
    memory_samples: tuple[float, float] | None = None,
) -> dict[str, Any]:
    def metric(value: float) -> dict[str, float]:
        return {
            "median": value,
            "min": value,
            "max": value,
            "stdev": 0.0,
            "coefficient_of_variation": 0.0,
        }

    latency_values = latency_samples or (latency, latency)
    throughput_values = tuple(shape_records / value for value in latency_values)
    memory_values: tuple[float | None, float | None] = (
        memory_samples if memory_samples is not None else (memory, memory)
    )
    return {
        "scenario": "example",
        "description": "Deterministic example",
        "profile": "small",
        "profile_shape": PROFILES["small"].as_dict(),
        "shape": {"records": shape_records},
        "unit_count": shape_records,
        "unit_label": "records",
        "workload_observations_per_operation": {},
        "memory_measurement": "paired_fresh_iteration",
        "samples": [
            {
                "elapsed_seconds": latency_values[index],
                "throughput_units_per_second": throughput_values[index],
                "total_measured_seconds": latency_values[index],
                "operation_count": 1,
                "sampling_cap_reached": False,
                "peak_traced_memory_bytes": memory_values[index],
                "workload_observation_totals": {},
                "workload_observation_rates_per_second": {},
            }
            for index in range(2)
        ],
        "summary": {
            "latency_seconds": metric(latency),
            "throughput_units_per_second": metric(sum(throughput_values) / 2),
            "peak_traced_memory_bytes": metric(memory) if memory is not None else None,
        },
    }


def _comparison_environment(
    *,
    python_version: str = "3.13.4",
    qbvisor_version: str = "0.3.1",
    revision: str = "a" * 40,
) -> dict[str, Any]:
    return {
        "qbvisor_version": qbvisor_version,
        "python": {"implementation": "CPython", "version": python_version},
        "dependencies": {name: "1.2.3" for name in core.DEPENDENCIES},
        "platform": {
            "system": "ExampleOS",
            "machine": "x86_64",
            "release": "1",
            "processor": "Example CPU",
            "cpu_logical_count": 8,
        },
        "source": {"revision": revision, "dirty": False},
        "clock": {
            "implementation": "example-clock",
            "monotonic": True,
            "adjustable": False,
            "resolution_seconds": 1e-09,
        },
    }


def _comparison_report(
    result: dict[str, Any],
    *,
    environment: dict[str, Any] | None = None,
    warmups: int = 1,
    repeats: int = 2,
) -> dict[str, Any]:
    return {
        "schema_version": core.BENCHMARK_SCHEMA_VERSION,
        "environment": environment or _comparison_environment(),
        "configuration": {
            "warmups": warmups,
            "repeats": repeats,
            "min_sample_seconds": 0.1,
            "max_operations_per_sample": 10_000,
            "memory_measurement": "paired_fresh_iteration",
        },
        "results": [result],
    }


def _replace_observation_identity(
    report: dict[str, Any], observations: dict[str, int | float]
) -> None:
    result = report["results"][0]
    result["workload_observations_per_operation"] = observations
    for sample in result["samples"]:
        count = sample["operation_count"]
        total_seconds = sample["total_measured_seconds"]
        totals = {key: value * count for key, value in observations.items()}
        sample["workload_observation_totals"] = totals
        sample["workload_observation_rates_per_second"] = {
            key: value / total_seconds for key, value in totals.items()
        }


def test_profiles_are_deterministic_and_large_upsert_crosses_40_mb():
    assert list(PROFILES) == ["small", "medium", "large"]
    assert [profile.record_count for profile in PROFILES.values()] == [100, 1_000, 10_000]
    assert [profile.page_size for profile in PROFILES.values()] == [25, 100, 250]
    assert get_profile(" MEDIUM ") is PROFILES["medium"]
    assert (
        PROFILES["medium"].upsert_record_count * PROFILES["medium"].upsert_value_bytes < 40_000_000
    )
    assert PROFILES["large"].upsert_record_count * PROFILES["large"].upsert_value_bytes > 40_000_000


def test_unknown_profile_explains_choices():
    with pytest.raises(ValueError, match="small, medium, large"):
        get_profile("production")


def test_profile_rejects_api_invalid_or_unhelpful_page_shapes():
    values = PROFILES["small"].as_dict()
    with pytest.raises(ValueError, match="1,000"):
        BenchmarkProfile(**{**values, "page_size": 1_001})
    with pytest.raises(ValueError, match="record_count"):
        BenchmarkProfile(**{**values, "record_count": 10, "page_size": 25})


def test_measurement_keeps_prepare_validate_cleanup_outside_timer(monkeypatch):
    scenario = Scenario()
    clock_values = iter([2.0, 4.0, 5.0, 9.0, 10.0, 18.0])
    monkeypatch.setattr(core, "perf_counter", lambda: next(clock_values))

    result = core.measure_scenario(
        scenario,
        PROFILES["small"],
        warmups=1,
        repeats=3,
    )

    assert (
        scenario.events
        == [
            "prepare:small",
            "run",
            "validate",
            "observe",
            "cleanup",
        ]
        * 4
    )
    assert result["samples"] == [
        {
            "elapsed_seconds": 2.0,
            "throughput_units_per_second": 4.0,
            "total_measured_seconds": 2.0,
            "operation_count": 1,
            "sampling_cap_reached": False,
            "peak_traced_memory_bytes": None,
            "workload_observation_totals": {"items": 8, "zero_events": 0},
            "workload_observation_rates_per_second": {
                "items": 4.0,
                "zero_events": 0.0,
            },
        },
        {
            "elapsed_seconds": 4.0,
            "throughput_units_per_second": 2.0,
            "total_measured_seconds": 4.0,
            "operation_count": 1,
            "sampling_cap_reached": False,
            "peak_traced_memory_bytes": None,
            "workload_observation_totals": {"items": 8, "zero_events": 0},
            "workload_observation_rates_per_second": {
                "items": 2.0,
                "zero_events": 0.0,
            },
        },
        {
            "elapsed_seconds": 8.0,
            "throughput_units_per_second": 1.0,
            "total_measured_seconds": 8.0,
            "operation_count": 1,
            "sampling_cap_reached": False,
            "peak_traced_memory_bytes": None,
            "workload_observation_totals": {"items": 8, "zero_events": 0},
            "workload_observation_rates_per_second": {
                "items": 1.0,
                "zero_events": 0.0,
            },
        },
    ]
    assert result["workload_observations_per_operation"] == {"items": 8, "zero_events": 0}
    assert result["summary"]["latency_seconds"]["median"] == 4.0
    assert result["summary"]["latency_seconds"]["min"] == 2.0
    assert result["summary"]["latency_seconds"]["max"] == 8.0
    assert result["summary"]["latency_seconds"]["stdev"] == pytest.approx(3.0550504633)
    assert result["summary"]["latency_seconds"]["coefficient_of_variation"] == pytest.approx(
        0.6546536707
    )
    assert result["summary"]["throughput_units_per_second"]["median"] == 2.0
    assert result["summary"]["total_measured_seconds"]["median"] == 4.0
    assert result["summary"]["operation_count"]["median"] == 1
    assert result["summary"]["sampling_cap_reached_count"] == 0
    assert result["summary"]["workload_observation_rates_per_second"]["items"]["median"] == 2.0
    assert result["summary"]["workload_observation_rates_per_second"]["zero_events"] == {
        "median": 0.0,
        "min": 0.0,
        "max": 0.0,
        "stdev": 0.0,
        "coefficient_of_variation": 0.0,
    }
    assert result["summary"]["peak_traced_memory_bytes"] is None


def test_logical_samples_aggregate_fresh_operations_until_minimum_duration(monkeypatch):
    scenario = Scenario()
    clock_values = iter(
        [
            0.0,
            0.04,
            1.0,
            1.04,
            2.0,
            2.04,
            3.0,
            3.06,
            4.0,
            4.06,
        ]
    )
    monkeypatch.setattr(core, "perf_counter", lambda: next(clock_values))

    result = core.measure_scenario(
        scenario,
        PROFILES["small"],
        warmups=0,
        repeats=2,
        min_sample_seconds=0.1,
        max_operations_per_sample=10,
    )

    first, second = result["samples"]
    assert [first["operation_count"], second["operation_count"]] == [3, 2]
    assert first["total_measured_seconds"] == pytest.approx(0.12)
    assert second["total_measured_seconds"] == pytest.approx(0.12)
    assert first["elapsed_seconds"] == pytest.approx(0.04)
    assert second["elapsed_seconds"] == pytest.approx(0.06)
    assert first["throughput_units_per_second"] == pytest.approx(200.0)
    assert second["throughput_units_per_second"] == pytest.approx(400 / 3)
    assert first["workload_observation_totals"] == {"items": 24, "zero_events": 0}
    assert scenario.events.count("prepare:small") == 5
    assert scenario.events.count("cleanup") == 5
    assert result["summary"]["operation_count"] == {
        "median": 2.5,
        "min": 2,
        "max": 3,
        "stdev": pytest.approx(2**-0.5),
        "coefficient_of_variation": pytest.approx((2**-0.5) / 2.5),
    }


def test_logical_sample_stops_at_safety_cap_and_reports_it(monkeypatch):
    scenario = Scenario()
    clock_values = iter([0.0, 0.01, 1.0, 1.01, 2.0, 2.01])
    monkeypatch.setattr(core, "perf_counter", lambda: next(clock_values))

    result = core.measure_scenario(
        scenario,
        PROFILES["small"],
        warmups=0,
        repeats=1,
        min_sample_seconds=1.0,
        max_operations_per_sample=3,
    )

    sample = result["samples"][0]
    assert sample["operation_count"] == 3
    assert sample["total_measured_seconds"] == pytest.approx(0.03)
    assert sample["sampling_cap_reached"] is True
    assert result["summary"]["sampling_cap_reached_count"] == 1
    assert scenario.events.count("cleanup") == 3


def test_latency_operations_are_untraced_and_memory_uses_one_fresh_operation():
    trace_states: list[bool] = []
    events: list[str] = []

    class TracePrepared(Prepared):
        def run(self) -> object:
            trace_states.append(tracemalloc.is_tracing())
            return super().run()

    class TraceScenario:
        name = "trace-state"
        description = "Record whether run executes under tracemalloc"
        track_peak_memory = True

        def prepare(self, profile: BenchmarkProfile) -> core.PreparedScenario:
            events.append(f"prepare:{profile.name}")
            return TracePrepared(events)

    core.measure_scenario(
        TraceScenario(),
        PROFILES["small"],
        warmups=0,
        repeats=1,
        min_sample_seconds=1e-9,
    )

    assert trace_states == [False, True]
    assert events.count("cleanup") == 2


def test_preexisting_tracemalloc_is_rejected_before_latency_run_and_still_cleans():
    scenario = Scenario()
    assert not tracemalloc.is_tracing()
    tracemalloc.start()
    try:
        with pytest.raises(BenchmarkContractError, match="tracemalloc to be stopped"):
            core.measure_scenario(
                scenario,
                PROFILES["small"],
                warmups=0,
                repeats=1,
                min_sample_seconds=1e-9,
            )
    finally:
        tracemalloc.stop()

    assert "run" not in scenario.events
    assert scenario.events[-1] == "cleanup"


def test_aggregated_failure_cleans_each_prepared_operation(monkeypatch):
    events: list[str] = []
    prepare_count = 0

    class SecondRunFails:
        name = "second-run-fails"
        description = "Fail after one successful aggregated operation"
        track_peak_memory = False

        def prepare(self, profile: BenchmarkProfile) -> core.PreparedScenario:
            nonlocal prepare_count
            prepare_count += 1
            events.append(f"prepare:{profile.name}")
            return Prepared(events, fail_at="run" if prepare_count == 2 else None)

    clock_values = iter([0.0, 0.04, 1.0])
    monkeypatch.setattr(core, "perf_counter", lambda: next(clock_values))

    with pytest.raises(RuntimeError, match="run failed"):
        core.measure_scenario(
            SecondRunFails(),
            PROFILES["small"],
            warmups=0,
            repeats=1,
            min_sample_seconds=0.1,
        )

    assert prepare_count == 2
    assert events.count("cleanup") == 2


def test_shape_drift_within_aggregated_sample_is_rejected_after_cleanup(monkeypatch):
    scenario = Scenario(identities=[8, 9])
    clock_values = iter([0.0, 0.04, 1.0, 1.04])
    monkeypatch.setattr(core, "perf_counter", lambda: next(clock_values))

    with pytest.raises(BenchmarkContractError, match="changed shape"):
        core.measure_scenario(
            scenario,
            PROFILES["small"],
            warmups=0,
            repeats=1,
            min_sample_seconds=0.1,
        )

    assert scenario.events.count("cleanup") == 2


def test_observation_drift_within_aggregated_sample_is_rejected_after_cleanup(monkeypatch):
    scenario = Scenario(observation_counts=[8, 9])
    clock_values = iter([0.0, 0.04, 1.0, 1.04])
    monkeypatch.setattr(core, "perf_counter", lambda: next(clock_values))

    with pytest.raises(BenchmarkContractError, match="changed workload observations"):
        core.measure_scenario(
            scenario,
            PROFILES["small"],
            warmups=0,
            repeats=1,
            min_sample_seconds=0.1,
        )

    assert scenario.events.count("cleanup") == 2


def test_integer_float_and_zero_observations_remain_strict_json_safe(monkeypatch):
    scenario = Scenario(observation_counts=[2.5])
    clock_values = iter([0.0, 1.0])
    monkeypatch.setattr(core, "perf_counter", lambda: next(clock_values))

    result = core.measure_scenario(
        scenario,
        PROFILES["small"],
        warmups=0,
        repeats=1,
        min_sample_seconds=0.1,
    )

    assert result["workload_observations_per_operation"] == {
        "items": 2.5,
        "zero_events": 0,
    }
    assert result["samples"][0]["workload_observation_totals"] == {
        "items": 2.5,
        "zero_events": 0,
    }
    assert result["samples"][0]["workload_observation_rates_per_second"] == {
        "items": 2.5,
        "zero_events": 0.0,
    }
    json.dumps(result, allow_nan=False)


@pytest.mark.parametrize("failure", ["run", "validate"])
def test_failure_still_cleans_up(failure):
    scenario = Scenario(fail_at=failure)

    with pytest.raises(RuntimeError, match="failed"):
        core.measure_scenario(scenario, PROFILES["small"], warmups=0, repeats=1)

    assert scenario.events[-1] == "cleanup"
    assert scenario.events.count("cleanup") == 1


def test_contract_failure_after_prepare_still_cleans_up():
    scenario = Scenario()
    scenario.identities = [0]

    with pytest.raises(BenchmarkContractError, match="unit_count"):
        core.measure_scenario(scenario, PROFILES["small"], warmups=0, repeats=1)

    assert scenario.events[-1] == "cleanup"


def test_changing_shape_is_rejected_after_each_iteration_is_cleaned():
    scenario = Scenario(identities=[8, 9])

    with pytest.raises(BenchmarkContractError, match="changed shape"):
        core.measure_scenario(scenario, PROFILES["small"], warmups=1, repeats=1)

    assert scenario.events.count("cleanup") == 2


def test_tracemalloc_peak_is_included_when_scenario_requests_it():
    scenario = Scenario(track_peak_memory=True)

    result = core.measure_scenario(
        scenario,
        PROFILES["small"],
        warmups=0,
        repeats=1,
        min_sample_seconds=1e-9,
    )

    peak = result["samples"][0]["peak_traced_memory_bytes"]
    assert isinstance(peak, int)
    assert peak >= 0
    assert result["summary"]["peak_traced_memory_bytes"]["median"] == peak
    assert result["memory_measurement"] == "paired_fresh_iteration"
    assert scenario.events.count("prepare:small") == 2
    assert scenario.events.count("cleanup") == 2


def test_paired_memory_iteration_must_have_same_identity():
    scenario = Scenario(track_peak_memory=True, identities=[8, 9])

    with pytest.raises(BenchmarkContractError, match="changed shape"):
        core.measure_scenario(
            scenario,
            PROFILES["small"],
            warmups=0,
            repeats=1,
            min_sample_seconds=1e-9,
        )

    assert scenario.events.count("cleanup") == 2


def test_prepare_failure_is_the_scenario_transaction_boundary():
    events: list[str] = []

    class PrepareFailure:
        name = "prepare-failure"
        description = "Exercise the documented transaction boundary"
        track_peak_memory = False

        def prepare(self, profile: BenchmarkProfile) -> core.PreparedScenario:
            events.append(f"prepare:{profile.name}")
            raise RuntimeError("prepare rolled back its own resources")

    with pytest.raises(RuntimeError, match="rolled back"):
        core.measure_scenario(PrepareFailure(), PROFILES["small"], warmups=0, repeats=1)

    assert events == ["prepare:small"]


def test_run_benchmarks_sorts_pairs_and_reports_sanitized_environment(monkeypatch):
    monkeypatch.setenv("QB_REALM_API_KEY", "QB-USER-TOKEN top-secret")
    monkeypatch.setenv("USER", "sensitive-user")
    monkeypatch.setenv("HOSTNAME", "sensitive-host")
    report = core.run_benchmarks(
        [Scenario(name="zeta"), Scenario(name="alpha")],
        [PROFILES["small"]],
        warmups=0,
        repeats=1,
        min_sample_seconds=1e-9,
    )

    assert report["schema_version"] == core.BENCHMARK_SCHEMA_VERSION
    assert [result["scenario"] for result in report["results"]] == ["alpha", "zeta"]
    assert report["configuration"] == {
        "warmups": 0,
        "repeats": 1,
        "min_sample_seconds": 1e-9,
        "max_operations_per_sample": core.DEFAULT_MAX_OPERATIONS_PER_SAMPLE,
        "memory_measurement": "paired_fresh_iteration",
        "scenarios": ["alpha", "zeta"],
        "profiles": ["small"],
    }
    environment = report["environment"]
    assert set(environment) == {
        "qbvisor_version",
        "python",
        "dependencies",
        "platform",
        "clock",
        "source",
    }
    assert environment["platform"]["cpu_logical_count"]
    assert environment["platform"]["processor"]
    assert set(environment["source"]) == {"revision", "dirty"}
    encoded = json.dumps(environment)
    assert "top-secret" not in encoded
    assert "sensitive-user" not in encoded
    assert "sensitive-host" not in encoded
    assert str(Path.cwd()) not in encoded


def test_compare_results_labels_regressions_as_signal_only():
    baseline = _comparison_report(_result_with_medians(1.0, 100.0, 1_000.0))
    current = _comparison_report(
        _result_with_medians(1.2, 80.0, 1_200.0),
        environment=_comparison_environment(
            qbvisor_version="0.4.0",
            revision="b" * 40,
        ),
    )

    comparison = core.compare_results(current, baseline, regression_percent=15.0)

    assert comparison["schema_version"] == "1.1"
    assert comparison["signal_only"] is True
    assert comparison["environment_compatible"] is True
    assert comparison["environment_differences"] == []
    assert comparison["report_compatible"] is True
    assert comparison["report_differences"] == []
    assert comparison["qbvisor_versions"] == {"baseline": "0.3.1", "current": "0.4.0"}
    assert comparison["source_states"] == {
        "baseline": {"revision": "a" * 40, "dirty": False},
        "current": {"revision": "b" * 40, "dirty": False},
    }
    assert comparison["source_reproducible"] is True
    assert comparison["source_warnings"] == []
    assert comparison["regression_threshold_percent"] == 15.0
    result = comparison["results"][0]
    assert result["classification"] == "regression_signal"
    assert result["unavailable_reasons"] == []
    assert result["metrics"]["latency_seconds"]["delta_percent"] == pytest.approx(20.0)
    assert result["metrics"]["latency_seconds"]["baseline_range"] == {
        "min": 1.0,
        "max": 1.0,
    }
    assert result["metrics"]["latency_seconds"]["current_range"] == {
        "min": 1.2,
        "max": 1.2,
    }
    assert result["metrics"]["latency_seconds"]["sample_ranges_overlap"] is False
    assert result["metrics"]["latency_seconds"]["threshold_crossed"] is True
    assert result["metrics"]["throughput_units_per_second"]["delta_percent"] == pytest.approx(
        -100 / 6
    )
    assert result["metrics"]["peak_traced_memory_bytes"]["classification"] == ("regression_signal")


def test_threshold_crossed_with_overlapping_ranges_is_not_a_qualified_signal():
    baseline = _comparison_report(
        _result_with_medians(
            1.0,
            100.0,
            1_000.0,
            latency_samples=(0.8, 1.2),
        )
    )
    current = _comparison_report(
        _result_with_medians(
            1.2,
            100.0,
            1_000.0,
            latency_samples=(1.0, 1.4),
        )
    )

    comparison = core.compare_results(current, baseline)

    metric = comparison["results"][0]["metrics"]["latency_seconds"]
    assert metric["delta_percent"] == pytest.approx(20.0)
    assert metric["threshold_crossed"] is True
    assert metric["sample_ranges_overlap"] is True
    assert metric["classification"] == "stable"
    assert comparison["results"][0]["classification"] == "stable"


def test_threshold_crossed_with_directionally_disjoint_ranges_qualifies_signal():
    baseline = _comparison_report(
        _result_with_medians(
            1.0,
            100.0,
            1_000.0,
            latency_samples=(0.9, 1.1),
        )
    )
    current = _comparison_report(
        _result_with_medians(
            1.3,
            100.0,
            1_000.0,
            latency_samples=(1.2, 1.4),
        )
    )

    metric = core.compare_results(current, baseline)["results"][0]["metrics"]["latency_seconds"]

    assert metric["threshold_crossed"] is True
    assert metric["sample_ranges_overlap"] is False
    assert metric["classification"] == "regression_signal"


def test_directionally_disjoint_improvement_qualifies_signal():
    baseline = _comparison_report(
        _result_with_medians(
            1.0,
            100.0,
            1_000.0,
            latency_samples=(0.9, 1.1),
        )
    )
    current = _comparison_report(
        _result_with_medians(
            0.7,
            100.0,
            1_000.0,
            latency_samples=(0.6, 0.8),
        )
    )

    metric = core.compare_results(current, baseline)["results"][0]["metrics"]["latency_seconds"]

    assert metric["threshold_crossed"] is True
    assert metric["sample_ranges_overlap"] is False
    assert metric["classification"] == "improvement_signal"


def test_touching_sample_range_endpoints_count_as_overlap():
    baseline = _comparison_report(
        _result_with_medians(
            1.0,
            100.0,
            1_000.0,
            latency_samples=(0.9, 1.1),
        )
    )
    current = _comparison_report(
        _result_with_medians(
            1.2,
            100.0,
            1_000.0,
            latency_samples=(1.1, 1.3),
        )
    )

    metric = core.compare_results(current, baseline)["results"][0]["metrics"]["latency_seconds"]

    assert metric["threshold_crossed"] is True
    assert metric["sample_ranges_overlap"] is True
    assert metric["classification"] == "stable"


def test_dirty_source_is_comparable_but_explicitly_less_reproducible():
    baseline = _comparison_report(_result_with_medians(1.0, 100.0, 1_000.0))
    current_environment = _comparison_environment(revision="b" * 40)
    current_environment["source"]["dirty"] = True
    current = _comparison_report(
        _result_with_medians(1.2, 80.0, 1_200.0),
        environment=current_environment,
    )

    comparison = core.compare_results(current, baseline)

    assert comparison["environment_compatible"] is True
    assert comparison["results"][0]["classification"] == "regression_signal"
    assert comparison["source_reproducible"] is False
    assert comparison["source_states"]["current"]["dirty"] is True
    assert comparison["source_warnings"] == ["current source has uncommitted changes"]


@pytest.mark.parametrize(
    ("section", "replacement", "reason"),
    [
        ("python", {}, "Python metadata is incomplete"),
        ("platform", {"processor": "Example CPU", "cpu_logical_count": 8}, "platform system"),
        ("clock", {}, "clock metadata is incomplete"),
    ],
)
def test_malformed_equal_environment_sections_are_not_comparable(section, replacement, reason):
    current_environment = _comparison_environment()
    baseline_environment = _comparison_environment()
    current_environment[section] = replacement
    baseline_environment[section] = replacement
    current = _comparison_report(
        _result_with_medians(1.2, 80.0, 1_200.0),
        environment=current_environment,
    )
    baseline = _comparison_report(
        _result_with_medians(1.0, 100.0, 1_000.0),
        environment=baseline_environment,
    )

    comparison = core.compare_results(current, baseline)

    assert comparison["environment_compatible"] is False
    assert any(reason in item for item in comparison["environment_differences"])
    assert comparison["results"][0]["classification"] == "unavailable"


def test_compare_results_handles_missing_pair_and_missing_memory():
    current_result = _result_with_medians(1.0, 100.0, None)
    environment = _comparison_environment()
    current = _comparison_report(current_result, environment=environment)
    baseline = _comparison_report(_result_with_medians(1.0, 100.0, None), environment=environment)
    baseline["results"] = []
    comparison = core.compare_results(current, baseline)
    assert comparison["results"][0] == {
        "scenario": "example",
        "profile": "small",
        "classification": "unavailable",
        "metrics": {},
        "unavailable_reasons": ["matching baseline scenario/profile is missing"],
    }


def test_compare_results_disables_signals_for_incompatible_environment():
    baseline = _comparison_report(
        _result_with_medians(1.0, 100.0, 1_000.0),
        environment=_comparison_environment(python_version="3.12.9"),
    )
    current = _comparison_report(
        _result_with_medians(1.2, 80.0, 1_200.0),
        environment=_comparison_environment(python_version="3.13.4"),
    )

    comparison = core.compare_results(current, baseline)

    assert comparison["environment_compatible"] is False
    assert comparison["environment_differences"] == ["Python version differs"]
    result = comparison["results"][0]
    assert result["classification"] == "unavailable"
    assert {metric["classification"] for metric in result["metrics"].values()} == {"unavailable"}
    assert result["metrics"]["latency_seconds"]["delta_percent"] == pytest.approx(20.0)


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        (lambda report: report.update(schema_version="9.0"), "schema version"),
        (
            lambda report: report["configuration"].update(warmups=3),
            "configuration warmups differs",
        ),
        (
            lambda report: report["configuration"].update(min_sample_seconds=0.2),
            "configuration min_sample_seconds differs",
        ),
        (
            lambda report: report["configuration"].update(max_operations_per_sample=20),
            "configuration max_operations_per_sample differs",
        ),
        (
            lambda report: report["results"][0].update(shape={"records": 9}),
            "result identity shape differs",
        ),
        (
            lambda report: report["results"][0].update(unit_label="pages"),
            "result identity unit_label differs",
        ),
        (
            lambda report: report["results"][0].update(memory_measurement="same_iteration"),
            "result identity memory_measurement differs",
        ),
        (
            lambda report: _replace_observation_identity(report, {"requests": 1}),
            "result identity workload_observations_per_operation differs",
        ),
    ],
)
def test_compare_results_disables_signals_when_method_or_workload_differs(mutation, reason):
    baseline = _comparison_report(_result_with_medians(1.0, 100.0, 1_000.0))
    current = _comparison_report(_result_with_medians(1.2, 80.0, 1_200.0))
    mutation(baseline)

    comparison = core.compare_results(current, baseline)

    result = comparison["results"][0]
    assert result["classification"] == "unavailable"
    assert any(reason in item for item in result["unavailable_reasons"])


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1.0, True])
def test_compare_results_rejects_invalid_external_medians(invalid):
    baseline_result = _result_with_medians(1.0, 100.0, 1_000.0)
    baseline_result["summary"]["latency_seconds"]["median"] = invalid
    baseline = _comparison_report(baseline_result)
    current = _comparison_report(_result_with_medians(1.2, 80.0, 1_200.0))

    with pytest.raises(ValueError, match="median"):
        core.compare_results(current, baseline)


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1.0, 0.0, True])
def test_compare_results_rejects_invalid_external_raw_samples(invalid):
    baseline_result = _result_with_medians(1.0, 100.0, 1_000.0)
    baseline_result["samples"][0]["elapsed_seconds"] = invalid
    baseline = _comparison_report(baseline_result)
    current = _comparison_report(_result_with_medians(1.2, 80.0, 1_200.0))

    with pytest.raises(ValueError, match="raw sample"):
        core.compare_results(current, baseline)


def test_compare_results_rejects_summary_and_raw_null_mismatch():
    baseline_result = _result_with_medians(1.0, 100.0, None)
    baseline_result["samples"][0]["peak_traced_memory_bytes"] = 100
    baseline = _comparison_report(baseline_result)
    current = _comparison_report(_result_with_medians(1.2, 80.0, None))

    with pytest.raises(ValueError, match="cannot mix null and numeric"):
        core.compare_results(current, baseline)


def test_compare_results_rejects_throughput_inconsistent_with_duration_and_units():
    baseline_result = _result_with_medians(1.0, 100.0, 1_000.0)
    baseline_result["samples"][0]["throughput_units_per_second"] *= 2
    baseline = _comparison_report(baseline_result)
    current = _comparison_report(_result_with_medians(1.2, 80.0, 1_200.0))

    with pytest.raises(ValueError, match="throughput is inconsistent"):
        core.compare_results(current, baseline)


def test_compare_results_rejects_median_outside_raw_sample_range():
    baseline_result = _result_with_medians(1.0, 100.0, 1_000.0)
    baseline_result["summary"]["latency_seconds"]["median"] = 2.0
    baseline = _comparison_report(baseline_result)
    current = _comparison_report(_result_with_medians(1.2, 80.0, 1_200.0))

    with pytest.raises(ValueError, match="within its raw sample range"):
        core.compare_results(current, baseline)


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), -1.0, True, "1"])
def test_compare_results_rejects_invalid_external_observations(invalid):
    baseline_result = _result_with_medians(1.0, 100.0, 1_000.0)
    baseline_result["workload_observations_per_operation"] = {"requests": invalid}
    for sample in baseline_result["samples"]:
        sample["workload_observation_totals"] = {"requests": invalid}
        sample["workload_observation_rates_per_second"] = {"requests": invalid}
    baseline = _comparison_report(baseline_result)
    current = _comparison_report(_result_with_medians(1.2, 80.0, 1_200.0))

    with pytest.raises(ValueError, match="workload observation"):
        core.compare_results(current, baseline)


def test_write_json_report_uses_sorted_indented_strict_json(tmp_path: Path):
    destination = tmp_path / "nested" / "result.json"

    core.write_json_report({"z": 1, "a": {"d": 4, "b": 2}}, destination)

    assert destination.read_text(encoding="utf-8") == (
        '{\n  "a": {\n    "b": 2,\n    "d": 4\n  },\n  "z": 1\n}\n'
    )
    with pytest.raises(FileExistsError):
        core.write_json_report({"replacement": True}, destination)
    assert '"replacement"' not in destination.read_text(encoding="utf-8")
    assert not list(destination.parent.glob(f".{destination.name}.*.tmp"))


def test_write_json_report_rejects_nonfinite_data_without_leaving_a_file(tmp_path: Path):
    destination = tmp_path / "invalid.json"

    with pytest.raises(ValueError, match="Out of range"):
        core.write_json_report({"latency": float("nan")}, destination)

    assert not destination.exists()


def test_measurement_arguments_are_validated():
    with pytest.raises(ValueError, match="warmups"):
        core.measure_scenario(Scenario(), PROFILES["small"], warmups=-1, repeats=1)
    with pytest.raises(ValueError, match="repeats"):
        core.measure_scenario(Scenario(), PROFILES["small"], warmups=0, repeats=0)
    for invalid in (0.0, float("nan"), float("inf"), True):
        with pytest.raises(ValueError, match="min_sample_seconds"):
            core.measure_scenario(
                Scenario(),
                PROFILES["small"],
                warmups=0,
                repeats=1,
                min_sample_seconds=invalid,
            )
    for invalid in (0, True, 1.5):
        with pytest.raises(ValueError, match="max_operations_per_sample"):
            core.measure_scenario(
                Scenario(),
                PROFILES["small"],
                warmups=0,
                repeats=1,
                max_operations_per_sample=invalid,  # type: ignore[arg-type]
            )


@pytest.mark.parametrize("invalid", ["nan", "inf", "-inf", "0"])
def test_cli_rejects_nonfinite_or_nonpositive_regression_percent(invalid):
    parser = benchmark_run.build_parser(["example"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--regression-percent", invalid])


@pytest.mark.parametrize("invalid", ["nan", "inf", "-inf", "0"])
def test_cli_rejects_nonfinite_or_nonpositive_minimum_sample_duration(invalid):
    parser = benchmark_run.build_parser(["example"])
    with pytest.raises(SystemExit):
        parser.parse_args(["--min-sample-seconds", invalid])


def test_cli_deduplicates_repeated_scenario_and_profile_flags(monkeypatch, tmp_path: Path):
    captured: dict[str, Any] = {}

    def fake_run(
        scenarios,
        profiles,
        *,
        warmups,
        repeats,
        min_sample_seconds,
        max_operations_per_sample,
    ):
        captured.update(
            scenarios=[scenario.name for scenario in scenarios],
            profiles=[profile.name for profile in profiles],
            warmups=warmups,
            repeats=repeats,
            min_sample_seconds=min_sample_seconds,
            max_operations_per_sample=max_operations_per_sample,
        )
        return {"schema_version": core.BENCHMARK_SCHEMA_VERSION, "results": []}

    monkeypatch.setattr(benchmark_run, "_load_scenarios", lambda: {"example": Scenario()})
    monkeypatch.setattr(benchmark_run, "run_benchmarks", fake_run)
    destination = tmp_path / "cli.json"

    exit_code = benchmark_run.main(
        [
            "--scenario",
            "example",
            "--scenario",
            "example",
            "--profile",
            "small",
            "--profile",
            "small",
            "--warmups",
            "0",
            "--repeats",
            "2",
            "--min-sample-seconds",
            "0.25",
            "--max-operations-per-sample",
            "12",
            "--output",
            str(destination),
        ]
    )

    assert exit_code == 0
    assert captured == {
        "scenarios": ["example"],
        "profiles": ["small"],
        "warmups": 0,
        "repeats": 2,
        "min_sample_seconds": 0.25,
        "max_operations_per_sample": 12,
    }
    assert destination.is_file()
