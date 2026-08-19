"""Measurement and reporting primitives for credential-free local benchmarks."""

from __future__ import annotations

import importlib.metadata
import json
import math
import os
import platform
import re
import statistics
import subprocess
import tracemalloc
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from time import get_clock_info, perf_counter
from typing import Any, Protocol
from uuid import uuid4

from .profiles import BenchmarkProfile

BENCHMARK_SCHEMA_VERSION = "1.1"
COMPARISON_SCHEMA_VERSION = "1.1"
DEPENDENCIES = ("aiofiles", "aiohttp", "pandas", "python-dotenv", "requests")
DEFAULT_MIN_SAMPLE_SECONDS = 0.100
DEFAULT_MAX_OPERATIONS_PER_SAMPLE = 10_000

JsonScalar = str | int | float | bool | None


class PreparedScenario(Protocol):
    """One prepared iteration whose setup and validation are not measured."""

    @property
    def shape(self) -> Mapping[str, JsonScalar]:
        """Return stable, JSON-scalar workload dimensions."""

    @property
    def unit_count(self) -> int:
        """Return the positive denominator used to calculate throughput."""

    @property
    def unit_label(self) -> str:
        """Return the human-readable throughput unit."""

    def run(self) -> object:
        """Perform only the operation whose performance is being measured."""

    def validate(self, result: object) -> None:
        """Validate the measured result outside the measured interval."""

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        """Return deterministic non-timing unit totals for one validated operation."""

    def cleanup(self) -> None:
        """Release resources outside the measured interval, including after failures."""


class BenchmarkScenario(Protocol):
    """Factory for deterministic prepared benchmark iterations."""

    @property
    def name(self) -> str:
        """Return the stable scenario identifier."""

    @property
    def description(self) -> str:
        """Return a concise description of the measured production behavior."""

    @property
    def track_peak_memory(self) -> bool:
        """Choose whether paired iterations measure peak traced memory."""

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        """Build one isolated iteration before measurement begins.

        Preparation is the scenario's transaction boundary. If it raises before returning a
        prepared object, it must roll back resources itself because the core has nothing to clean.
        """


class BenchmarkContractError(ValueError):
    """Raised when a scenario violates the benchmark integration contract."""


def _distribution_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def _source_metadata() -> dict[str, str | bool | None]:
    """Identify source without recording its checkout path or branch name."""

    repository = Path(__file__).resolve().parents[1]
    try:
        revision_process = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        revision = revision_process.stdout.strip().lower()
        if revision_process.returncode != 0 or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
            return {"revision": None, "dirty": None}
        status_process = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=normal"],
            cwd=repository,
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
        dirty = bool(status_process.stdout) if status_process.returncode == 0 else None
        return {"revision": revision, "dirty": dirty}
    except (OSError, subprocess.SubprocessError):
        return {"revision": None, "dirty": None}


def _processor_identity() -> str:
    processor = platform.processor().strip()
    if processor:
        return processor
    cpu_info = Path("/proc/cpuinfo")
    try:
        for line in cpu_info.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition(":")
            if separator and key.strip().casefold() in {"model name", "hardware"}:
                return value.strip() or "unknown"
    except OSError:
        pass
    return "unknown"


def environment_metadata() -> dict[str, Any]:
    """Return comparison context without hostnames, users, paths, or configuration secrets."""

    clock = get_clock_info("perf_counter")
    return {
        "qbvisor_version": _distribution_version("qbvisor") or "0+unknown",
        "python": {
            "implementation": platform.python_implementation(),
            "version": platform.python_version(),
        },
        "dependencies": {name: _distribution_version(name) for name in DEPENDENCIES},
        "platform": {
            "cpu_logical_count": os.cpu_count(),
            "machine": platform.machine() or "unknown",
            "processor": _processor_identity(),
            "release": platform.release() or "unknown",
            "system": platform.system() or "unknown",
        },
        "source": _source_metadata(),
        "clock": {
            "adjustable": clock.adjustable,
            "implementation": clock.implementation,
            "monotonic": clock.monotonic,
            "resolution_seconds": clock.resolution,
        },
    }


def _validate_scenario(scenario: BenchmarkScenario) -> None:
    if not isinstance(scenario.name, str) or not scenario.name.strip():
        raise BenchmarkContractError("Scenario name must be a non-empty string")
    if not isinstance(scenario.description, str) or not scenario.description.strip():
        raise BenchmarkContractError(f"Scenario {scenario.name!r} needs a non-empty description")
    if not isinstance(scenario.track_peak_memory, bool):
        raise BenchmarkContractError(f"Scenario {scenario.name!r} track_peak_memory must be a bool")


def _prepared_identity(prepared: PreparedScenario) -> tuple[dict[str, JsonScalar], int, str]:
    if not isinstance(prepared.shape, Mapping):
        raise BenchmarkContractError("Prepared scenario shape must be a mapping")

    shape: dict[str, JsonScalar] = {}
    for key, value in prepared.shape.items():
        if not isinstance(key, str) or not key:
            raise BenchmarkContractError("Prepared scenario shape keys must be non-empty strings")
        if not isinstance(value, (str, int, float, bool)) and value is not None:
            raise BenchmarkContractError(
                f"Prepared scenario shape value for {key!r} must be a JSON scalar"
            )
        if isinstance(value, float) and not math.isfinite(value):
            raise BenchmarkContractError(
                f"Prepared scenario shape value for {key!r} must be finite"
            )
        shape[key] = value

    if (
        not isinstance(prepared.unit_count, int)
        or isinstance(prepared.unit_count, bool)
        or prepared.unit_count <= 0
    ):
        raise BenchmarkContractError("Prepared scenario unit_count must be a positive integer")
    if not isinstance(prepared.unit_label, str) or not prepared.unit_label.strip():
        raise BenchmarkContractError("Prepared scenario unit_label must be a non-empty string")
    return dict(sorted(shape.items())), prepared.unit_count, prepared.unit_label


def _prepared_observations(prepared: PreparedScenario, result: object) -> dict[str, int | float]:
    raw = prepared.workload_observations(result)
    if not isinstance(raw, Mapping):
        raise BenchmarkContractError("Prepared scenario workload observations must be a mapping")
    observations: dict[str, int | float] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key.strip():
            raise BenchmarkContractError(
                "Prepared scenario workload observation keys must be non-empty strings"
            )
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise BenchmarkContractError(
                f"Prepared scenario workload observation {key!r} must be numeric"
            )
        if not math.isfinite(float(value)) or value < 0:
            raise BenchmarkContractError(
                f"Prepared scenario workload observation {key!r} must be finite and non-negative"
            )
        observations[key] = value
    return dict(sorted(observations.items()))


def _run_measured(
    prepared: PreparedScenario, track_peak_memory: bool
) -> tuple[object, float, int | None]:
    tracing_before = tracemalloc.is_tracing()
    if tracing_before and not track_peak_memory:
        raise BenchmarkContractError(
            "Latency measurement requires tracemalloc to be stopped before the benchmark"
        )
    baseline_bytes = 0
    if track_peak_memory:
        if tracing_before:
            baseline_bytes, _ = tracemalloc.get_traced_memory()
            tracemalloc.reset_peak()
        else:
            tracemalloc.start()

    try:
        started = perf_counter()
        result = prepared.run()
        elapsed = perf_counter() - started
        peak_bytes: int | None = None
        if track_peak_memory:
            _, absolute_peak = tracemalloc.get_traced_memory()
            peak_bytes = max(0, absolute_peak - baseline_bytes)
        if elapsed <= 0 or not math.isfinite(elapsed):
            raise BenchmarkContractError("perf_counter produced a non-positive elapsed interval")
        return result, elapsed, peak_bytes
    finally:
        if track_peak_memory and not tracing_before:
            tracemalloc.stop()


def _run_iteration(
    scenario: BenchmarkScenario,
    profile: BenchmarkProfile,
    *,
    capture_latency: bool,
    capture_memory: bool,
) -> tuple[
    dict[str, JsonScalar],
    int,
    str,
    dict[str, int | float],
    float | None,
    int | None,
]:
    prepared = scenario.prepare(profile)
    primary_error: BaseException | None = None
    primary_traceback = None
    iteration: (
        tuple[
            dict[str, JsonScalar],
            int,
            str,
            dict[str, int | float],
            float | None,
            int | None,
        ]
        | None
    ) = None
    try:
        shape, unit_count, unit_label = _prepared_identity(prepared)
        if capture_latency or capture_memory:
            result, elapsed, peak_bytes = _run_measured(
                prepared,
                track_peak_memory=capture_memory,
            )
        else:
            result = prepared.run()
            elapsed = None
            peak_bytes = None
        prepared.validate(result)
        observations = _prepared_observations(prepared, result)
        iteration = (
            shape,
            unit_count,
            unit_label,
            observations,
            elapsed if capture_latency else None,
            peak_bytes if capture_memory else None,
        )
    except BaseException as exc:
        primary_error = exc
        primary_traceback = exc.__traceback__

    cleanup_error: BaseException | None = None
    cleanup_traceback = None
    try:
        prepared.cleanup()
    except BaseException as exc:
        cleanup_error = exc
        cleanup_traceback = exc.__traceback__

    if primary_error is not None:
        if cleanup_error is not None:
            primary_error.add_note(
                "Prepared scenario cleanup also failed: "
                f"{type(cleanup_error).__name__}: {cleanup_error}"
            )
        raise primary_error.with_traceback(primary_traceback)
    if cleanup_error is not None:
        raise cleanup_error.with_traceback(cleanup_traceback)
    assert iteration is not None
    return iteration


def _summary(values: Sequence[float | int]) -> dict[str, float | int]:
    if not values:
        raise ValueError("Cannot summarize an empty sample")
    mean = statistics.fmean(values)
    stdev = statistics.stdev(values) if len(values) > 1 else 0.0
    return {
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "stdev": stdev,
        "coefficient_of_variation": stdev / mean if mean else 0.0,
    }


def _sample_float(sample: Mapping[str, Any], key: str) -> float:
    value = sample[key]
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise AssertionError(f"Internal benchmark sample {key!r} is not numeric")
    return float(value)


def measure_scenario(
    scenario: BenchmarkScenario,
    profile: BenchmarkProfile,
    *,
    warmups: int,
    repeats: int,
    min_sample_seconds: float = DEFAULT_MIN_SAMPLE_SECONDS,
    max_operations_per_sample: int = DEFAULT_MAX_OPERATIONS_PER_SAMPLE,
) -> dict[str, Any]:
    """Measure duration-backed logical samples with setup kept out of every interval."""

    _validate_scenario(scenario)
    if not isinstance(warmups, int) or isinstance(warmups, bool) or warmups < 0:
        raise ValueError("warmups must be zero or greater")
    if not isinstance(repeats, int) or isinstance(repeats, bool) or repeats <= 0:
        raise ValueError("repeats must be greater than zero")
    if (
        not isinstance(min_sample_seconds, (int, float))
        or isinstance(min_sample_seconds, bool)
        or not math.isfinite(float(min_sample_seconds))
        or min_sample_seconds <= 0
    ):
        raise ValueError("min_sample_seconds must be a positive finite number")
    if (
        not isinstance(max_operations_per_sample, int)
        or isinstance(max_operations_per_sample, bool)
        or max_operations_per_sample <= 0
    ):
        raise ValueError("max_operations_per_sample must be a positive integer")

    identity: tuple[dict[str, JsonScalar], int, str] | None = None
    observation_identity: dict[str, int | float] | None = None
    samples: list[dict[str, Any]] = []

    def check_identity(
        shape: dict[str, JsonScalar],
        unit_count: int,
        unit_label: str,
        observations: dict[str, int | float],
    ) -> None:
        nonlocal identity, observation_identity
        current_identity = (shape, unit_count, unit_label)
        if identity is None:
            identity = current_identity
        elif current_identity != identity:
            raise BenchmarkContractError(
                f"Scenario {scenario.name!r} changed shape or units between iterations"
            )
        if observation_identity is None:
            observation_identity = observations
        elif observations != observation_identity:
            raise BenchmarkContractError(
                f"Scenario {scenario.name!r} changed workload observations between iterations"
            )

    for _ in range(warmups):
        shape, unit_count, unit_label, observations, _, _ = _run_iteration(
            scenario,
            profile,
            capture_latency=False,
            capture_memory=False,
        )
        check_identity(shape, unit_count, unit_label, observations)

    for _ in range(repeats):
        total_measured_seconds = 0.0
        operation_count = 0
        while operation_count < max_operations_per_sample:
            shape, unit_count, unit_label, observations, elapsed, _ = _run_iteration(
                scenario,
                profile,
                capture_latency=True,
                capture_memory=False,
            )
            check_identity(shape, unit_count, unit_label, observations)
            assert elapsed is not None
            total_measured_seconds += elapsed
            operation_count += 1
            if total_measured_seconds >= min_sample_seconds:
                break

        peak_bytes: int | None = None
        if scenario.track_peak_memory:
            (
                memory_shape,
                memory_count,
                memory_label,
                memory_observations,
                _,
                peak_bytes,
            ) = _run_iteration(
                scenario,
                profile,
                capture_latency=False,
                capture_memory=True,
            )
            check_identity(
                memory_shape,
                memory_count,
                memory_label,
                memory_observations,
            )

        assert identity is not None
        _, unit_count, _ = identity
        assert observation_identity is not None
        observation_totals = {
            key: value * operation_count for key, value in observation_identity.items()
        }
        samples.append(
            {
                "elapsed_seconds": total_measured_seconds / operation_count,
                "throughput_units_per_second": (
                    unit_count * operation_count / total_measured_seconds
                ),
                "total_measured_seconds": total_measured_seconds,
                "operation_count": operation_count,
                "sampling_cap_reached": total_measured_seconds < min_sample_seconds,
                "peak_traced_memory_bytes": peak_bytes,
                "workload_observation_totals": observation_totals,
                "workload_observation_rates_per_second": {
                    key: value / total_measured_seconds for key, value in observation_totals.items()
                },
            }
        )

    assert identity is not None
    assert observation_identity is not None
    shape, unit_count, unit_label = identity
    latencies = [_sample_float(sample, "elapsed_seconds") for sample in samples]
    throughputs = [_sample_float(sample, "throughput_units_per_second") for sample in samples]
    measured_totals = [_sample_float(sample, "total_measured_seconds") for sample in samples]
    operation_counts = [int(_sample_float(sample, "operation_count")) for sample in samples]
    observation_rate_summaries = {
        key: _summary(
            [float(sample["workload_observation_rates_per_second"][key]) for sample in samples]
        )
        for key in observation_identity
    }
    peaks = [
        int(sample["peak_traced_memory_bytes"])
        for sample in samples
        if sample["peak_traced_memory_bytes"] is not None
    ]
    return {
        "scenario": scenario.name,
        "description": scenario.description,
        "profile": profile.name,
        "profile_shape": profile.as_dict(),
        "shape": shape,
        "unit_count": unit_count,
        "unit_label": unit_label,
        "workload_observations_per_operation": observation_identity,
        "memory_measurement": (
            "paired_fresh_iteration" if scenario.track_peak_memory else "not_measured"
        ),
        "samples": samples,
        "summary": {
            "latency_seconds": _summary(latencies),
            "throughput_units_per_second": _summary(throughputs),
            "total_measured_seconds": _summary(measured_totals),
            "operation_count": _summary(operation_counts),
            "sampling_cap_reached_count": sum(
                bool(sample["sampling_cap_reached"]) for sample in samples
            ),
            "workload_observation_rates_per_second": observation_rate_summaries,
            "peak_traced_memory_bytes": _summary(peaks) if peaks else None,
        },
    }


def run_benchmarks(
    scenarios: Sequence[BenchmarkScenario],
    profiles: Sequence[BenchmarkProfile],
    *,
    warmups: int = 1,
    repeats: int = 5,
    min_sample_seconds: float = DEFAULT_MIN_SAMPLE_SECONDS,
    max_operations_per_sample: int = DEFAULT_MAX_OPERATIONS_PER_SAMPLE,
) -> dict[str, Any]:
    """Run selected benchmark pairs and return the versioned JSON-compatible report."""

    scenario_names = [scenario.name for scenario in scenarios]
    profile_names = [profile.name for profile in profiles]
    if not scenarios:
        raise ValueError("Select at least one benchmark scenario")
    if not profiles:
        raise ValueError("Select at least one benchmark profile")
    if len(set(scenario_names)) != len(scenario_names):
        raise ValueError("Benchmark scenario names must be unique")
    if len(set(profile_names)) != len(profile_names):
        raise ValueError("Benchmark profile names must be unique")

    ordered_scenarios = sorted(scenarios, key=lambda scenario: scenario.name)
    ordered_profiles = sorted(profiles, key=lambda profile: (profile.record_count, profile.name))
    results = [
        measure_scenario(
            scenario,
            profile,
            warmups=warmups,
            repeats=repeats,
            min_sample_seconds=min_sample_seconds,
            max_operations_per_sample=max_operations_per_sample,
        )
        for scenario in ordered_scenarios
        for profile in ordered_profiles
    ]
    return {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "environment": environment_metadata(),
        "configuration": {
            "warmups": warmups,
            "repeats": repeats,
            "min_sample_seconds": min_sample_seconds,
            "max_operations_per_sample": max_operations_per_sample,
            "memory_measurement": "paired_fresh_iteration",
            "scenarios": [scenario.name for scenario in ordered_scenarios],
            "profiles": [profile.name for profile in ordered_profiles],
        },
        "results": results,
    }


def _median_metric(result: Mapping[str, Any], metric: str) -> float | None:
    summary = result.get("summary")
    if not isinstance(summary, Mapping):
        return None
    metric_summary = summary.get(metric)
    if metric_summary is None:
        return None
    if not isinstance(metric_summary, Mapping):
        raise ValueError(f"Benchmark summary {metric!r} must be an object or null")
    median = metric_summary.get("median")
    if not isinstance(median, (int, float)) or isinstance(median, bool):
        raise ValueError(f"Benchmark summary {metric!r} median must be numeric")
    numeric = float(median)
    allows_zero = metric == "peak_traced_memory_bytes"
    if not math.isfinite(numeric) or numeric < 0 or (not allows_zero and numeric == 0):
        qualifier = "non-negative" if allows_zero else "positive"
        raise ValueError(f"Benchmark summary {metric!r} median must be finite and {qualifier}")
    return numeric


def _raw_metric_samples(result: Mapping[str, Any], metric: str) -> list[float] | None:
    samples = result.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError("Benchmark result must contain a non-empty raw samples array")
    values: list[float] = []
    saw_null = False
    for index, sample in enumerate(samples):
        if not isinstance(sample, Mapping) or metric not in sample:
            raise ValueError(f"Benchmark raw sample {index} is missing {metric!r}")
        value = sample[metric]
        if value is None:
            saw_null = True
            continue
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise ValueError(f"Benchmark raw sample {metric!r} must be numeric or null")
        numeric = float(value)
        allows_zero = metric == "peak_traced_memory_bytes"
        if not math.isfinite(numeric) or numeric < 0 or (not allows_zero and numeric == 0):
            qualifier = "non-negative" if allows_zero else "positive"
            raise ValueError(f"Benchmark raw sample {metric!r} must be finite and {qualifier}")
        values.append(numeric)
    if saw_null and values:
        raise ValueError(f"Benchmark raw samples for {metric!r} cannot mix null and numeric values")
    return values or None


def _metric_inputs(
    result: Mapping[str, Any], summary_metric: str, sample_metric: str
) -> tuple[float | None, list[float] | None]:
    median = _median_metric(result, summary_metric)
    samples = _raw_metric_samples(result, sample_metric)
    if (median is None) != (samples is None):
        raise ValueError(
            f"Benchmark {summary_metric!r} summary and raw samples must both be measured or null"
        )
    if median is not None and samples is not None and not min(samples) <= median <= max(samples):
        raise ValueError(
            f"Benchmark {summary_metric!r} median must fall within its raw sample range"
        )
    return median, samples


def _metric_delta(
    current: float | None,
    baseline: float | None,
    current_samples: Sequence[float] | None,
    baseline_samples: Sequence[float] | None,
    *,
    higher_is_better: bool,
    threshold_percent: float,
) -> dict[str, Any]:
    current_range = (
        {"min": min(current_samples), "max": max(current_samples)} if current_samples else None
    )
    baseline_range = (
        {"min": min(baseline_samples), "max": max(baseline_samples)} if baseline_samples else None
    )
    if (
        current is None
        or baseline is None
        or baseline == 0
        or current_range is None
        or baseline_range is None
    ):
        return {
            "baseline": baseline,
            "current": current,
            "delta_percent": None,
            "baseline_range": baseline_range,
            "current_range": current_range,
            "sample_ranges_overlap": None,
            "threshold_crossed": None,
            "classification": "unavailable",
        }

    delta_percent = (current - baseline) / baseline * 100
    signed_quality_delta = delta_percent if higher_is_better else -delta_percent
    sample_ranges_overlap = not (
        current_range["max"] < baseline_range["min"] or baseline_range["max"] < current_range["min"]
    )
    if signed_quality_delta <= -threshold_percent:
        threshold_classification = "regression_signal"
    elif signed_quality_delta >= threshold_percent:
        threshold_classification = "improvement_signal"
    else:
        threshold_classification = None
    threshold_crossed = threshold_classification is not None
    directionally_qualified = False
    if threshold_classification == "regression_signal":
        directionally_qualified = (
            current_range["max"] < baseline_range["min"]
            if higher_is_better
            else current_range["min"] > baseline_range["max"]
        )
    elif threshold_classification == "improvement_signal":
        directionally_qualified = (
            current_range["min"] > baseline_range["max"]
            if higher_is_better
            else current_range["max"] < baseline_range["min"]
        )
    classification = threshold_classification if directionally_qualified else "stable"
    return {
        "baseline": baseline,
        "current": current,
        "delta_percent": delta_percent,
        "baseline_range": baseline_range,
        "current_range": current_range,
        "sample_ranges_overlap": sample_ranges_overlap,
        "threshold_crossed": threshold_crossed,
        "classification": classification,
    }


def _nonempty_string(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _valid_clock(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    resolution = value.get("resolution_seconds")
    return (
        _nonempty_string(value.get("implementation"))
        and isinstance(value.get("monotonic"), bool)
        and isinstance(value.get("adjustable"), bool)
        and isinstance(resolution, (int, float))
        and not isinstance(resolution, bool)
        and math.isfinite(float(resolution))
        and float(resolution) > 0
    )


def _environment_compatibility(
    current: Mapping[str, Any], baseline: Mapping[str, Any]
) -> tuple[bool, list[str]]:
    current_environment = current.get("environment")
    baseline_environment = baseline.get("environment")
    if not isinstance(current_environment, Mapping) or not isinstance(
        baseline_environment, Mapping
    ):
        return False, ["environment metadata is missing"]

    differences: list[str] = []
    current_python = current_environment.get("python")
    baseline_python = baseline_environment.get("python")
    if not isinstance(current_python, Mapping) or not isinstance(baseline_python, Mapping):
        differences.append("Python metadata is missing")
    else:
        if not all(
            _nonempty_string(metadata.get("implementation"))
            and _nonempty_string(metadata.get("version"))
            for metadata in (current_python, baseline_python)
        ):
            differences.append("Python metadata is incomplete")
        elif current_python.get("implementation") != baseline_python.get("implementation"):
            differences.append("Python implementation differs")
        elif current_python.get("version") != baseline_python.get("version"):
            differences.append("Python version differs")

    current_platform = current_environment.get("platform")
    baseline_platform = baseline_environment.get("platform")
    if not isinstance(current_platform, Mapping) or not isinstance(baseline_platform, Mapping):
        differences.append("platform metadata is missing")
    else:
        for field in ("system", "release", "machine", "processor"):
            values = (current_platform.get(field), baseline_platform.get(field))
            if not all(
                _nonempty_string(value) and str(value).casefold() != "unknown" for value in values
            ):
                differences.append(f"platform {field} is unavailable")
                continue
            if current_platform.get(field) != baseline_platform.get(field):
                differences.append(f"platform {field} differs")
        current_cpu_count = current_platform.get("cpu_logical_count")
        baseline_cpu_count = baseline_platform.get("cpu_logical_count")
        if (
            not isinstance(current_cpu_count, int)
            or isinstance(current_cpu_count, bool)
            or current_cpu_count <= 0
            or not isinstance(baseline_cpu_count, int)
            or isinstance(baseline_cpu_count, bool)
            or baseline_cpu_count <= 0
        ):
            differences.append("logical CPU count is unavailable")
        elif current_cpu_count != baseline_cpu_count:
            differences.append("platform cpu_logical_count differs")

    current_dependencies = current_environment.get("dependencies")
    baseline_dependencies = baseline_environment.get("dependencies")
    if not isinstance(current_dependencies, Mapping) or not isinstance(
        baseline_dependencies, Mapping
    ):
        differences.append("dependency metadata is missing")
    else:
        for dependency in DEPENDENCIES:
            current_version = current_dependencies.get(dependency)
            baseline_version = baseline_dependencies.get(dependency)
            if not isinstance(current_version, str) or not isinstance(baseline_version, str):
                differences.append(f"dependency {dependency} version is unavailable")
            elif current_version != baseline_version:
                differences.append(f"dependency {dependency} version differs")

    if not _valid_clock(current_environment.get("clock")) or not _valid_clock(
        baseline_environment.get("clock")
    ):
        differences.append("perf_counter clock metadata is incomplete")
    elif current_environment.get("clock") != baseline_environment.get("clock"):
        differences.append("perf_counter clock metadata differs")

    return not differences, differences


def _source_comparison_context(
    current: Mapping[str, Any], baseline: Mapping[str, Any]
) -> tuple[dict[str, Any], bool, list[str]]:
    states: dict[str, Any] = {}
    warnings: list[str] = []
    for label, report in (("current", current), ("baseline", baseline)):
        environment = report.get("environment")
        source = environment.get("source") if isinstance(environment, Mapping) else None
        revision = source.get("revision") if isinstance(source, Mapping) else None
        dirty = source.get("dirty") if isinstance(source, Mapping) else None
        states[label] = {"revision": revision, "dirty": dirty}
        if not isinstance(revision, str) or re.fullmatch(r"[0-9a-f]{40}", revision) is None:
            warnings.append(f"{label} source revision is unavailable")
        if dirty is True:
            warnings.append(f"{label} source has uncommitted changes")
        elif dirty is not False:
            warnings.append(f"{label} source dirty state is unknown")
    return states, not warnings, warnings


def _report_compatibility(current: Mapping[str, Any], baseline: Mapping[str, Any]) -> list[str]:
    differences: list[str] = []
    if current.get("schema_version") != BENCHMARK_SCHEMA_VERSION:
        differences.append("current benchmark schema version is unsupported")
    if baseline.get("schema_version") != BENCHMARK_SCHEMA_VERSION:
        differences.append("baseline benchmark schema version is unsupported")
    current_configuration = current.get("configuration")
    baseline_configuration = baseline.get("configuration")
    if not isinstance(current_configuration, Mapping) or not isinstance(
        baseline_configuration, Mapping
    ):
        differences.append("benchmark configuration is missing")
        return differences
    for field in (
        "warmups",
        "repeats",
        "min_sample_seconds",
        "max_operations_per_sample",
        "memory_measurement",
    ):
        if field not in current_configuration or field not in baseline_configuration:
            differences.append(f"benchmark configuration {field} is missing")
        elif current_configuration[field] != baseline_configuration[field]:
            differences.append(f"benchmark configuration {field} differs")
    return differences


def _result_index(
    report: Mapping[str, Any], label: str
) -> dict[tuple[str, str], Mapping[str, Any]]:
    configuration = report.get("configuration")
    if not isinstance(configuration, Mapping):
        raise ValueError(f"{label} benchmark configuration must be an object")
    min_sample_seconds = configuration.get("min_sample_seconds")
    max_operations = configuration.get("max_operations_per_sample")
    if (
        not isinstance(min_sample_seconds, (int, float))
        or isinstance(min_sample_seconds, bool)
        or not math.isfinite(float(min_sample_seconds))
        or min_sample_seconds <= 0
    ):
        raise ValueError(f"{label} min_sample_seconds must be positive and finite")
    if (
        not isinstance(max_operations, int)
        or isinstance(max_operations, bool)
        or max_operations <= 0
    ):
        raise ValueError(f"{label} max_operations_per_sample must be a positive integer")
    raw_results = report.get("results")
    if not isinstance(raw_results, list):
        raise ValueError(f"{label} benchmark results must be an array")
    indexed: dict[tuple[str, str], Mapping[str, Any]] = {}
    for item in raw_results:
        if not isinstance(item, Mapping):
            raise ValueError(f"{label} benchmark result must be an object")
        scenario = item.get("scenario")
        profile = item.get("profile")
        if (
            not isinstance(scenario, str)
            or not scenario
            or not isinstance(profile, str)
            or not profile
        ):
            raise ValueError(f"{label} benchmark result needs scenario and profile strings")
        key = (scenario, profile)
        if key in indexed:
            raise ValueError(f"{label} benchmark results contain duplicate pair {key!r}")
        for summary_metric, sample_metric in (
            ("latency_seconds", "elapsed_seconds"),
            ("throughput_units_per_second", "throughput_units_per_second"),
            ("peak_traced_memory_bytes", "peak_traced_memory_bytes"),
        ):
            _metric_inputs(item, summary_metric, sample_metric)
        observations = item.get("workload_observations_per_operation")
        if not isinstance(observations, Mapping):
            raise ValueError(f"{label} workload observations must be an object")
        for observation, per_operation in observations.items():
            if not isinstance(observation, str) or not observation.strip():
                raise ValueError(f"{label} workload observation keys must be non-empty strings")
            if (
                not isinstance(per_operation, (int, float))
                or isinstance(per_operation, bool)
                or not math.isfinite(float(per_operation))
                or per_operation < 0
            ):
                raise ValueError(
                    f"{label} workload observation {observation!r} must be finite, "
                    "numeric, and non-negative"
                )
        unit_count = item.get("unit_count")
        if not isinstance(unit_count, int) or isinstance(unit_count, bool) or unit_count <= 0:
            raise ValueError(f"{label} unit_count must be a positive integer")
        samples = item["samples"]
        assert isinstance(samples, list)
        for index, sample in enumerate(samples):
            assert isinstance(sample, Mapping)
            total_seconds = sample.get("total_measured_seconds")
            operation_count = sample.get("operation_count")
            cap_reached = sample.get("sampling_cap_reached")
            if (
                not isinstance(total_seconds, (int, float))
                or isinstance(total_seconds, bool)
                or not math.isfinite(float(total_seconds))
                or total_seconds <= 0
            ):
                raise ValueError(f"{label} sample {index} total_measured_seconds is invalid")
            if (
                not isinstance(operation_count, int)
                or isinstance(operation_count, bool)
                or not 1 <= operation_count <= max_operations
            ):
                raise ValueError(f"{label} sample {index} operation_count is invalid")
            if not isinstance(cap_reached, bool):
                raise ValueError(f"{label} sample {index} sampling_cap_reached must be bool")
            expected_cap = total_seconds < min_sample_seconds
            if cap_reached != expected_cap or (cap_reached and operation_count != max_operations):
                raise ValueError(f"{label} sample {index} sampling cap state is inconsistent")
            elapsed = sample.get("elapsed_seconds")
            if not isinstance(elapsed, (int, float)) or not math.isclose(
                float(elapsed),
                float(total_seconds) / operation_count,
                rel_tol=1e-12,
                abs_tol=0.0,
            ):
                raise ValueError(f"{label} sample {index} per-operation latency is inconsistent")
            throughput = sample.get("throughput_units_per_second")
            if not isinstance(throughput, (int, float)) or not math.isclose(
                float(throughput),
                unit_count * operation_count / float(total_seconds),
                rel_tol=1e-12,
                abs_tol=0.0,
            ):
                raise ValueError(f"{label} sample {index} throughput is inconsistent")
            totals = sample.get("workload_observation_totals")
            rates = sample.get("workload_observation_rates_per_second")
            if not isinstance(totals, Mapping) or not isinstance(rates, Mapping):
                raise ValueError(f"{label} sample {index} workload observation data is missing")
            if set(totals) != set(observations) or set(rates) != set(observations):
                raise ValueError(f"{label} sample {index} workload observation keys differ")
            for observation, per_operation in observations.items():
                total = totals[observation]
                rate = rates[observation]
                expected_total = float(per_operation) * operation_count
                if (
                    not isinstance(total, (int, float))
                    or isinstance(total, bool)
                    or not math.isfinite(float(total))
                    or total < 0
                    or not math.isclose(float(total), expected_total, rel_tol=1e-12, abs_tol=0.0)
                ):
                    raise ValueError(
                        f"{label} sample {index} workload observation total is inconsistent"
                    )
                if (
                    not isinstance(rate, (int, float))
                    or isinstance(rate, bool)
                    or not math.isfinite(float(rate))
                    or rate < 0
                    or not math.isclose(
                        float(rate),
                        expected_total / float(total_seconds),
                        rel_tol=1e-12,
                        abs_tol=0.0,
                    )
                ):
                    raise ValueError(
                        f"{label} sample {index} workload observation rate is inconsistent"
                    )
        indexed[key] = item
    return indexed


def _pair_compatibility(
    current: Mapping[str, Any],
    baseline: Mapping[str, Any],
    *,
    current_repeats: object,
    baseline_repeats: object,
) -> list[str]:
    differences: list[str] = []
    for field in (
        "profile_shape",
        "shape",
        "unit_count",
        "unit_label",
        "workload_observations_per_operation",
        "memory_measurement",
    ):
        if field not in current or field not in baseline:
            differences.append(f"result identity {field} is missing")
        elif current[field] != baseline[field]:
            differences.append(f"result identity {field} differs")
    for result, expected, label in (
        (current, current_repeats, "current"),
        (baseline, baseline_repeats, "baseline"),
    ):
        samples = result.get("samples")
        if not isinstance(samples, list):
            differences.append(f"{label} raw samples are missing")
        elif (
            not isinstance(expected, int) or isinstance(expected, bool) or len(samples) != expected
        ):
            differences.append(f"{label} raw sample count does not match repeats")
    return differences


def compare_results(
    current: Mapping[str, Any],
    baseline: Mapping[str, Any],
    *,
    regression_percent: float = 15.0,
) -> dict[str, Any]:
    """Compare medians as informational signals without establishing a pass/fail gate."""

    if regression_percent <= 0 or not math.isfinite(regression_percent):
        raise ValueError("regression_percent must be a positive finite number")
    environment_compatible, environment_differences = _environment_compatibility(current, baseline)
    source_states, source_reproducible, source_warnings = _source_comparison_context(
        current, baseline
    )
    report_differences = _report_compatibility(current, baseline)
    current_results = _result_index(current, "current")
    baseline_results = _result_index(baseline, "baseline")
    current_configuration = current.get("configuration")
    baseline_configuration = baseline.get("configuration")
    current_repeats = (
        current_configuration.get("repeats") if isinstance(current_configuration, Mapping) else None
    )
    baseline_repeats = (
        baseline_configuration.get("repeats")
        if isinstance(baseline_configuration, Mapping)
        else None
    )
    comparisons: list[dict[str, Any]] = []
    for key, result in current_results.items():
        prior = baseline_results.get(key)
        if prior is None:
            comparisons.append(
                {
                    "scenario": key[0],
                    "profile": key[1],
                    "classification": "unavailable",
                    "metrics": {},
                    "unavailable_reasons": ["matching baseline scenario/profile is missing"],
                }
            )
            continue
        pair_differences = _pair_compatibility(
            result,
            prior,
            current_repeats=current_repeats,
            baseline_repeats=baseline_repeats,
        )
        unavailable_reasons = [
            *(f"environment: {reason}" for reason in environment_differences),
            *(f"report: {reason}" for reason in report_differences),
            *(f"workload: {reason}" for reason in pair_differences),
        ]
        current_latency = _metric_inputs(result, "latency_seconds", "elapsed_seconds")
        baseline_latency = _metric_inputs(prior, "latency_seconds", "elapsed_seconds")
        current_throughput = _metric_inputs(
            result,
            "throughput_units_per_second",
            "throughput_units_per_second",
        )
        baseline_throughput = _metric_inputs(
            prior,
            "throughput_units_per_second",
            "throughput_units_per_second",
        )
        current_memory = _metric_inputs(
            result,
            "peak_traced_memory_bytes",
            "peak_traced_memory_bytes",
        )
        baseline_memory = _metric_inputs(
            prior,
            "peak_traced_memory_bytes",
            "peak_traced_memory_bytes",
        )
        metrics = {
            "latency_seconds": _metric_delta(
                current_latency[0],
                baseline_latency[0],
                current_latency[1],
                baseline_latency[1],
                higher_is_better=False,
                threshold_percent=regression_percent,
            ),
            "throughput_units_per_second": _metric_delta(
                current_throughput[0],
                baseline_throughput[0],
                current_throughput[1],
                baseline_throughput[1],
                higher_is_better=True,
                threshold_percent=regression_percent,
            ),
            "peak_traced_memory_bytes": _metric_delta(
                current_memory[0],
                baseline_memory[0],
                current_memory[1],
                baseline_memory[1],
                higher_is_better=False,
                threshold_percent=regression_percent,
            ),
        }
        if unavailable_reasons:
            for metric in metrics.values():
                metric["classification"] = "unavailable"
        classifications = {metric["classification"] for metric in metrics.values()}
        if "regression_signal" in classifications:
            classification = "regression_signal"
        elif "improvement_signal" in classifications:
            classification = "improvement_signal"
        elif classifications == {"unavailable"}:
            classification = "unavailable"
        else:
            classification = "stable"
        comparisons.append(
            {
                "scenario": key[0],
                "profile": key[1],
                "classification": classification,
                "metrics": metrics,
                "unavailable_reasons": unavailable_reasons,
            }
        )

    return {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "signal_only": True,
        "environment_compatible": environment_compatible,
        "environment_differences": environment_differences,
        "report_compatible": not report_differences,
        "report_differences": report_differences,
        "qbvisor_versions": {
            "baseline": (
                baseline.get("environment", {}).get("qbvisor_version")
                if isinstance(baseline.get("environment"), Mapping)
                else None
            ),
            "current": (
                current.get("environment", {}).get("qbvisor_version")
                if isinstance(current.get("environment"), Mapping)
                else None
            ),
        },
        "source_states": source_states,
        "source_reproducible": source_reproducible,
        "source_warnings": source_warnings,
        "regression_threshold_percent": regression_percent,
        "results": comparisons,
    }


def write_json_report(report: Mapping[str, Any], destination: str | Path) -> None:
    """Atomically publish strict JSON without replacing an existing benchmark artifact."""

    output = Path(destination)
    output.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    temporary = output.with_name(f".{output.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)
