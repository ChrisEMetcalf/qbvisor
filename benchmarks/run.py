"""Command-line entry point for the credential-free local benchmark suite."""

from __future__ import annotations

import argparse
import json
import math
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .core import (
    DEFAULT_MAX_OPERATIONS_PER_SAMPLE,
    DEFAULT_MIN_SAMPLE_SECONDS,
    compare_results,
    run_benchmarks,
    write_json_report,
)
from .profiles import PROFILES, get_profile


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0 or not math.isfinite(parsed):
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def _default_output() -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    return Path(".qbvisor") / "benchmarks" / f"result-{timestamp}.json"


def build_parser(scenario_names: Sequence[str]) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic, credential-free qbvisor performance benchmarks."
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=sorted(scenario_names),
        help="Scenario to run; repeat to select multiple (default: all).",
    )
    parser.add_argument(
        "--profile",
        action="append",
        choices=sorted(PROFILES),
        help="Profile to run; repeat to select multiple (default: all).",
    )
    parser.add_argument("--warmups", type=_nonnegative_int, default=1)
    parser.add_argument("--repeats", type=_positive_int, default=5)
    parser.add_argument(
        "--min-sample-seconds",
        type=_positive_float,
        default=DEFAULT_MIN_SAMPLE_SECONDS,
        help="Minimum accumulated run() time per logical sample (default: 0.100).",
    )
    parser.add_argument(
        "--max-operations-per-sample",
        type=_positive_int,
        default=DEFAULT_MAX_OPERATIONS_PER_SAMPLE,
        help="Safety cap on fresh operations in one logical sample (default: 10000).",
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument(
        "--baseline",
        type=Path,
        help="Optional prior JSON result for signal-only median comparisons.",
    )
    parser.add_argument(
        "--regression-percent",
        type=_positive_float,
        default=15.0,
        help="Informational comparison signal boundary (default: 15; never changes exit status).",
    )
    return parser


def _load_scenarios() -> Mapping[str, Any]:
    from .scenarios import SCENARIOS

    return SCENARIOS


def _unique(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(values))


def main(argv: Sequence[str] | None = None) -> int:
    scenarios = _load_scenarios()
    parser = build_parser(tuple(scenarios))
    args = parser.parse_args(argv)

    scenario_names = _unique(args.scenario) if args.scenario else sorted(scenarios)
    profile_names = _unique(args.profile) if args.profile else list(PROFILES)
    selected_scenarios = [scenarios[name] for name in scenario_names]
    selected_profiles = [get_profile(name) for name in profile_names]
    report = run_benchmarks(
        selected_scenarios,
        selected_profiles,
        warmups=args.warmups,
        repeats=args.repeats,
        min_sample_seconds=args.min_sample_seconds,
        max_operations_per_sample=args.max_operations_per_sample,
    )
    if args.baseline is not None:
        with args.baseline.open(encoding="utf-8") as source:
            baseline = json.load(source)
        report["comparison"] = compare_results(
            report,
            baseline,
            regression_percent=args.regression_percent,
        )

    output = args.output or _default_output()
    write_json_report(report, output)
    print(output)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through the CLI
    raise SystemExit(main())
