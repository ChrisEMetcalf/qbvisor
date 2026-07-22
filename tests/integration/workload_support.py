from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Final

import pandas as pd


@dataclass(frozen=True)
class WorkloadProfile:
    """Bounded generated-record settings for the live stabilization contract."""

    name: str
    record_count: int
    update_every: int
    export_chunk_size: int
    backup_page_size: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


WORKLOAD_PROFILES: Final[dict[str, WorkloadProfile]] = {
    "smoke": WorkloadProfile(
        name="smoke",
        record_count=30,
        update_every=5,
        export_chunk_size=10,
        backup_page_size=10,
    ),
    "standard": WorkloadProfile(
        name="standard",
        record_count=300,
        update_every=5,
        export_chunk_size=100,
        backup_page_size=100,
    ),
    "scale": WorkloadProfile(
        name="scale",
        record_count=1500,
        update_every=5,
        export_chunk_size=250,
        backup_page_size=250,
    ),
}


def get_workload_profile(name: str | None) -> WorkloadProfile:
    """Return a named profile, defaulting to the smallest safe live workload."""

    normalized = (name or "smoke").strip().lower()
    try:
        return WORKLOAD_PROFILES[normalized]
    except KeyError as error:
        choices = ", ".join(WORKLOAD_PROFILES)
        raise ValueError(
            f"Unknown QBVISOR_WORKLOAD_PROFILE {name!r}; choose one of: {choices}"
        ) from error


def build_workload_records(profile: WorkloadProfile, run_prefix: str) -> list[dict[str, Any]]:
    """Build deterministic, developer-readable records for one isolated run."""

    statuses = ("Ready", "Running", "Complete")
    start_date = date(2026, 1, 1)
    return [
        {
            "Fixture Key": f"{run_prefix}-{index:05d}",
            "Name": f"Workload record {index:05d}",
            "Amount": round((index + 1) * 1.25, 2),
            "Status": statuses[index % len(statuses)],
            "Active": index % 2 == 0,
            "Event Date": (start_date + timedelta(days=index % 365)).isoformat(),
        }
        for index in range(profile.record_count)
    ]


def build_update_pass(
    profile: WorkloadProfile, records: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], int]:
    """Copy a workload and change a predictable subset for outcome verification."""

    updated: list[dict[str, Any]] = []
    changed_count = 0
    for index, record in enumerate(records):
        candidate = record.copy()
        if index % profile.update_every == 0:
            candidate["Amount"] = float(candidate["Amount"]) + 1000
            changed_count += 1
        updated.append(candidate)
    return updated, changed_count


def select_workload_rows(frame: pd.DataFrame, run_prefix: str) -> pd.DataFrame:
    """Select one workload safely when unrelated rows contain empty fixture keys."""

    keys = frame["Fixture Key"].astype("string")
    return frame[keys.str.startswith(f"{run_prefix}-", na=False)]


def finalize_workload_run(
    summary: dict[str, Any],
    *,
    expected_deleted: int | None,
    cleanup: Callable[[], int],
    write_summary: Callable[[dict[str, Any]], Path],
    primary_error: BaseException | None = None,
    failure_phase: str | None = None,
) -> tuple[int, Path]:
    """Clean up, persist an honest result, and preserve the primary workload failure."""

    deleted: int | None = None
    cleanup_error: Exception | None = None
    try:
        deleted = cleanup()
    except Exception as error:
        cleanup_error = error

    failure = primary_error
    if primary_error is not None:
        summary.update(
            {
                "status": "failed",
                "errorType": type(primary_error).__name__,
                "failurePhase": failure_phase or "workload",
            }
        )

    summary["deletedRecords"] = deleted
    if cleanup_error is not None:
        summary["cleanupErrorType"] = type(cleanup_error).__name__
        if failure is None:
            failure = cleanup_error
            summary.update(
                {
                    "status": "failed",
                    "errorType": type(cleanup_error).__name__,
                    "failurePhase": "cleanup",
                }
            )
        else:
            failure.add_note(f"Cleanup also failed: {type(cleanup_error).__name__}")
    elif expected_deleted is not None and deleted != expected_deleted:
        mismatch = AssertionError(
            f"Workload cleanup deleted {deleted} records; expected {expected_deleted}"
        )
        summary["cleanupErrorType"] = "CleanupCountMismatch"
        if failure is None:
            failure = mismatch
            summary.update(
                {
                    "status": "failed",
                    "errorType": "CleanupCountMismatch",
                    "failurePhase": "cleanup",
                }
            )
        else:
            failure.add_note(str(mismatch))

    if failure is None:
        summary["status"] = "passed"

    try:
        summary_path = write_summary(summary)
    except Exception as summary_error:
        if failure is None:
            raise
        failure.add_note(
            f"Writing the workload summary also failed: {type(summary_error).__name__}"
        )
        raise failure.with_traceback(failure.__traceback__) from None

    if failure is not None:
        raise failure.with_traceback(failure.__traceback__)

    assert deleted is not None
    return deleted, summary_path
