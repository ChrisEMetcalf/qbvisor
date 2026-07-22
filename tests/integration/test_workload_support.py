from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from workload_support import (
    WORKLOAD_PROFILES,
    build_update_pass,
    build_workload_records,
    finalize_workload_run,
    get_workload_profile,
    select_workload_rows,
)


def test_workload_profiles_scale_without_changing_behavior():
    smoke = get_workload_profile(None)
    standard = get_workload_profile("STANDARD")
    scale = get_workload_profile(" scale ")

    assert smoke is WORKLOAD_PROFILES["smoke"]
    assert smoke.record_count < standard.record_count < scale.record_count
    assert all(profile.export_chunk_size < profile.record_count for profile in (smoke, standard))


def test_unknown_workload_profile_explains_valid_choices():
    with pytest.raises(ValueError, match="smoke, standard, scale"):
        get_workload_profile("production")


def test_workload_records_are_unique_and_update_pass_is_bounded():
    profile = get_workload_profile("smoke")
    records = build_workload_records(profile, "qbvisor-workload-example")
    updated, changed_count = build_update_pass(profile, records)

    assert len(records) == profile.record_count
    assert len({record["Fixture Key"] for record in records}) == profile.record_count
    assert all(record["Fixture Key"].startswith("qbvisor-workload-example-") for record in records)
    assert changed_count == profile.record_count // profile.update_every
    assert (
        sum(before != after for before, after in zip(records, updated, strict=True))
        == changed_count
    )


def test_workload_row_selection_ignores_empty_unrelated_keys():
    frame = pd.DataFrame(
        {
            "Fixture Key": [None, "qbvisor-workload-run-00001", pd.NA, "qbvisor-alpha"],
            "Amount": [0, 10, 20, 30],
        }
    )

    selected = select_workload_rows(frame, "qbvisor-workload-run")

    assert list(selected["Fixture Key"]) == ["qbvisor-workload-run-00001"]


def _recording_writer(written: list[dict[str, object]], path: Path):
    def write(summary: dict[str, object]) -> Path:
        written.append(summary.copy())
        return path

    return write


def test_finalization_preserves_primary_failure_after_successful_cleanup(tmp_path: Path):
    summary: dict[str, object] = {"status": "running"}
    written: list[dict[str, object]] = []
    primary = RuntimeError("query failed")

    with pytest.raises(RuntimeError) as captured:
        finalize_workload_run(
            summary,
            expected_deleted=3,
            cleanup=lambda: 3,
            write_summary=_recording_writer(written, tmp_path / "result.json"),
            primary_error=primary,
            failure_phase="query_dataframe",
        )

    assert captured.value is primary
    assert written[0]["status"] == "failed"
    assert written[0]["errorType"] == "RuntimeError"
    assert written[0]["failurePhase"] == "query_dataframe"
    assert written[0]["deletedRecords"] == 3


@pytest.mark.parametrize("deleted", [0, 7])
def test_primary_create_failure_accepts_successful_prefix_cleanup(tmp_path: Path, deleted: int):
    summary: dict[str, object] = {"status": "running"}
    written: list[dict[str, object]] = []
    primary = RuntimeError("create failed before its full outcome was known")

    with pytest.raises(RuntimeError) as captured:
        finalize_workload_run(
            summary,
            expected_deleted=None,
            cleanup=lambda: deleted,
            write_summary=_recording_writer(written, tmp_path / "result.json"),
            primary_error=primary,
            failure_phase="create",
        )

    assert captured.value is primary
    assert written[0]["deletedRecords"] == deleted
    assert "cleanupErrorType" not in written[0]


def test_finalization_reports_cleanup_count_mismatch(tmp_path: Path):
    summary: dict[str, object] = {"status": "running"}
    written: list[dict[str, object]] = []

    with pytest.raises(AssertionError, match="deleted 2 records; expected 3"):
        finalize_workload_run(
            summary,
            expected_deleted=3,
            cleanup=lambda: 2,
            write_summary=_recording_writer(written, tmp_path / "result.json"),
        )

    assert written[0]["status"] == "failed"
    assert written[0]["errorType"] == "CleanupCountMismatch"
    assert written[0]["failurePhase"] == "cleanup"
    assert written[0]["deletedRecords"] == 2


def test_finalization_writes_summary_when_cleanup_raises(tmp_path: Path):
    summary: dict[str, object] = {"status": "running"}
    written: list[dict[str, object]] = []

    def fail_cleanup() -> int:
        raise OSError("sandbox unavailable")

    with pytest.raises(OSError, match="sandbox unavailable"):
        finalize_workload_run(
            summary,
            expected_deleted=3,
            cleanup=fail_cleanup,
            write_summary=_recording_writer(written, tmp_path / "result.json"),
        )

    assert written[0]["status"] == "failed"
    assert written[0]["errorType"] == "OSError"
    assert written[0]["failurePhase"] == "cleanup"
    assert written[0]["deletedRecords"] is None


def test_finalization_keeps_primary_error_when_cleanup_also_raises(tmp_path: Path):
    summary: dict[str, object] = {"status": "running"}
    written: list[dict[str, object]] = []
    primary = ValueError("export validation failed")

    def fail_cleanup() -> int:
        raise OSError("sandbox unavailable")

    with pytest.raises(ValueError) as captured:
        finalize_workload_run(
            summary,
            expected_deleted=3,
            cleanup=fail_cleanup,
            write_summary=_recording_writer(written, tmp_path / "result.json"),
            primary_error=primary,
            failure_phase="csv_export",
        )

    assert captured.value is primary
    assert written[0]["errorType"] == "ValueError"
    assert written[0]["cleanupErrorType"] == "OSError"
    assert any("Cleanup also failed" in note for note in primary.__notes__)
