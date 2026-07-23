from __future__ import annotations

import json
from pathlib import Path

import pytest
from operational_support import (
    REQUIRED_OPERATIONAL_CHECKS,
    CleanupCountMismatch,
    IncompleteOperationalRun,
    IntentionalCleanupFailure,
    OperationalDiagnostics,
    recover_operational_records,
    safe_candidate_ref,
    safe_run_id,
)


def test_safe_labels_reject_arbitrary_environment_text_without_echoing_it():
    secret_like_value = "candidate\nQB-USER-TOKEN top-secret"

    with pytest.raises(ValueError) as run_error:
        safe_run_id(secret_like_value)
    with pytest.raises(ValueError) as ref_error:
        safe_candidate_ref(secret_like_value)

    assert secret_like_value not in str(run_error.value)
    assert secret_like_value not in str(ref_error.value)

    token_shaped_but_syntactically_safe = "QBUSER1234567890abcdef"
    assert token_shaped_but_syntactically_safe not in safe_run_id(
        token_shaped_but_syntactically_safe
    )


def test_diagnostics_whitelist_failure_metadata_without_exception_messages(tmp_path: Path):
    output_path = tmp_path / "operational.json"
    diagnostics = OperationalDiagnostics(output_path, run_id="unit-test")

    with pytest.raises(RuntimeError, match="top-secret"):
        with diagnostics.check("read"):
            raise RuntimeError("top-secret")
    diagnostics.finish()

    serialized = output_path.read_text(encoding="utf-8")
    payload = json.loads(serialized)
    assert "top-secret" not in serialized
    assert payload["status"] == "failed"
    assert payload["checks"]["read"]["errorType"] == "RuntimeError"


def test_diagnostics_keep_http_status_and_safe_ray_but_not_message(tmp_path: Path):
    class SimulatedHTTPError(RuntimeError):
        status_code = 503
        qb_api_ray = "ray-123"

    output_path = tmp_path / "operational.json"
    diagnostics = OperationalDiagnostics(output_path, run_id="unit-test")

    with pytest.raises(SimulatedHTTPError):
        with diagnostics.check("backup"):
            raise SimulatedHTTPError("authorization=top-secret")

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["checks"]["backup"]["httpStatus"] == 503
    assert payload["checks"]["backup"]["qbApiRay"] == "ray-123"
    assert "top-secret" not in output_path.read_text(encoding="utf-8")


def test_mutating_check_verifies_cleanup_count(tmp_path: Path):
    diagnostics = OperationalDiagnostics(tmp_path / "operational.json", run_id="unit-test")

    with pytest.raises(CleanupCountMismatch, match="deleted 0 record"):
        with diagnostics.mutating_check(
            "upsert",
            cleanup=lambda: 0,
            expected_deleted=lambda: 1,
        ):
            pass

    assert diagnostics.summary["checks"]["upsert"]["cleanup"]["errorType"] == (
        "CleanupCountMismatch"
    )


def test_intentionally_failed_cleanup_is_recovered_by_the_next_run(tmp_path: Path):
    records = {"qbvisor-operational-recovery-proof-upsert"}
    first = OperationalDiagnostics(tmp_path / "first.json", run_id="first")

    with pytest.raises(IntentionalCleanupFailure):
        with first.mutating_check(
            "upsert",
            cleanup=lambda: records.clear() or 1,
            expected_deleted=lambda: 1,
            fail_cleanup=True,
        ):
            pass

    assert records

    def delete_matching() -> int:
        count = len(records)
        records.clear()
        return count

    recovered = recover_operational_records(
        delete_matching=delete_matching,
        count_remaining=lambda: len(records),
    )

    assert recovered == 1
    assert not records


def test_recovery_fails_when_prefixed_records_remain():
    with pytest.raises(AssertionError, match="left 1 prefixed record"):
        recover_operational_records(
            delete_matching=lambda: 0,
            count_remaining=lambda: 1,
        )


def test_finish_requires_the_exact_operational_contract(tmp_path: Path):
    diagnostics = OperationalDiagnostics(tmp_path / "operational.json", run_id="unit-test")
    for check in REQUIRED_OPERATIONAL_CHECKS - {"schema-plan"}:
        with diagnostics.check(check):
            pass

    with pytest.raises(IncompleteOperationalRun, match="every required check"):
        diagnostics.finish(require_complete=True)

    assert diagnostics.summary["status"] == "failed"
    assert diagnostics.summary["missingChecks"] == ["schema-plan"]
    assert diagnostics.summary["failedChecks"] == []


def test_finish_fails_an_explicit_run_when_a_required_check_was_skipped(tmp_path: Path):
    diagnostics = OperationalDiagnostics(tmp_path / "operational.json", run_id="unit-test")
    for check in REQUIRED_OPERATIONAL_CHECKS:
        if check == "attachment":
            with pytest.raises(pytest.skip.Exception):
                with diagnostics.check(check):
                    pytest.skip("simulated unavailable fixture")
        else:
            with diagnostics.check(check):
                pass

    with pytest.raises(IncompleteOperationalRun, match="every required check"):
        diagnostics.finish(require_complete=True)

    assert diagnostics.summary["status"] == "failed"
    assert diagnostics.summary["missingChecks"] == []
    assert diagnostics.summary["failedChecks"] == ["attachment"]


def test_finish_passes_only_after_all_six_required_checks(tmp_path: Path):
    diagnostics = OperationalDiagnostics(tmp_path / "operational.json", run_id="unit-test")
    for check in REQUIRED_OPERATIONAL_CHECKS:
        with diagnostics.check(check):
            pass

    diagnostics.finish(require_complete=True)

    assert diagnostics.summary["status"] == "passed"
    assert diagnostics.summary["missingChecks"] == []
    assert diagnostics.summary["failedChecks"] == []
