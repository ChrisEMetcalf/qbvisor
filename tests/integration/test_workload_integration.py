from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from platform import python_version
from time import perf_counter
from typing import Any

import pandas as pd
import pytest
from conftest import APP_NAME, MUTATION_ENV, SandboxContract
from workload_support import (
    build_update_pass,
    build_workload_records,
    finalize_workload_run,
    get_workload_profile,
    select_workload_rows,
)

from qbvisor import BackupOptions, QueryHelper, QuickBaseClient, __version__

WORKLOAD_ENV = "QBVISOR_RUN_WORKLOADS"
PROFILE_ENV = "QBVISOR_WORKLOAD_PROFILE"
RESULTS_ENV = "QBVISOR_WORKLOAD_RESULTS"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.sandbox_mutation,
    pytest.mark.workload,
    pytest.mark.skipif(
        os.getenv(WORKLOAD_ENV) != "1",
        reason=f"Set {WORKLOAD_ENV}=1 to run the generated-record stabilization workload",
    ),
    pytest.mark.skipif(
        os.getenv(MUTATION_ENV) != "1",
        reason=f"Set {MUTATION_ENV}=1 to allow temporary workload records",
    ),
]


@contextmanager
def _timed(timings: dict[str, float], operation: str) -> Iterator[None]:
    started = perf_counter()
    try:
        yield
    finally:
        timings[operation] = round(perf_counter() - started, 4)


def _write_summary(summary: dict[str, Any], run_id: str) -> Path:
    output_dir = Path(os.getenv(RESULTS_ENV, ".qbvisor/workloads"))
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{run_id}.json"
    output_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return output_path


def _amounts_by_key(frame: pd.DataFrame) -> dict[str, float]:
    return {
        str(key): float(amount)
        for key, amount in zip(frame["Fixture Key"], frame["Amount"], strict=True)
    }


def test_generated_record_workload_round_trips_and_cleans_up(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    tmp_path: Path,
):
    profile = get_workload_profile(os.getenv(PROFILE_ENV))
    run_id = f"qbvisor-workload-{uuid.uuid4().hex[:12]}"
    records = build_workload_records(profile, run_id)
    update_pass, expected_updated = build_update_pass(profile, records)
    expected_unchanged = profile.record_count - expected_updated
    expected_amounts = {
        str(record["Fixture Key"]): float(record["Amount"]) for record in update_pass
    }
    timings: dict[str, float] = {}
    total_started = perf_counter()
    summary: dict[str, Any] = {
        "schemaVersion": 2,
        "runId": run_id,
        "status": "running",
        "startedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "qbvisorVersion": __version__,
        "pythonVersion": python_version(),
        "profile": profile.to_dict(),
        "backupScope": "full-application",
        "exportPaginationMode": "sequential-keyset",
        "operationsSeconds": timings,
    }
    phase = "query_setup"
    primary_error: BaseException | None = None
    failure_phase: str | None = None
    where: str | None = None
    create_confirmed = False

    try:
        query = QueryHelper(sandbox_client, APP_NAME, sandbox_contract.records_table_id)
        where = query.starts_with("Fixture Key", f"{run_id}-")

        phase = "create"
        with _timed(timings, "create"):
            created = sandbox_client.upsert_records(
                APP_NAME,
                sandbox_contract.records_table_id,
                records,
                merge_field_label="Fixture Key",
                fields_to_return=["Fixture Key", "Amount", "Status"],
            )
        assert created["success"] is True
        assert created["totalProcessed"] == profile.record_count
        assert "lineErrors" not in created
        assert len(created["createdRecordIds"]) == profile.record_count
        assert created["updatedRecordIds"] == []
        assert created["unchangedRecordIds"] == []
        assert len(created["data"]) == profile.record_count
        create_confirmed = True

        phase = "update_and_replay"
        with _timed(timings, "update_and_replay"):
            replayed = sandbox_client.upsert_records(
                APP_NAME,
                sandbox_contract.records_table_id,
                update_pass,
                merge_field_label="Fixture Key",
                fields_to_return=["Fixture Key", "Amount", "Status"],
            )
        assert replayed["success"] is True
        assert replayed["totalProcessed"] == profile.record_count
        assert "lineErrors" not in replayed
        assert replayed["createdRecordIds"] == []
        assert len(replayed["updatedRecordIds"]) == expected_updated
        assert len(replayed["unchangedRecordIds"]) == expected_unchanged
        assert len(replayed["data"]) == profile.record_count

        phase = "query_dataframe"
        with _timed(timings, "query_dataframe"):
            frame = sandbox_client.query_dataframe(
                APP_NAME,
                sandbox_contract.records_table_id,
                ["Fixture Key", "Name", "Amount", "Status", "Active", "Event Date"],
                where=where,
            )
        assert len(frame) == profile.record_count
        assert set(frame["Fixture Key"]) == {record["Fixture Key"] for record in records}
        assert pd.api.types.is_numeric_dtype(frame["Amount"])
        assert _amounts_by_key(frame) == pytest.approx(expected_amounts)

        phase = "csv_export"
        with _timed(timings, "csv_export"):
            csv_path = sandbox_client.download_records_to_csv(
                APP_NAME,
                sandbox_contract.records_table_id,
                str(tmp_path / "csv"),
                where=where,
                chunk_size=profile.export_chunk_size,
            )
            exported = pd.read_csv(csv_path)
        assert len(exported) == profile.record_count
        assert set(exported["Fixture Key"]) == set(frame["Fixture Key"])
        assert _amounts_by_key(exported) == pytest.approx(expected_amounts)

        phase = "backup"
        with _timed(timings, "backup"):
            backup = sandbox_client.backup_app(
                APP_NAME,
                tmp_path / "backups",
                options=BackupOptions(
                    attachment_versions="latest",
                    page_size=profile.backup_page_size,
                    fail_on_changes=True,
                ),
            )
            verification = backup.verify()
            backup_frame = backup.table_dataframe(sandbox_contract.records_table_id)
        backed_up = select_workload_rows(backup_frame, run_id)
        assert len(backed_up) == profile.record_count
        assert _amounts_by_key(backed_up) == pytest.approx(expected_amounts)
        assert backup.manifest.consistent is True
        assert backup.manifest.source_app_id == sandbox_contract.app_id
        assert verification.artifact_count == len(backup.manifest.artifacts)

        phase = "result_summary"
        summary.update(
            {
                "createdRecords": len(created["createdRecordIds"]),
                "updatedRecords": len(replayed["updatedRecordIds"]),
                "unchangedRecords": len(replayed["unchangedRecordIds"]),
                "queriedRecords": len(frame),
                "exportedRecords": len(exported),
                "backedUpRecords": len(backed_up),
                "backupArtifacts": verification.artifact_count,
                "backupBytes": verification.total_bytes,
                "backupConsistent": backup.manifest.consistent,
                "backupTotalRecords": sum(table.record_count for table in backup.manifest.tables),
                "backupTotalAttachments": sum(
                    table.attachment_count for table in backup.manifest.tables
                ),
            }
        )
    except BaseException as error:
        primary_error = error
        failure_phase = phase
    finally:

        def cleanup() -> int:
            if where is None:
                return 0
            with _timed(timings, "cleanup"):
                return sandbox_client.delete_records(
                    APP_NAME,
                    sandbox_contract.records_table_id,
                    where,
                )

        def persist_summary(result: dict[str, Any]) -> Path:
            result["completedAt"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
            result["totalSeconds"] = round(perf_counter() - total_started, 4)
            path = _write_summary(result, run_id)
            print(f"Workload summary: {path}")
            return path

        finalize_workload_run(
            summary,
            expected_deleted=profile.record_count if create_confirmed else None,
            cleanup=cleanup,
            write_summary=persist_summary,
            primary_error=primary_error,
            failure_phase=failure_phase,
        )
