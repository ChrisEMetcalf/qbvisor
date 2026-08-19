from __future__ import annotations

import hashlib
import json
import warnings
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest

from benchmarks import scenarios as benchmark_scenarios
from benchmarks.profiles import PROFILES, BenchmarkProfile
from benchmarks.scenarios import SCENARIOS
from qbvisor import SchemaPlan
from qbvisor._records.upsert import MAX_UPSERT_PAYLOAD_BYTES, _json_payload_size


def profile(**updates: int | str) -> BenchmarkProfile:
    values: dict[str, int | str] = {
        "name": "test",
        "record_count": 7,
        "page_size": 3,
        "field_count": 2,
        "attachment_count": 6,
        "attachment_bytes": 31,
        "schema_table_count": 2,
        "schema_fields_per_table": 3,
        "upsert_record_count": 5,
        "upsert_value_bytes": 32,
    }
    values.update(updates)
    return BenchmarkProfile(**values)  # type: ignore[arg-type]


def test_registry_covers_the_seven_surfaces_with_explicit_cold_and_warm_metadata():
    assert list(SCENARIOS) == [
        "attachment-concurrent-transfer",
        "backup-read-verify-dataframe",
        "csv-sequential-export",
        "dataframe-materialization-cold-metadata",
        "dataframe-materialization-warm-metadata",
        "record-keyset-pagination",
        "schema-readonly-planning",
        "upsert-batch-planning",
    ]
    assert list(SCENARIOS) == sorted(SCENARIOS)
    assert all(scenario.description for scenario in SCENARIOS.values())
    assert all(scenario.track_peak_memory for scenario in SCENARIOS.values())


def test_keyset_scenario_consumes_every_record_once_with_sequential_record_id_cursors():
    prepared = cast(
        benchmark_scenarios._PreparedKeysetPagination,
        SCENARIOS["record-keyset-pagination"].prepare(profile()),
    )

    class ForbiddenTimedDatasetAccess:
        def __iter__(self):
            raise AssertionError("timed fake response path scanned the prepared dataset")

        def __len__(self):
            raise AssertionError("timed fake response path inspected the prepared dataset")

        def __getitem__(self, _key):
            raise AssertionError("timed fake response path rebuilt a prepared response")

    prepared.client.records = cast(Any, ForbiddenTimedDatasetAccess())
    try:
        result = prepared.run()
        prepared.validate(result)

        assert result.record_count == 7
        assert result.page_count == 3
        assert [call["where"] for call in prepared.client.calls] == [
            None,
            "{3.GT.3}",
            "{3.GT.6}",
        ]
        assert prepared.client.max_active_queries == 1
        assert prepared.workload_observations(result) == {
            "records": 7,
            "pages": 3,
        }
    finally:
        prepared.cleanup()


def test_dataframe_scenarios_separate_cold_resolution_from_prepared_warm_cache():
    cold = cast(
        benchmark_scenarios._PreparedDataFrame,
        SCENARIOS["dataframe-materialization-cold-metadata"].prepare(profile()),
    )
    warm = cast(
        benchmark_scenarios._PreparedDataFrame,
        SCENARIOS["dataframe-materialization-warm-metadata"].prepare(profile()),
    )
    try:
        assert cold.shape["metadata_cache"] == "cold"
        assert cold.metadata_calls_before == 0
        assert warm.shape["metadata_cache"] == "warm"
        assert warm.metadata_calls_before == 3

        cold_result = cold.run()
        warm_result = warm.run()
        cold.validate(cold_result)
        warm.validate(warm_result)

        assert isinstance(cold_result, pd.DataFrame)
        assert cold_result.equals(warm_result)
        assert len(cold.metadata_transport.calls) - cold.metadata_calls_before == 3
        assert len(warm.metadata_transport.calls) - warm.metadata_calls_before == 0
        assert list(cold_result.columns) == ["Field 1", "Field 2"]
        assert "Record ID#" not in cold_result.columns
        assert cold.workload_observations(cold_result) == {
            "records": 7,
            "cells": 14,
            "pages": 3,
            "metadata_requests": 3,
        }
        assert warm.workload_observations(warm_result) == {
            "records": 7,
            "cells": 14,
            "pages": 3,
            "metadata_requests": 0,
        }
    finally:
        cold.cleanup()
        warm.cleanup()


def test_csv_scenario_keeps_page_reads_sequential_and_publishes_no_partial_file():
    prepared = cast(
        benchmark_scenarios._PreparedCsvExport,
        SCENARIOS["csv-sequential-export"].prepare(profile()),
    )
    work_root = prepared.output_root.parent
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = prepared.run()
        prepared.validate(result)

        assert caught == []
        assert Path(result).is_file()
        assert prepared.client.max_active_queries == 1
        assert all(call["skip"] == 0 for call in prepared.client.calls)
        assert not list(prepared.output_root.glob(".*.tmp"))
        assert prepared.workload_observations(result) == {
            "records": 7,
            "pages": 3,
            "published_bytes": Path(result).stat().st_size,
        }
    finally:
        prepared.cleanup()
    assert not work_root.exists()


def test_upsert_scenario_splits_only_at_the_real_payload_ceiling_without_sending_writes():
    prepared = cast(
        benchmark_scenarios._PreparedUpsertPlanning,
        SCENARIOS["upsert-batch-planning"].prepare(
            profile(
                upsert_record_count=41,
                upsert_value_bytes=1_000_000,
            )
        ),
    )
    try:
        assert prepared.shape["payload_limit_bytes"] == 40_000_000
        unbatched_payload = prepared.shape["unbatched_payload_bytes"]
        assert isinstance(unbatched_payload, int) and not isinstance(unbatched_payload, bool)
        assert unbatched_payload > MAX_UPSERT_PAYLOAD_BYTES

        result = prepared.run()
        prepared.validate(result)

        assert len(result) == 2
        assert sum(len(batch.records) for batch in result) == 41
        assert all(batch.payload_bytes <= MAX_UPSERT_PAYLOAD_BYTES for batch in result)
        assert all(
            batch.payload_bytes == _json_payload_size(batch.json_body(prepared.request_template))
            for batch in result
        )
        assert prepared.shape["planning_only"] is True
        assert prepared.workload_observations(result) == {
            "records": 41,
            "batches": 2,
            "serialized_bytes": sum(batch.payload_bytes for batch in result),
        }
    finally:
        prepared.cleanup()


def test_attachment_scenario_reaches_but_never_exceeds_bound_and_leaves_no_part_files():
    prepared = cast(
        benchmark_scenarios._PreparedAttachmentTransfer,
        SCENARIOS["attachment-concurrent-transfer"].prepare(profile()),
    )
    work_root = prepared.output_root.parent
    try:
        assert all(isinstance(seed, int) for seed in prepared.transport.payload_seeds.values())
        assert not any(
            isinstance(seed, bytes) for seed in prepared.transport.payload_seeds.values()
        )
        result = prepared.run()
        prepared.validate(result)

        assert result.max_active == 4
        assert len(list(prepared.output_root.iterdir())) == 6
        assert not list(prepared.output_root.glob(".*.part"))
        assert all(outcome["status"] == "downloaded" for outcome in result.outcomes)
        assert prepared.workload_observations(result) == {
            "attachments": 6,
            "bytes": 6 * 31,
        }
    finally:
        prepared.cleanup()
    assert not work_root.exists()


def test_backup_scenario_opens_verifies_and_reads_without_modifying_archive():
    prepared = cast(
        benchmark_scenarios._PreparedBackupRead,
        SCENARIOS["backup-read-verify-dataframe"].prepare(profile()),
    )
    before = dict(prepared.fixture_hashes)
    work_root = prepared.backup_root.parent
    try:
        result = prepared.run()
        prepared.validate(result)

        assert result.artifact_count == 3
        assert result.frame.shape == (7, 3)
        assert result.frame.iloc[-1].to_dict() == {
            "Record ID#": 7,
            "Field 1": "record-00000007-field-6",
            "Field 2": "record-00000007-field-7",
        }
        after = {
            path.relative_to(prepared.backup_root).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in prepared.backup_root.rglob("*")
            if path.is_file()
        }
        assert after == before
        assert prepared.workload_observations(result) == {
            "records": 7,
            "artifacts": 3,
            "artifact_bytes": result.total_bytes,
        }
    finally:
        prepared.cleanup()
    assert not work_root.exists()


def test_schema_scenario_plans_matching_metadata_with_gets_only_and_no_state_or_record_reads():
    prepared = cast(
        benchmark_scenarios._PreparedSchemaPlan,
        SCENARIOS["schema-readonly-planning"].prepare(profile()),
    )
    work_root = prepared.work_root
    try:
        result = cast(SchemaPlan, prepared.run())
        prepared.validate(result)

        assert len(result.changes) == 9
        assert result.quickbase_change_count == 0
        assert result.action_counts["unchanged"] == 9
        assert [call["path"] for call in prepared.client.calls] == [
            "apps/appbenchmark",
            "tables",
            "fields",
            "fields",
        ]
        assert all(call["method"] == "GET" for call in prepared.client.calls)
        assert not prepared.state_path.exists()
        assert prepared.workload_observations(result) == {
            "resources": 9,
            "metadata_requests": 4,
        }
    finally:
        prepared.cleanup()
    assert not work_root.exists()


def test_temporary_root_falls_back_when_cwd_temp_creation_itself_fails(
    monkeypatch,
    tmp_path,
):
    calls: list[Path | None] = []

    def fake_mkdtemp(*, prefix: str, dir: Path | None = None) -> str:
        calls.append(dir)
        if dir is not None:
            raise PermissionError("cwd temp is unavailable")
        fallback = tmp_path / f"{prefix}fallback"
        fallback.mkdir()
        return str(fallback)

    monkeypatch.setattr(benchmark_scenarios.tempfile, "mkdtemp", fake_mkdtemp)

    root = benchmark_scenarios._temporary_work_root("qbvisor-test-")
    try:
        assert root.name == "qbvisor-test-fallback"
        assert calls == [Path.cwd(), None]
    finally:
        benchmark_scenarios._remove_tree(root)


def test_file_scenario_prepare_failures_remove_incomplete_fixture_roots(tmp_path):
    cases = (
        (
            "csv-sequential-export",
            "_dataset_client",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture failed")),
        ),
        (
            "attachment-concurrent-transfer",
            "_DeterministicAsyncFileTransport",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture failed")),
        ),
        (
            "backup-read-verify-dataframe",
            "_populate_local_backup",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture failed")),
        ),
    )
    for index, (scenario_name, failure_target, failure) in enumerate(cases):
        work_root = tmp_path / f"case-{index}"
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(
                benchmark_scenarios,
                "_temporary_work_root",
                lambda _prefix, root=work_root: (root.mkdir(), root)[1],
            )
            patch.setattr(benchmark_scenarios, failure_target, failure)

            with pytest.raises(RuntimeError, match="fixture failed"):
                SCENARIOS[scenario_name].prepare(profile())

        assert not work_root.exists()


def test_every_prepared_shape_is_deterministic_json_and_exposes_positive_operation_units():
    for scenario in SCENARIOS.values():
        prepared = scenario.prepare(profile())
        try:
            first = json.dumps(dict(prepared.shape), sort_keys=True, allow_nan=False)
            second = json.dumps(dict(prepared.shape), sort_keys=True, allow_nan=False)

            assert first == second
            assert prepared.unit_count > 0
            assert prepared.unit_label
        finally:
            prepared.cleanup()


@pytest.mark.parametrize(
    ("scenario_name", "expected_keys"),
    [
        ("attachment-concurrent-transfer", {"attachments", "bytes"}),
        ("backup-read-verify-dataframe", {"records", "artifacts", "artifact_bytes"}),
        ("csv-sequential-export", {"records", "pages", "published_bytes"}),
        (
            "dataframe-materialization-cold-metadata",
            {"records", "cells", "pages", "metadata_requests"},
        ),
        (
            "dataframe-materialization-warm-metadata",
            {"records", "cells", "pages", "metadata_requests"},
        ),
        ("record-keyset-pagination", {"records", "pages"}),
        ("schema-readonly-planning", {"resources", "metadata_requests"}),
        ("upsert-batch-planning", {"records", "batches", "serialized_bytes"}),
    ],
)
def test_workload_observations_are_stable_json_safe_per_operation(
    scenario_name: str,
    expected_keys: set[str],
):
    repetitions: list[dict[str, int | float]] = []
    for _ in range(2):
        prepared = SCENARIOS[scenario_name].prepare(profile())
        try:
            result = prepared.run()
            prepared.validate(result)
            observations = dict(prepared.workload_observations(result))
            json.dumps(observations, sort_keys=True, allow_nan=False)

            assert set(observations) == expected_keys
            assert all(
                isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0
                for value in observations.values()
            )
            repetitions.append(observations)
        finally:
            prepared.cleanup()

    assert repetitions[0] == repetitions[1]


@pytest.mark.parametrize(
    ("profile_name", "expected_batch_count"),
    [("small", 1), ("medium", 1), ("large", 2)],
)
def test_upsert_observations_cover_each_profile_payload_boundary(
    profile_name: str,
    expected_batch_count: int,
):
    benchmark_profile = PROFILES[profile_name]
    prepared = cast(
        benchmark_scenarios._PreparedUpsertPlanning,
        SCENARIOS["upsert-batch-planning"].prepare(benchmark_profile),
    )
    try:
        result = prepared.run()
        prepared.validate(result)
        observations = prepared.workload_observations(result)

        assert observations == {
            "records": benchmark_profile.upsert_record_count,
            "batches": expected_batch_count,
            "serialized_bytes": sum(batch.payload_bytes for batch in result),
        }
        assert observations["serialized_bytes"] <= (expected_batch_count * MAX_UPSERT_PAYLOAD_BYTES)
        unbatched_payload_bytes = prepared.shape["unbatched_payload_bytes"]
        assert isinstance(unbatched_payload_bytes, int) and not isinstance(
            unbatched_payload_bytes, bool
        )
        assert (expected_batch_count > 1) == (unbatched_payload_bytes > MAX_UPSERT_PAYLOAD_BYTES)
    finally:
        prepared.cleanup()
