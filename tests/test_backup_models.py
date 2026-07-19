from copy import deepcopy
from pathlib import Path

import pytest

from qbvisor.backup import (
    ApplicationBackup,
    BackupArtifact,
    BackupManifest,
    BackupOptions,
    BackupTable,
)

ZERO_SHA256 = "0" * 64


def sample_manifest() -> BackupManifest:
    artifacts = (
        BackupArtifact(
            path="app.json",
            kind="application",
            sha256=ZERO_SHA256,
            bytes=128,
        ),
        BackupArtifact(
            path="tables/tbl_projects/records.jsonl",
            kind="records",
            sha256="1" * 64,
            bytes=512,
            item_count=2,
        ),
    )
    table = BackupTable(
        id="tbl_projects",
        name="Projects",
        record_count=2,
        attachment_count=0,
        artifacts=("tables/tbl_projects/records.jsonl",),
    )
    return BackupManifest(
        snapshot_id="12345678-1234-5678-1234-567812345678",
        source_realm="example.quickbase.com",
        source_app_id="app_operations",
        source_app_name="Operations",
        qbvisor_version="0.2.0",
        started_at="2026-07-19T01:00:00Z",
        completed_at="2026-07-19T01:05:00Z",
        options=BackupOptions(),
        consistent=True,
        changed_tables=(),
        tables=(table,),
        artifacts=artifacts,
    )


def test_backup_manifest_round_trips_without_losing_contract_data():
    manifest = sample_manifest()

    restored = BackupManifest.from_dict(manifest.to_dict())
    backup = ApplicationBackup(Path("/tmp/operations-backup"), restored)

    assert restored == manifest
    assert backup.manifest.tables[0].record_count == 2
    assert restored.to_dict()["format_version"] == 1


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"attachment_versions": "some"}, "Unsupported attachment version mode"),
        ({"page_size": 0}, "page_size must be between"),
        ({"page_size": 1001}, "page_size must be between"),
        ({"max_attachment_concurrency": 0}, "must be at least 1"),
    ],
)
def test_backup_options_reject_values_that_could_create_incomplete_runs(kwargs, message):
    with pytest.raises(ValueError, match=message):
        BackupOptions(**kwargs)


@pytest.mark.parametrize("path", ["../token", "/absolute/file", "tables\\escape"])
def test_backup_artifacts_cannot_escape_the_snapshot_directory(path):
    with pytest.raises(ValueError, match="backup directory|POSIX relative path"):
        BackupArtifact(
            path=path,
            kind="records",
            sha256=ZERO_SHA256,
            bytes=1,
        )


def test_manifest_rejects_unknown_versions_and_missing_artifact_references():
    payload = sample_manifest().to_dict()
    unknown = deepcopy(payload)
    unknown["format_version"] = 2
    with pytest.raises(ValueError, match="Unsupported backup format version"):
        BackupManifest.from_dict(unknown)

    missing = deepcopy(payload)
    missing["artifacts"] = [missing["artifacts"][0]]
    with pytest.raises(ValueError, match="references missing artifacts"):
        BackupManifest.from_dict(missing)


def test_manifest_cannot_claim_consistency_when_tables_changed():
    payload = sample_manifest().to_dict()
    payload["consistency"]["changed_tables"] = ["tbl_projects"]

    with pytest.raises(ValueError, match="consistent backup cannot contain changed_tables"):
        BackupManifest.from_dict(payload)


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("options", "page_size"), "1000", "page_size must be an integer"),
        (("consistency", "consistent"), "yes", "consistent must be a boolean"),
        (("source", "app_id"), "", "app_id must be a non-empty string"),
    ],
)
def test_manifest_rejects_invalid_field_types(path, value, message):
    payload = sample_manifest().to_dict()
    parent, key = path
    payload[parent][key] = value

    with pytest.raises(ValueError, match=message):
        BackupManifest.from_dict(payload)
