from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from qbvisor import ApplicationBackup, BackupOptions, QuickBaseClient
from qbvisor.exceptions import BackupConsistencyError, BackupIntegrityError


class FakeBackupClient:
    def __init__(self, *, changed: bool = False, fail_query: bool = False):
        self.transport = SimpleNamespace(realm_hostname="example.quickbase.com")
        self.changed = changed
        self.fail_query = fail_query

    def _ids(self, app_name: str, table_name: str | None = None) -> tuple[str, str | None]:
        assert app_name in {"Operations", "appoperations"}
        if table_name is None:
            return "appoperations", None
        assert table_name == "tbl_projects"
        return "appoperations", "tbl_projects"

    def get_app(self, app_name: str) -> dict[str, Any]:
        return {"id": "appoperations", "name": "Operations", "description": "Test app"}

    def get_app_events(self, app_name: str) -> list[dict[str, Any]]:
        return []

    def get_app_roles(self, app_name: str) -> list[dict[str, Any]]:
        return [{"id": 10, "name": "Viewer"}]

    def get_tables_for_app(self, app_name: str) -> list[dict[str, Any]]:
        return [{"id": "tbl_projects", "name": "Projects"}]

    def get_table(self, app_name: str, table_name: str) -> dict[str, Any]:
        return {"id": "tbl_projects", "name": "Projects", "nextRecordId": 3}

    def get_fields_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        return [
            {"id": 3, "label": "Record ID#", "fieldType": "recordid"},
            {"id": 6, "label": "Name", "fieldType": "text"},
        ]

    def get_all_relationships(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        return []

    def get_reports_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        return []

    def get_report(self, app_name: str, table_name: str, report_id: int) -> dict[str, Any]:
        raise AssertionError("The fake table has no reports")

    def _query_records_by_ids(
        self,
        table_id: str,
        *,
        select_fields: Sequence[int] | None = None,
        where: str | None = None,
        sort_by: Sequence[tuple[int, str]] | None = None,
        group_by: Sequence[int] | None = None,
        skip: int = 0,
        top: int = 1000,
    ) -> dict[str, Any]:
        if self.fail_query:
            raise RuntimeError("query failed")
        assert select_fields == (3, 6)
        assert where is None
        return {
            "fields": [{"id": 3}, {"id": 6}],
            "data": [
                {"3": {"value": 1}, "6": {"value": "Alpha"}},
                {"3": {"value": 2}, "6": {"value": "Beta"}},
            ],
            "metadata": {"numFields": 2, "numRecords": 2, "totalRecords": 2},
        }

    def records_modified_since(
        self,
        app_name: str,
        table_name: str,
        after: Any,
        *,
        field_list: Any = None,
        include_details: bool = False,
    ) -> dict[str, Any]:
        return {"count": 1 if self.changed else 0}


def create_backup(client: FakeBackupClient, output: Path, **options: Any) -> ApplicationBackup:
    return QuickBaseClient.backup_app(
        client,
        "Operations",
        output,
        options=BackupOptions(attachment_versions="none", **options),
    )


def write_manifest(root: Path, payload: dict[str, Any]) -> ApplicationBackup:
    (root / "manifest.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ApplicationBackup.open(root)


def replace_artifact(
    backup: ApplicationBackup,
    relative_path: str,
    content: bytes,
    *,
    item_count: int,
) -> dict[str, Any]:
    (backup.path / relative_path).write_bytes(content)
    payload = deepcopy(backup.manifest.to_dict())
    artifact = next(item for item in payload["artifacts"] if item["path"] == relative_path)
    artifact.update(
        sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        item_count=item_count,
    )
    return payload


def backup_with_one_attachment(tmp_path: Path) -> tuple[ApplicationBackup, dict[str, Any]]:
    backup = create_backup(FakeBackupClient(), tmp_path)
    attachment_path = "tables/tbl_projects/attachments/1/8/1/evidence.txt"
    attachment = b"verified attachment"
    (backup.path / attachment_path).parent.mkdir(parents=True)
    (backup.path / attachment_path).write_bytes(attachment)

    index_path = "tables/tbl_projects/attachments.jsonl"
    entry = {
        "path": attachment_path,
        "sha256": hashlib.sha256(attachment).hexdigest(),
        "bytes": len(attachment),
    }
    index_content = (json.dumps(entry, sort_keys=True) + "\n").encode()
    payload = replace_artifact(backup, index_path, index_content, item_count=1)
    payload["artifacts"].append(
        {
            "path": attachment_path,
            "kind": "attachment",
            "sha256": entry["sha256"],
            "bytes": entry["bytes"],
        }
    )
    table = payload["tables"][0]
    table["attachment_count"] = 1
    table["artifacts"].append(attachment_path)
    return write_manifest(backup.path, payload), entry


def test_application_backup_is_atomic_verifiable_and_dataframe_ready(tmp_path):
    backup = create_backup(FakeBackupClient(), tmp_path)

    assert backup.path.parent == tmp_path.resolve()
    assert backup.path.name.startswith("appoperations-")
    assert not list(tmp_path.glob(".qbvisor-*.tmp"))
    assert (backup.path / "manifest.json").is_file()
    verification = backup.verify()
    assert verification.artifact_count == len(backup.manifest.artifacts)
    assert verification.total_bytes > 0

    reopened = ApplicationBackup.open(backup.path)
    assert reopened.manifest == backup.manifest
    frame = reopened.table_dataframe("Projects")
    assert list(frame["Record ID#"]) == [1, 2]
    assert list(frame["Name"]) == ["Alpha", "Beta"]


def test_integrity_verification_detects_artifact_tampering(tmp_path):
    backup = create_backup(FakeBackupClient(), tmp_path)
    records = backup.path / "tables/tbl_projects/records.jsonl"
    records.write_text(records.read_text() + '{"3":{"value":99}}\n')

    with pytest.raises(BackupIntegrityError) as caught:
        backup.verify()

    assert any("hash does not match" in issue for issue in caught.value.issues)
    assert any("item count does not match" in issue for issue in caught.value.issues)


def test_integrity_verification_detects_manifest_changes_after_open(tmp_path):
    backup = create_backup(FakeBackupClient(), tmp_path)
    manifest_path = backup.path / "manifest.json"
    payload = backup.manifest.to_dict()
    payload["source"]["app_name"] = "Changed"
    manifest_path.write_text(json.dumps(payload))

    with pytest.raises(BackupIntegrityError, match="manifest.json changed"):
        backup.verify()


def test_failed_backup_removes_its_incomplete_staging_directory(tmp_path):
    with pytest.raises(RuntimeError, match="query failed"):
        create_backup(FakeBackupClient(fail_query=True), tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_consistency_policy_can_report_or_reject_concurrent_changes(tmp_path):
    reported = create_backup(FakeBackupClient(changed=True), tmp_path / "reported")
    assert reported.manifest.consistent is False
    assert reported.manifest.changed_tables == ("tbl_projects",)

    rejected_root = tmp_path / "rejected"
    with pytest.raises(BackupConsistencyError) as caught:
        create_backup(FakeBackupClient(changed=True), rejected_root, fail_on_changes=True)

    assert caught.value.changed_tables == ("tbl_projects",)
    assert list(rejected_root.iterdir()) == []


def test_open_backup_rejects_unsupported_manifest_changes(tmp_path):
    backup = create_backup(FakeBackupClient(), tmp_path)
    manifest_path = backup.path / "manifest.json"
    payload = deepcopy(backup.manifest.to_dict())
    payload["format_version"] = 999
    manifest_path.write_text(json.dumps(payload))

    with pytest.raises(ValueError, match="Unsupported backup format version"):
        ApplicationBackup.open(backup.path)


@pytest.mark.parametrize(
    ("content", "item_count", "expected_issue"),
    [
        (b'{"3":{"value":1}}\nnot-json\n', 1, "contains invalid JSON at line 2"),
        (b'{"3":{"value":1}}\n[]\n', 2, "contains a non-object at line 2"),
    ],
)
def test_semantically_corrupt_records_fail_with_matching_manifest_integrity(
    tmp_path,
    content,
    item_count,
    expected_issue,
):
    backup = create_backup(FakeBackupClient(), tmp_path)
    records_path = "tables/tbl_projects/records.jsonl"
    payload = replace_artifact(backup, records_path, content, item_count=item_count)
    payload["tables"][0]["record_count"] = item_count
    reopened = write_manifest(backup.path, payload)

    with pytest.raises(BackupIntegrityError) as caught:
        reopened.verify()

    assert any(expected_issue in issue for issue in caught.value.issues)
    assert not any("hash does not match" in issue for issue in caught.value.issues)
    assert not any("byte count does not match" in issue for issue in caught.value.issues)
    assert not any("item count does not match" in issue for issue in caught.value.issues)


@pytest.mark.parametrize(
    ("entries", "attachment_count", "expected_issue"),
    [
        (
            [
                {
                    "path": "tables/another_table/attachments/1/8/1/evidence.txt",
                    "sha256": "unused",
                    "bytes": 19,
                }
            ],
            1,
            "references another table",
        ),
        (
            [
                {
                    "path": "tables/tbl_projects/attachments/1/8/1/missing.txt",
                    "sha256": "unused",
                    "bytes": 19,
                }
            ],
            1,
            "references a missing attachment",
        ),
        ("duplicate", 2, "attachment is indexed more than once"),
        ("incorrect-integrity", 1, "has incorrect attachment integrity"),
        ([], 0, "attachment artifacts are not indexed"),
    ],
)
def test_attachment_index_semantics_reject_validly_hashed_corruption(
    tmp_path,
    entries,
    attachment_count,
    expected_issue,
):
    backup, valid_entry = backup_with_one_attachment(tmp_path)
    if entries == "duplicate":
        entries = [valid_entry, valid_entry]
    elif entries == "incorrect-integrity":
        entries = [{**valid_entry, "sha256": "0" * 64, "bytes": 0}]

    index_path = "tables/tbl_projects/attachments.jsonl"
    content = "".join(json.dumps(entry, sort_keys=True) + "\n" for entry in entries).encode()
    payload = replace_artifact(
        backup,
        index_path,
        content,
        item_count=len(entries),
    )
    payload["tables"][0]["attachment_count"] = attachment_count
    reopened = write_manifest(backup.path, payload)

    with pytest.raises(BackupIntegrityError) as caught:
        reopened.verify()

    assert any(expected_issue in issue for issue in caught.value.issues)
    assert not any("hash does not match" in issue for issue in caught.value.issues)
    assert not any("byte count does not match" in issue for issue in caught.value.issues)
    assert not any("item count does not match" in issue for issue in caught.value.issues)


def test_partial_copy_reports_missing_and_untracked_artifacts_together(tmp_path):
    backup = create_backup(FakeBackupClient(), tmp_path)
    missing = next(
        artifact for artifact in backup.manifest.artifacts if artifact.kind == "application"
    )
    (backup.path / missing.path).unlink()
    (backup.path / "untracked.txt").write_text("not declared by the manifest")

    with pytest.raises(BackupIntegrityError) as caught:
        backup.verify()

    assert any(
        "backup files are missing" in issue and missing.path in issue
        for issue in caught.value.issues
    )
    assert any(
        "backup contains untracked files" in issue and "untracked.txt" in issue
        for issue in caught.value.issues
    )
