import asyncio
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from qbvisor._backup import (
    BackupWorkspace,
    CapturedSchema,
    CapturedTable,
    capture_attachments,
)


class FakeAsyncFileTransport:
    def __init__(self, files: dict[str, bytes]):
        self.files = files
        self.calls: list[str] = []
        self.active = 0
        self.max_active = 0
        self.enter_count = 0

    async def __aenter__(self):
        self.enter_count += 1
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get_file(self, path: str) -> bytes:
        self.calls.append(path)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)
        self.active -= 1
        return self.files[path]


def schema() -> CapturedSchema:
    return CapturedSchema(
        app_id="app_operations",
        app_name="Operations",
        tables=(
            CapturedTable(
                id="tbl_projects",
                name="Projects",
                fields=(
                    {"id": 3, "label": "Record ID#", "fieldType": "recordid"},
                    {"id": 8, "label": "Evidence", "fieldType": "file"},
                ),
                artifacts=("tables/tbl_projects/records.jsonl",),
            ),
        ),
        artifacts=("app.json", "tables/tbl_projects/records.jsonl"),
    )


def seed_records(root: Path) -> None:
    workspace = BackupWorkspace(root)
    workspace.write_json_lines(
        "tables/tbl_projects/records.jsonl",
        "records",
        [
            {
                "3": {"value": 10},
                "8": {
                    "value": {
                        "fileName": "current.txt",
                        "versions": [
                            {"versionNumber": 2, "fileName": "../current?.txt", "size": 6},
                            {"versionNumber": 1, "fileName": "original.txt", "size": 5},
                        ],
                    }
                },
            },
            {
                "3": {"value": 11},
                "8": {
                    "value": {
                        "versions": [{"versionNumber": 1, "fileName": "second.txt", "size": 6}]
                    }
                },
            },
        ],
    )


def test_all_attachment_versions_are_hashed_indexed_and_bounded(tmp_path):
    seed_records(tmp_path)
    transport = FakeAsyncFileTransport(
        {
            "files/tbl_projects/10/8/1": b"first",
            "files/tbl_projects/10/8/2": b"second",
            "files/tbl_projects/11/8/1": b"eleven",
        }
    )
    workspace = BackupWorkspace(tmp_path)

    captured = capture_attachments(
        SimpleNamespace(transport=SimpleNamespace()),
        schema(),
        workspace,
        mode="all",
        max_concurrency=2,
        transport_factory=lambda _: transport,
    )

    assert captured.tables[0].attachment_count == 3
    assert transport.max_active == 2
    expected = tmp_path / "tables/tbl_projects/attachments/10/8/2/current.txt"
    assert expected.read_bytes() == b"second"
    assert expected.is_relative_to(tmp_path)
    entries = [
        json.loads(line)
        for line in (tmp_path / "tables/tbl_projects/attachments.jsonl").read_text().splitlines()
    ]
    assert [(entry["record_id"], entry["version_number"]) for entry in entries] == [
        (10, 1),
        (10, 2),
        (11, 1),
    ]
    assert entries[1]["metadata"]["size"] == 6
    assert (
        entries[1]["sha256"] == "16367aacb67a4a017c8da8ab95682ccb390863780f7114dda0a0e0c55644c7c4"
    )


def test_latest_mode_archives_only_the_highest_version(tmp_path):
    seed_records(tmp_path)
    transport = FakeAsyncFileTransport(
        {
            "files/tbl_projects/10/8/2": b"second",
            "files/tbl_projects/11/8/1": b"eleven",
        }
    )

    captured = capture_attachments(
        SimpleNamespace(transport=SimpleNamespace()),
        schema(),
        BackupWorkspace(tmp_path),
        mode="latest",
        max_concurrency=4,
        transport_factory=lambda _: transport,
    )

    assert captured.tables[0].attachment_count == 2
    assert transport.calls == [
        "files/tbl_projects/10/8/2",
        "files/tbl_projects/11/8/1",
    ]


def test_none_mode_writes_an_empty_index_without_opening_files(tmp_path):
    seed_records(tmp_path)
    transport = FakeAsyncFileTransport({})

    captured = capture_attachments(
        SimpleNamespace(transport=SimpleNamespace()),
        schema(),
        BackupWorkspace(tmp_path),
        mode="none",
        max_concurrency=2,
        transport_factory=lambda _: transport,
    )

    assert captured.tables[0].attachment_count == 0
    assert transport.calls == []
    assert (tmp_path / "tables/tbl_projects/attachments.jsonl").read_bytes() == b""


def test_duplicate_attachment_versions_abort_before_overwriting(tmp_path):
    workspace = BackupWorkspace(tmp_path)
    workspace.write_json_lines(
        "tables/tbl_projects/records.jsonl",
        "records",
        [
            {
                "3": {"value": 10},
                "8": {
                    "value": {
                        "versions": [
                            {"versionNumber": 1, "fileName": "first.txt"},
                            {"versionNumber": 1, "fileName": "duplicate.txt"},
                        ]
                    }
                },
            }
        ],
    )

    with pytest.raises(ValueError, match="contain duplicates"):
        capture_attachments(
            SimpleNamespace(transport=SimpleNamespace()),
            schema(),
            BackupWorkspace(tmp_path),
            mode="all",
            max_concurrency=2,
            transport_factory=lambda _: FakeAsyncFileTransport({}),
        )

    assert not (tmp_path / "tables/tbl_projects/attachments.jsonl").exists()


def test_attachment_capture_rejects_unbounded_worker_configuration(tmp_path):
    with pytest.raises(ValueError, match="max_concurrency must be at least 1"):
        capture_attachments(
            SimpleNamespace(transport=SimpleNamespace()),
            schema(),
            BackupWorkspace(tmp_path),
            mode="all",
            max_concurrency=0,
            transport_factory=lambda _: FakeAsyncFileTransport({}),
        )


def test_multiple_tables_share_one_async_transport_session(tmp_path):
    seed_records(tmp_path)
    workspace = BackupWorkspace(tmp_path)
    workspace.write_json_lines("tables/tbl_notes/records.jsonl", "records", [])
    original = schema()
    second_table = CapturedTable(
        id="tbl_notes",
        name="Notes",
        fields=({"id": 3, "label": "Record ID#", "fieldType": "recordid"},),
        artifacts=("tables/tbl_notes/records.jsonl",),
    )
    multi_table_schema = CapturedSchema(
        app_id=original.app_id,
        app_name=original.app_name,
        tables=(*original.tables, second_table),
        artifacts=(*original.artifacts, "tables/tbl_notes/records.jsonl"),
    )
    transport = FakeAsyncFileTransport(
        {
            "files/tbl_projects/10/8/2": b"second",
            "files/tbl_projects/11/8/1": b"eleven",
        }
    )

    captured = capture_attachments(
        SimpleNamespace(transport=SimpleNamespace()),
        multi_table_schema,
        BackupWorkspace(tmp_path),
        mode="latest",
        max_concurrency=2,
        transport_factory=lambda _: transport,
    )

    assert transport.enter_count == 1
    assert [table.attachment_count for table in captured.tables] == [2, 0]
