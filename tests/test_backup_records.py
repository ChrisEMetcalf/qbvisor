import json
from collections.abc import Sequence
from typing import Any

import pytest

from qbvisor._backup import (
    BackupWorkspace,
    CapturedSchema,
    CapturedTable,
    capture_records,
)
from qbvisor.exceptions import QuickbaseResponseError


def record(record_id: int, name: str) -> dict[str, Any]:
    return {"3": {"value": record_id}, "6": {"value": name}}


class FakeRecordClient:
    def __init__(self, pages: list[dict[str, Any]]):
        self.pages = iter(pages)
        self.calls: list[dict[str, Any]] = []

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
        self.calls.append(
            {
                "table_id": table_id,
                "select_fields": select_fields,
                "where": where,
                "sort_by": sort_by,
                "group_by": group_by,
                "skip": skip,
                "top": top,
            }
        )
        return next(self.pages)


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
                    {"id": 6, "label": "Name", "fieldType": "text"},
                ),
                artifacts=("tables/tbl_projects/fields.json",),
            ),
        ),
        artifacts=("app.json", "tables/tbl_projects/fields.json"),
    )


def test_records_stream_with_keyset_pagination_and_all_field_ids(tmp_path):
    client = FakeRecordClient(
        [
            {
                "data": [record(1, "Alpha"), record(4, "Delta")],
                "fields": [{"id": 3}, {"id": 6}],
                "metadata": {"numFields": 2, "numRecords": 2, "totalRecords": 3},
            },
            {
                "data": [record(9, "Iota")],
                "fields": [{"id": 3}, {"id": 6}],
                "metadata": {"numFields": 2, "numRecords": 1, "totalRecords": 1},
            },
        ]
    )
    workspace = BackupWorkspace(tmp_path)

    captured = capture_records(client, schema(), workspace, page_size=2)

    lines = (tmp_path / "tables/tbl_projects/records.jsonl").read_text().splitlines()
    assert [json.loads(line) for line in lines] == [
        record(1, "Alpha"),
        record(4, "Delta"),
        record(9, "Iota"),
    ]
    assert captured.tables[0].record_count == 3
    assert client.calls == [
        {
            "table_id": "tbl_projects",
            "select_fields": (3, 6),
            "where": None,
            "sort_by": ((3, "ASC"),),
            "group_by": None,
            "skip": 0,
            "top": 2,
        },
        {
            "table_id": "tbl_projects",
            "select_fields": (3, 6),
            "where": "{3.GT.4}",
            "sort_by": ((3, "ASC"),),
            "group_by": None,
            "skip": 0,
            "top": 2,
        },
    ]


def test_empty_table_produces_a_verifiable_empty_jsonl_artifact(tmp_path):
    client = FakeRecordClient(
        [
            {
                "data": [],
                "fields": [{"id": 3}, {"id": 6}],
                "metadata": {"numFields": 2, "numRecords": 0, "totalRecords": 0},
            }
        ]
    )
    workspace = BackupWorkspace(tmp_path)

    captured = capture_records(client, schema(), workspace, page_size=1000)

    artifact = workspace.artifacts[-1]
    assert captured.tables[0].record_count == 0
    assert artifact.bytes == 0
    assert artifact.sha256 == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_invalid_pagination_removes_partial_record_artifact(tmp_path):
    client = FakeRecordClient(
        [
            {
                "data": [record(1, "Alpha"), record(1, "Duplicate")],
                "fields": [{"id": 3}, {"id": 6}],
                "metadata": {"numFields": 2, "numRecords": 2, "totalRecords": 2},
            }
        ]
    )

    with pytest.raises(QuickbaseResponseError, match="strictly increasing"):
        capture_records(client, schema(), BackupWorkspace(tmp_path), page_size=1000)

    assert not (tmp_path / "tables/tbl_projects/records.jsonl").exists()
    assert not list(tmp_path.rglob("*.tmp"))


def test_incomplete_query_fields_remove_the_partial_record_artifact(tmp_path):
    client = FakeRecordClient(
        [
            {
                "data": [record(1, "Alpha")],
                "fields": [{"id": 3}],
                "metadata": {"numFields": 1, "numRecords": 1, "totalRecords": 1},
            }
        ]
    )

    with pytest.raises(QuickbaseResponseError, match="all requested field IDs"):
        capture_records(client, schema(), BackupWorkspace(tmp_path), page_size=1000)

    assert not (tmp_path / "tables/tbl_projects/records.jsonl").exists()


def test_record_capture_requires_the_stable_record_id_field(tmp_path):
    invalid_schema = schema()
    invalid_table = CapturedTable(
        id="tbl_projects",
        name="Projects",
        fields=({"id": 6, "label": "Name", "fieldType": "text"},),
        artifacts=invalid_schema.tables[0].artifacts,
    )
    invalid_schema = CapturedSchema(
        app_id=invalid_schema.app_id,
        app_name=invalid_schema.app_name,
        tables=(invalid_table,),
        artifacts=invalid_schema.artifacts,
    )

    with pytest.raises(ValueError, match="Record ID# field 3"):
        capture_records(
            FakeRecordClient([]), invalid_schema, BackupWorkspace(tmp_path), page_size=1000
        )
