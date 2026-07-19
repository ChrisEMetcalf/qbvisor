"""Stream complete table records into deterministic JSON Lines artifacts."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from .._records.pagination import (
    RECORD_ID_FIELD_ID,
    RecordQueryClient,
    iter_record_pages_by_id,
)
from .schema import CapturedSchema, CapturedTable
from .workspace import BackupWorkspace

RecordClient = RecordQueryClient


@dataclass(frozen=True, slots=True)
class CapturedRecordTable:
    id: str
    record_count: int
    artifact: str


@dataclass(frozen=True, slots=True)
class CapturedRecords:
    tables: tuple[CapturedRecordTable, ...]


def _field_ids(table: CapturedTable) -> tuple[int, ...]:
    field_ids: list[int] = []
    for field in table.fields:
        field_id = field.get("id")
        if not isinstance(field_id, int) or isinstance(field_id, bool):
            raise ValueError(f"Quickbase field in table {table.id} is missing a valid id")
        field_ids.append(field_id)
    if len(set(field_ids)) != len(field_ids):
        raise ValueError(f"Quickbase returned duplicate field IDs for table {table.id}")
    if RECORD_ID_FIELD_ID not in field_ids:
        raise ValueError(f"Quickbase table {table.id} does not include Record ID# field 3")
    return tuple(field_ids)


def _iter_table_records(
    client: RecordClient,
    table: CapturedTable,
    page_size: int,
) -> Iterator[dict[str, Any]]:
    select_fields = _field_ids(table)
    for page in iter_record_pages_by_id(
        client,
        table.id,
        select_fields=select_fields,
        page_size=page_size,
    ):
        yield from page


def capture_records(
    client: RecordClient,
    schema: CapturedSchema,
    workspace: BackupWorkspace,
    *,
    page_size: int,
) -> CapturedRecords:
    """Stream all fields and records for every captured table."""
    captured: list[CapturedRecordTable] = []
    for table in schema.tables:
        artifact = workspace.write_json_lines(
            f"tables/{table.id}/records.jsonl",
            "records",
            _iter_table_records(client, table, page_size),
        )
        captured.append(
            CapturedRecordTable(
                id=table.id,
                record_count=artifact.item_count or 0,
                artifact=artifact.path,
            )
        )
    return CapturedRecords(tables=tuple(captured))
