"""Stream complete table records into deterministic JSON Lines artifacts."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from ..exceptions import QuickbaseResponseError
from .schema import CapturedSchema, CapturedTable
from .workspace import BackupWorkspace

RECORD_ID_FIELD_ID = 3


class RecordClient(Protocol):
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
    ) -> dict[str, Any]: ...


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


def _required_count(metadata: dict[str, Any], key: str) -> int:
    value = metadata.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected=f"non-negative integer metadata.{key}",
            actual=type(value).__name__,
        )
    return value


def _record_id(record: dict[str, Any], table_id: str) -> int:
    cell = record.get(str(RECORD_ID_FIELD_ID))
    value = cell.get("value") if isinstance(cell, dict) else None
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected=f"positive Record ID# value for table {table_id}",
            actual=type(value).__name__,
        )
    return value


def _validate_response_fields(
    response: dict[str, Any],
    metadata: dict[str, Any],
    expected_field_ids: tuple[int, ...],
) -> None:
    fields = response.get("fields")
    if not isinstance(fields, list) or not all(isinstance(field, dict) for field in fields):
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected="fields array of field objects",
            actual=type(fields).__name__,
        )
    response_field_ids = [field.get("id") for field in fields]
    num_fields = _required_count(metadata, "numFields")
    if (
        num_fields != len(fields)
        or not all(
            isinstance(field_id, int) and not isinstance(field_id, bool)
            for field_id in response_field_ids
        )
        or len(set(response_field_ids)) != len(response_field_ids)
        or set(response_field_ids) != set(expected_field_ids)
    ):
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected="all requested field IDs exactly once",
            actual=f"requested={list(expected_field_ids)}, returned={response_field_ids}",
        )


def _iter_table_records(
    client: RecordClient,
    table: CapturedTable,
    page_size: int,
) -> Iterator[dict[str, Any]]:
    select_fields = _field_ids(table)
    last_record_id = 0
    while True:
        where = f"{{{RECORD_ID_FIELD_ID}.GT.{last_record_id}}}" if last_record_id else None
        response = client._query_records_by_ids(
            table.id,
            select_fields=select_fields,
            where=where,
            sort_by=((RECORD_ID_FIELD_ID, "ASC"),),
            top=page_size,
        )
        data = response.get("data")
        metadata = response.get("metadata")
        if not isinstance(data, list) or not all(isinstance(record, dict) for record in data):
            raise QuickbaseResponseError(
                "POST",
                "records/query",
                expected="data array of record objects",
                actual=type(data).__name__,
            )
        if not isinstance(metadata, dict):
            raise QuickbaseResponseError(
                "POST",
                "records/query",
                expected="metadata object",
                actual=type(metadata).__name__,
            )
        _validate_response_fields(response, metadata, select_fields)
        num_records = _required_count(metadata, "numRecords")
        total_records = _required_count(metadata, "totalRecords")
        if num_records != len(data) or num_records > total_records:
            raise QuickbaseResponseError(
                "POST",
                "records/query",
                expected="pagination metadata matching returned records",
                actual=f"numRecords={num_records}, totalRecords={total_records}, data={len(data)}",
            )
        if not data:
            if total_records:
                raise QuickbaseResponseError(
                    "POST",
                    "records/query",
                    expected="a non-empty page while records remain",
                    actual=f"totalRecords={total_records}",
                )
            return

        for record in data:
            record_id = _record_id(record, table.id)
            if record_id <= last_record_id:
                raise QuickbaseResponseError(
                    "POST",
                    "records/query",
                    expected="strictly increasing Record ID# values",
                    actual=str(record_id),
                )
            last_record_id = record_id
            yield record

        if num_records == total_records:
            return


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
