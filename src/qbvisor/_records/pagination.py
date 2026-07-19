"""Validated keyset pagination for complete Quickbase record scans."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any, Protocol

from .._pagination import required_count
from ..exceptions import QuickbaseResponseError

RECORD_ID_FIELD_ID = 3


class RecordQueryClient(Protocol):
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
    num_fields = required_count(metadata, "numFields", path="records/query")
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


def _field_ids(select_fields: Sequence[int]) -> tuple[int, ...]:
    field_ids = tuple(select_fields)
    if not field_ids or not all(
        isinstance(field_id, int) and not isinstance(field_id, bool) and field_id > 0
        for field_id in field_ids
    ):
        raise ValueError("select_fields must contain positive integer field IDs")
    if len(set(field_ids)) != len(field_ids):
        raise ValueError("select_fields cannot contain duplicate field IDs")
    if RECORD_ID_FIELD_ID not in field_ids:
        raise ValueError("select_fields must include Record ID# field 3")
    return field_ids


def _page_where(where: str | None, last_record_id: int) -> str | None:
    if not last_record_id:
        return where or None
    cursor = f"{{{RECORD_ID_FIELD_ID}.GT.{last_record_id}}}"
    return f"({where})AND{cursor}" if where else cursor


def iter_record_pages_by_id(
    client: RecordQueryClient,
    table_id: str,
    *,
    select_fields: Sequence[int],
    where: str | None = None,
    page_size: int = 1000,
    record_limit: int | None = None,
) -> Iterator[tuple[dict[str, Any], ...]]:
    """Yield complete record pages using the immutable Record ID# field as a cursor."""
    field_ids = _field_ids(select_fields)
    if not isinstance(page_size, int) or isinstance(page_size, bool) or not 1 <= page_size <= 1000:
        raise ValueError("page_size must be an integer between 1 and 1000")
    if record_limit is not None and (
        not isinstance(record_limit, int) or isinstance(record_limit, bool) or record_limit < 0
    ):
        raise ValueError("record_limit must be a non-negative integer or None")
    if record_limit == 0:
        return

    last_record_id = 0
    yielded_records = 0
    while record_limit is None or yielded_records < record_limit:
        request_size = page_size
        if record_limit is not None:
            request_size = min(page_size, record_limit - yielded_records)
        response = client._query_records_by_ids(
            table_id,
            select_fields=field_ids,
            where=_page_where(where, last_record_id),
            sort_by=((RECORD_ID_FIELD_ID, "ASC"),),
            top=request_size,
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
        _validate_response_fields(response, metadata, field_ids)
        num_records = required_count(metadata, "numRecords", path="records/query")
        total_records = required_count(metadata, "totalRecords", path="records/query")
        if num_records != len(data) or num_records > total_records or num_records > request_size:
            raise QuickbaseResponseError(
                "POST",
                "records/query",
                expected="pagination metadata matching returned records",
                actual=(
                    f"numRecords={num_records}, totalRecords={total_records}, "
                    f"requested={request_size}, data={len(data)}"
                ),
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

        page: list[dict[str, Any]] = []
        for record in data:
            record_id = _record_id(record, table_id)
            if record_id <= last_record_id:
                raise QuickbaseResponseError(
                    "POST",
                    "records/query",
                    expected="strictly increasing Record ID# values",
                    actual=str(record_id),
                )
            last_record_id = record_id
            page.append(record)

        yielded_records += len(page)
        yield tuple(page)
        if num_records == total_records:
            return
