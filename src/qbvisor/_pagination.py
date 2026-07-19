"""Validated pagination primitives for Quickbase responses."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from typing import Any

from .exceptions import QuickbaseResponseError

PageFetcher = Callable[[int, int | None], dict[str, Any]]


def required_count(metadata: dict[str, Any], key: str, *, path: str) -> int:
    """Return a required non-negative integer from response metadata."""
    value = metadata.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise QuickbaseResponseError(
            "POST",
            path,
            expected=f"non-negative integer metadata.{key}",
            actual=type(value).__name__,
        )
    return value


def _page_parts(
    response: dict[str, Any],
    *,
    path: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    data = response.get("data")
    fields = response.get("fields")
    metadata = response.get("metadata")
    if not isinstance(data, list) or not all(isinstance(record, dict) for record in data):
        raise QuickbaseResponseError(
            "POST",
            path,
            expected="data array of record objects",
            actual=type(data).__name__,
        )
    if not isinstance(fields, list) or not all(isinstance(field, dict) for field in fields):
        raise QuickbaseResponseError(
            "POST",
            path,
            expected="fields array of field objects",
            actual=type(fields).__name__,
        )
    if not isinstance(metadata, dict):
        raise QuickbaseResponseError(
            "POST",
            path,
            expected="metadata object",
            actual=type(metadata).__name__,
        )
    return data, fields, metadata


def _field_signature(fields: list[dict[str, Any]]) -> tuple[tuple[Any, ...], ...]:
    return tuple(
        (
            field.get("id"),
            field.get("label"),
            field.get("labelOverride"),
            field.get("type"),
        )
        for field in fields
    )


def iter_intelligent_pages(
    fetch_page: PageFetcher,
    *,
    path: str,
    skip: int = 0,
    top: int | None = None,
) -> Iterator[dict[str, Any]]:
    """Yield every response needed to satisfy one intelligent-pagination request."""
    if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
        raise ValueError("skip must be a non-negative integer")
    if top is not None and (not isinstance(top, int) or isinstance(top, bool) or top < 0):
        raise ValueError("top must be a non-negative integer or None")

    requested_skip = skip
    requested_top = top
    target_end: int | None = None
    expected_fields: tuple[tuple[Any, ...], ...] | None = None

    while True:
        response = fetch_page(requested_skip, requested_top)
        data, fields, metadata = _page_parts(response, path=path)
        num_fields = required_count(metadata, "numFields", path=path)
        num_records = required_count(metadata, "numRecords", path=path)
        total_records = required_count(metadata, "totalRecords", path=path)
        response_skip = metadata.get("skip", requested_skip)

        if (
            not isinstance(response_skip, int)
            or isinstance(response_skip, bool)
            or response_skip != requested_skip
        ):
            raise QuickbaseResponseError(
                "POST",
                path,
                expected=f"metadata.skip equal to requested skip {requested_skip}",
                actual=repr(response_skip),
            )
        if num_fields != len(fields):
            raise QuickbaseResponseError(
                "POST",
                path,
                expected="metadata.numFields matching returned fields",
                actual=f"numFields={num_fields}, fields={len(fields)}",
            )
        remaining_records = max(total_records - response_skip, 0)
        if num_records != len(data) or num_records > remaining_records:
            raise QuickbaseResponseError(
                "POST",
                path,
                expected="pagination metadata matching returned records",
                actual=(
                    f"skip={response_skip}, numRecords={num_records}, "
                    f"totalRecords={total_records}, data={len(data)}"
                ),
            )
        if requested_top is not None and requested_top > 0 and num_records > requested_top:
            raise QuickbaseResponseError(
                "POST",
                path,
                expected="no more records than the requested top",
                actual=f"requested={requested_top}, numRecords={num_records}",
            )

        field_signature = _field_signature(fields)
        if expected_fields is None:
            expected_fields = field_signature
        elif field_signature != expected_fields:
            raise QuickbaseResponseError(
                "POST",
                path,
                expected="stable field metadata across pages",
                actual=f"expected={expected_fields}, returned={field_signature}",
            )

        if target_end is None:
            target_end = total_records
            if top is not None and top > 0:
                target_end = min(target_end, skip + top)
        else:
            target_end = min(target_end, total_records)

        yield response

        next_skip = response_skip + num_records
        if next_skip >= target_end:
            return
        if not num_records:
            raise QuickbaseResponseError(
                "POST",
                path,
                expected="a non-empty page while records remain",
                actual=f"skip={response_skip}, totalRecords={total_records}",
            )

        requested_skip = next_skip
        requested_top = None if top is None or top == 0 else target_end - next_skip
