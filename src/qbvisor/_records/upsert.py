"""Validation and compatibility normalization for record upsert responses."""

from __future__ import annotations

from typing import Any

from ..exceptions import QuickbaseResponseError

UPSERT_PATH = "records"


def _response_error(expected: str, actual: Any) -> QuickbaseResponseError:
    return QuickbaseResponseError(
        "POST",
        UPSERT_PATH,
        expected=expected,
        actual=actual if isinstance(actual, str) else type(actual).__name__,
    )


def _record_ids(metadata: dict[str, Any], key: str) -> list[int]:
    values = metadata.get(key, [])
    if not isinstance(values, list) or not all(
        isinstance(value, int) and not isinstance(value, bool) and value > 0 for value in values
    ):
        raise _response_error(f"metadata.{key} array of positive record IDs", values)
    return values


def _line_errors(metadata: dict[str, Any], record_count: int) -> dict[str, list[str]]:
    errors = metadata.get("lineErrors", {})
    if not isinstance(errors, dict):
        raise _response_error("metadata.lineErrors object", errors)
    validated: dict[str, list[str]] = {}
    for position, messages in errors.items():
        try:
            line_number = int(position)
        except (TypeError, ValueError) as error:
            raise _response_error(
                "metadata.lineErrors keys containing one-based record positions",
                repr(position),
            ) from error
        if str(line_number) != position or not 1 <= line_number <= record_count:
            raise _response_error(
                "metadata.lineErrors keys containing one-based record positions",
                repr(position),
            )
        if (
            not isinstance(messages, list)
            or not messages
            or not all(isinstance(message, str) and message for message in messages)
        ):
            raise _response_error("non-empty error-message arrays in metadata.lineErrors", messages)
        validated[position] = messages
    return validated


def normalize_upsert_response(
    response: dict[str, Any],
    *,
    record_count: int,
) -> dict[str, Any]:
    """Validate a Quickbase upsert response and retain the legacy result keys."""
    metadata = response.get("metadata")
    if not isinstance(metadata, dict):
        raise _response_error("metadata object", metadata)

    total_processed = metadata.get("totalNumberOfRecordsProcessed")
    if (
        not isinstance(total_processed, int)
        or isinstance(total_processed, bool)
        or total_processed != record_count
    ):
        raise _response_error(
            f"metadata.totalNumberOfRecordsProcessed equal to submitted records ({record_count})",
            repr(total_processed),
        )

    data = response.get("data", [])
    if not isinstance(data, list) or not all(isinstance(record, dict) for record in data):
        raise _response_error("data array of record objects", data)

    created_ids = _record_ids(metadata, "createdRecordIds")
    updated_ids = _record_ids(metadata, "updatedRecordIds")
    unchanged_ids = _record_ids(metadata, "unchangedRecordIds")
    line_errors = _line_errors(metadata, record_count)

    result: dict[str, Any] = {
        "success": not line_errors,
        "createdRecordIds": created_ids,
        "updatedRecordIds": updated_ids,
        "unchangedRecordIds": unchanged_ids,
        "totalProcessed": total_processed,
        "data": data,
    }
    if line_errors:
        result.update(
            {
                "partial": True,
                "lineErrors": line_errors,
            }
        )
    return result
