"""Validation and compatibility normalization for record upsert responses."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import requests

from ..exceptions import QuickbaseBatchError, QuickbaseHTTPError, QuickbaseResponseError

UPSERT_PATH = "records"
MAX_UPSERT_PAYLOAD_BYTES = 40_000_000


@dataclass(frozen=True, slots=True)
class UpsertBatch:
    """One preflighted, one-based input range for a Quickbase upsert request."""

    start_line: int
    records: tuple[dict[str, Any], ...]
    payload_bytes: int

    @property
    def end_line(self) -> int:
        return self.start_line + len(self.records) - 1

    def json_body(self, request_template: dict[str, Any]) -> dict[str, Any]:
        body = dict(request_template)
        body["data"] = list(self.records)
        return body


def _json_payload_size(value: Any) -> int:
    """Return the exact body size Requests will produce for a JSON value."""
    prepared = requests.Request(
        "POST",
        "https://api.quickbase.com/v1/records",
        json=value,
    ).prepare()
    body = prepared.body
    if isinstance(body, str):
        body = body.encode("utf-8")
    if not isinstance(body, bytes):
        raise TypeError("Requests did not produce a JSON byte payload")
    return len(body)


def _make_batch(
    request_template: dict[str, Any],
    records: list[dict[str, Any]],
    *,
    start_line: int,
    expected_size: int,
) -> UpsertBatch:
    batch = UpsertBatch(start_line, tuple(records), expected_size)
    actual_size = _json_payload_size(batch.json_body(request_template))
    if actual_size != expected_size:
        raise AssertionError(
            f"Upsert payload size calculation drifted: expected {expected_size}, got {actual_size}"
        )
    return batch


def plan_upsert_batches(
    records: list[dict[str, Any]],
    *,
    request_template: dict[str, Any],
    max_payload_bytes: int = MAX_UPSERT_PAYLOAD_BYTES,
) -> tuple[UpsertBatch, ...]:
    """Preflight sequential requests whose serialized JSON fits Quickbase's payload limit."""
    if not isinstance(max_payload_bytes, int) or isinstance(max_payload_bytes, bool):
        raise ValueError("max_payload_bytes must be a positive integer")
    if max_payload_bytes < 1:
        raise ValueError("max_payload_bytes must be a positive integer")
    if "data" in request_template:
        raise ValueError("request_template cannot contain data")

    try:
        empty_size = _json_payload_size({**request_template, "data": []})
    except (TypeError, requests.exceptions.InvalidJSONError) as error:
        raise ValueError("Upsert request options cannot be serialized as JSON") from error
    if empty_size > max_payload_bytes:
        raise ValueError(
            f"Upsert request options require {empty_size} bytes, exceeding "
            f"max_payload_bytes={max_payload_bytes}"
        )
    if not records:
        return (
            _make_batch(
                request_template,
                [],
                start_line=1,
                expected_size=empty_size,
            ),
        )

    batches: list[UpsertBatch] = []
    current_records: list[dict[str, Any]] = []
    current_size = empty_size
    start_line = 1
    for line_number, record in enumerate(records, start=1):
        try:
            record_size = _json_payload_size(record)
        except (TypeError, requests.exceptions.InvalidJSONError) as error:
            raise ValueError(
                f"Upsert record at position {line_number} cannot be serialized as JSON"
            ) from error

        single_record_size = empty_size + record_size
        if single_record_size > max_payload_bytes:
            raise ValueError(
                f"Upsert record at position {line_number} requires {single_record_size} bytes, "
                f"exceeding max_payload_bytes={max_payload_bytes}"
            )

        separator_size = 2 if current_records else 0
        candidate_size = current_size + separator_size + record_size
        if current_records and candidate_size > max_payload_bytes:
            batches.append(
                _make_batch(
                    request_template,
                    current_records,
                    start_line=start_line,
                    expected_size=current_size,
                )
            )
            current_records = []
            current_size = empty_size
            start_line = line_number
            candidate_size = current_size + record_size

        current_records.append(record)
        current_size = candidate_size

    batches.append(
        _make_batch(
            request_template,
            current_records,
            start_line=start_line,
            expected_size=current_size,
        )
    )
    return tuple(batches)


def aggregate_upsert_results(
    completed: Sequence[tuple[UpsertBatch, dict[str, Any]]],
) -> dict[str, Any]:
    """Combine validated batch results and restore original one-based line positions."""
    created_ids: list[int] = []
    updated_ids: list[int] = []
    unchanged_ids: list[int] = []
    data: list[dict[str, Any]] = []
    line_errors: dict[str, list[str]] = {}
    total_processed = 0

    for batch, result in completed:
        created_ids.extend(result["createdRecordIds"])
        updated_ids.extend(result["updatedRecordIds"])
        unchanged_ids.extend(result["unchangedRecordIds"])
        data.extend(result["data"])
        total_processed += result["totalProcessed"]
        for local_position, messages in result.get("lineErrors", {}).items():
            global_position = batch.start_line + int(local_position) - 1
            line_errors[str(global_position)] = messages

    aggregate: dict[str, Any] = {
        "success": not line_errors,
        "createdRecordIds": created_ids,
        "updatedRecordIds": updated_ids,
        "unchangedRecordIds": unchanged_ids,
        "totalProcessed": total_processed,
        "data": data,
    }
    if line_errors:
        aggregate.update({"partial": True, "lineErrors": line_errors})
    return aggregate


def _completed_outcome(
    batch_number: int,
    batch: UpsertBatch,
    result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "batchNumber": batch_number,
        "startLine": batch.start_line,
        "endLine": batch.end_line,
        "payloadBytes": batch.payload_bytes,
        "status": "completed",
        "result": result,
    }


def _failure_outcome(
    batch_number: int,
    batch: UpsertBatch,
    error: Exception,
) -> dict[str, Any]:
    status = (
        "failed"
        if isinstance(error, QuickbaseHTTPError) and error.status_code < 500
        else "uncertain"
    )
    return {
        "batchNumber": batch_number,
        "startLine": batch.start_line,
        "endLine": batch.end_line,
        "payloadBytes": batch.payload_bytes,
        "status": status,
        "error": str(error),
    }


def execute_upsert_batches(
    batches: Sequence[UpsertBatch],
    *,
    request_template: dict[str, Any],
    send: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Execute a preflighted plan sequentially and preserve prior committed outcomes."""
    completed: list[tuple[UpsertBatch, dict[str, Any]]] = []
    outcomes: list[dict[str, Any]] = []
    for batch_number, batch in enumerate(batches, start=1):
        try:
            response = send(batch.json_body(request_template))
            result = normalize_upsert_response(response, record_count=len(batch.records))
        except Exception as error:
            if not completed:
                raise
            outcomes.append(_failure_outcome(batch_number, batch, error))
            raise QuickbaseBatchError("Record upsert", outcomes, [error]) from error
        completed.append((batch, result))
        outcomes.append(_completed_outcome(batch_number, batch, result))
    return aggregate_upsert_results(completed)


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
