from datetime import datetime

import pytest

from qbvisor._records.upsert import (
    _json_payload_size,
    execute_upsert_batches,
    plan_upsert_batches,
)
from qbvisor.exceptions import QuickbaseBatchError, QuickbaseHTTPError


def test_batch_plan_uses_exact_requests_size_for_unicode_records():
    template = {"to": "table", "mergeFieldId": 6, "fieldsToReturn": [3, 7]}
    records = [
        {"6": {"value": "café"}, "7": {"value": "Ready"}},
        {"6": {"value": "launch 🚀"}, "7": {"value": "Running"}},
    ]
    combined_size = _json_payload_size({**template, "data": records})

    combined = plan_upsert_batches(
        records,
        request_template=template,
        max_payload_bytes=combined_size,
    )
    split = plan_upsert_batches(
        records,
        request_template=template,
        max_payload_bytes=combined_size - 1,
    )

    assert len(combined) == 1
    assert combined[0].payload_bytes == combined_size
    assert combined[0].json_body(template) == {**template, "data": records}
    assert [(batch.start_line, batch.end_line) for batch in split] == [(1, 1), (2, 2)]
    assert all(
        batch.payload_bytes == _json_payload_size(batch.json_body(template)) for batch in split
    )


def test_batch_plan_rejects_a_later_oversized_record():
    template = {"to": "table"}
    records = [
        {"6": {"value": "small"}},
        {"6": {"value": "x" * 100}},
    ]
    maximum = _json_payload_size({**template, "data": [records[1]]}) - 1

    with pytest.raises(ValueError, match="record at position 2 requires"):
        plan_upsert_batches(
            records,
            request_template=template,
            max_payload_bytes=maximum,
        )


def test_batch_plan_rejects_non_json_data_before_returning_batches():
    with pytest.raises(ValueError, match="record at position 2 cannot be serialized"):
        plan_upsert_batches(
            [
                {"6": {"value": "valid"}},
                {"6": {"value": datetime(2026, 7, 19)}},
            ],
            request_template={"to": "table"},
        )


def test_batch_plan_preserves_an_empty_compatibility_request():
    template = {"to": "table", "fieldsToReturn": [3]}

    batches = plan_upsert_batches([], request_template=template)

    assert len(batches) == 1
    assert batches[0].start_line == 1
    assert batches[0].end_line == 0
    assert batches[0].json_body(template) == {**template, "data": []}


@pytest.mark.parametrize("maximum", [0, -1, True, 1.5])
def test_batch_plan_requires_a_positive_integer_limit(maximum):
    with pytest.raises(ValueError, match="positive integer"):
        plan_upsert_batches(
            [],
            request_template={"to": "table"},
            max_payload_bytes=maximum,
        )


def test_batch_plan_rejects_data_in_the_request_template():
    with pytest.raises(ValueError, match="cannot contain data"):
        plan_upsert_batches([], request_template={"to": "table", "data": []})


def test_later_definitive_http_failure_retains_completed_batch_range():
    template = {"to": "table"}
    records = [
        {"6": {"value": "First"}},
        {"6": {"value": "Second"}},
    ]
    maximum = max(_json_payload_size({**template, "data": [record]}) for record in records)
    batches = plan_upsert_batches(
        records,
        request_template=template,
        max_payload_bytes=maximum,
    )
    failure = QuickbaseHTTPError(
        method="POST",
        path="records",
        status_code=400,
        message="Bad request",
    )
    responses: list[dict | Exception] = [
        {
            "metadata": {
                "createdRecordIds": [101],
                "totalNumberOfRecordsProcessed": 1,
            }
        },
        failure,
    ]

    def send(_body):
        response = responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    with pytest.raises(QuickbaseBatchError) as caught:
        execute_upsert_batches(batches, request_template=template, send=send)

    assert caught.value.errors == [failure]
    assert caught.value.results[0]["status"] == "completed"
    assert caught.value.results[0]["startLine"] == 1
    assert caught.value.results[0]["endLine"] == 1
    assert caught.value.results[1]["status"] == "failed"
    assert caught.value.results[1]["startLine"] == 2
    assert caught.value.results[1]["endLine"] == 2
