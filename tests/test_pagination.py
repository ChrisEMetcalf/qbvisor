from typing import Any

import pytest

from qbvisor._pagination import iter_intelligent_pages
from qbvisor.exceptions import QuickbaseResponseError


def response(
    record_ids: list[int],
    *,
    skip: int,
    total_records: int,
    fields: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    page_fields = fields or [{"id": 3, "label": "Record ID#", "type": "recordid"}]
    return {
        "data": [{"3": {"value": record_id}} for record_id in record_ids],
        "fields": page_fields,
        "metadata": {
            "numFields": len(page_fields),
            "numRecords": len(record_ids),
            "totalRecords": total_records,
            "skip": skip,
        },
    }


def test_native_pages_continue_from_the_returned_record_count():
    pages = iter(
        [
            response(list(range(1, 893)), skip=0, total_records=1201),
            response(list(range(893, 1202)), skip=892, total_records=1201),
        ]
    )
    calls: list[tuple[int, int | None]] = []

    def fetch_page(skip: int, top: int | None) -> dict[str, Any]:
        calls.append((skip, top))
        return next(pages)

    results = list(iter_intelligent_pages(fetch_page, path="reports/1/run"))

    assert [len(page["data"]) for page in results] == [892, 309]
    assert calls == [(0, None), (892, None)]


def test_explicit_top_is_a_total_limit_across_short_pages():
    pages = iter(
        [
            response(list(range(1, 601)), skip=0, total_records=1201),
            response(list(range(601, 1001)), skip=600, total_records=1201),
        ]
    )
    calls: list[tuple[int, int | None]] = []

    def fetch_page(skip: int, top: int | None) -> dict[str, Any]:
        calls.append((skip, top))
        return next(pages)

    results = list(iter_intelligent_pages(fetch_page, path="reports/1/run", top=1000))

    assert [len(page["data"]) for page in results] == [600, 400]
    assert calls == [(0, 1000), (600, 400)]


def test_explicit_skip_reads_only_the_remaining_result_range():
    calls: list[tuple[int, int | None]] = []

    def fetch_page(skip: int, top: int | None) -> dict[str, Any]:
        calls.append((skip, top))
        return response(list(range(201, 501)), skip=200, total_records=500)

    results = list(iter_intelligent_pages(fetch_page, path="reports/1/run", skip=200))

    assert len(results[0]["data"]) == 300
    assert calls == [(200, None)]


def test_skip_beyond_the_result_set_returns_the_empty_page():
    empty = response([], skip=600, total_records=500)

    results = list(
        iter_intelligent_pages(
            lambda _skip, _top: empty,
            path="reports/1/run",
            skip=600,
        )
    )

    assert results == [empty]


def test_empty_page_raises_when_metadata_says_records_remain():
    def fetch_page(skip: int, top: int | None) -> dict[str, Any]:
        return response([], skip=skip, total_records=1)

    with pytest.raises(QuickbaseResponseError, match="non-empty page"):
        list(iter_intelligent_pages(fetch_page, path="reports/1/run"))


def test_fields_must_remain_stable_across_pages():
    pages = iter(
        [
            response([1], skip=0, total_records=2),
            response(
                [2],
                skip=1,
                total_records=2,
                fields=[{"id": 6, "label": "Name", "type": "text"}],
            ),
        ]
    )

    with pytest.raises(QuickbaseResponseError, match="stable field metadata"):
        list(iter_intelligent_pages(lambda _skip, _top: next(pages), path="reports/1/run"))


@pytest.mark.parametrize(
    ("metadata_update", "message"),
    [
        ({"skip": 1}, "metadata.skip"),
        ({"numFields": 2}, "numFields"),
        ({"numRecords": 2}, "pagination metadata"),
        ({"totalRecords": "1"}, "metadata.totalRecords"),
    ],
)
def test_invalid_pagination_metadata_is_rejected(metadata_update, message):
    invalid = response([1], skip=0, total_records=1)
    invalid["metadata"].update(metadata_update)

    with pytest.raises(QuickbaseResponseError, match=message):
        list(iter_intelligent_pages(lambda _skip, _top: invalid, path="records/query"))
