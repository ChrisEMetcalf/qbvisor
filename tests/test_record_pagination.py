from collections.abc import Sequence
from typing import Any

from qbvisor._records.pagination import iter_record_pages_by_id


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
        top: int | None = 1000,
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


def response(records: list[dict[str, Any]], total_records: int) -> dict[str, Any]:
    return {
        "data": records,
        "fields": [{"id": 3}, {"id": 6}],
        "metadata": {
            "numFields": 2,
            "numRecords": len(records),
            "totalRecords": total_records,
        },
    }


def test_keyset_pages_combine_filters_and_honor_the_exact_record_limit():
    client = FakeRecordClient(
        [
            response([record(10, "Alpha"), record(20, "Beta")], total_records=5),
            response([record(30, "Gamma")], total_records=3),
        ]
    )

    pages = list(
        iter_record_pages_by_id(
            client,
            "tbl_projects",
            select_fields=(3, 6),
            where="{6.EX.'Alpha'}OR{6.EX.'Beta'}",
            page_size=2,
            record_limit=3,
        )
    )

    assert pages == [
        (record(10, "Alpha"), record(20, "Beta")),
        (record(30, "Gamma"),),
    ]
    assert client.calls == [
        {
            "table_id": "tbl_projects",
            "select_fields": (3, 6),
            "where": "{6.EX.'Alpha'}OR{6.EX.'Beta'}",
            "sort_by": ((3, "ASC"),),
            "group_by": None,
            "skip": 0,
            "top": 2,
        },
        {
            "table_id": "tbl_projects",
            "select_fields": (3, 6),
            "where": "({6.EX.'Alpha'}OR{6.EX.'Beta'})AND{3.GT.20}",
            "sort_by": ((3, "ASC"),),
            "group_by": None,
            "skip": 0,
            "top": 1,
        },
    ]


def test_zero_record_limit_does_not_query_quickbase():
    client = FakeRecordClient([])

    assert (
        list(
            iter_record_pages_by_id(
                client,
                "tbl_projects",
                select_fields=(3, 6),
                record_limit=0,
            )
        )
        == []
    )
    assert client.calls == []


def test_native_page_size_omits_top_and_continues_after_a_short_response():
    client = FakeRecordClient(
        [
            response([record(10, "Alpha"), record(20, "Beta")], total_records=3),
            response([record(30, "Gamma")], total_records=1),
        ]
    )

    pages = list(
        iter_record_pages_by_id(
            client,
            "tbl_projects",
            select_fields=(3, 6),
            page_size=None,
        )
    )

    assert [len(page) for page in pages] == [2, 1]
    assert [request["top"] for request in client.calls] == [None, None]
