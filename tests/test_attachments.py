import asyncio
import base64
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, Mock, call

import pytest

import qbvisor.client as client_module
from qbvisor._attachments import LatestAttachment, latest_attachment
from qbvisor.client import QuickBaseClient
from qbvisor.exceptions import QuickbaseBatchError, QuickbaseResponseError, QuickbaseTimeoutError


class FakeAsyncTransport:
    def __init__(self, outcomes: dict[str, bytes | Exception]):
        self.outcomes = outcomes
        self.calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get_file(self, path: str) -> bytes:
        self.calls.append(path)
        outcome = self.outcomes[path]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture
def client() -> QuickBaseClient:
    instance = QuickBaseClient.__new__(QuickBaseClient)
    instance.transport = SimpleNamespace()
    instance.logger = Mock()
    return instance


def install_async_transport(monkeypatch, transport: FakeAsyncTransport) -> None:
    monkeypatch.setattr(client_module, "AsyncQuickBaseTransport", lambda _sync: transport)


def attachment_query_response(
    records: list[dict[str, Any]],
    *,
    field_ids: list[int],
    total_records: int,
) -> dict[str, Any]:
    return {
        "data": records,
        "fields": [{"id": field_id} for field_id in field_ids],
        "metadata": {
            "numFields": len(field_ids),
            "numRecords": len(records),
            "totalRecords": total_records,
        },
    }


def configure_attachment_client(
    client: QuickBaseClient,
    *,
    field_map: dict[str, dict[str, Any]],
    responses: list[dict[str, Any]],
) -> AsyncMock:
    client._ids = Mock(return_value=("app", "table"))
    client.meta = Mock()
    client.meta.get_field_map.return_value = field_map
    client.transport = SimpleNamespace(base_url="https://api.quickbase.com/v1")
    client._query_records_by_ids = Mock(side_effect=responses)
    downloader = AsyncMock(
        side_effect=lambda jobs, *_args, **_kwargs: [
            {"record_id": job["record_id"], "status": "downloaded"} for job in jobs
        ]
    )
    client._async_download_attachments = downloader
    return downloader


def test_latest_attachment_selects_highest_version_and_preserves_its_name():
    result = latest_attachment(
        {
            "fileName": "field-level.txt",
            "versions": [
                {"versionNumber": 3, "fileName": "current.txt"},
                {"versionNumber": 1, "fileName": "original.txt"},
            ],
        },
        table_id="table",
        record_id=101,
        field_id=8,
    )

    assert result == LatestAttachment(version_number=3, file_name="current.txt")


@pytest.mark.parametrize("value", [None, "", {}, {"versions": None}, {"versions": []}])
def test_latest_attachment_treats_documented_empty_cells_as_empty(value):
    assert latest_attachment(value, table_id="table", record_id=101, field_id=8) is None


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ([], "file attachment value object"),
        ({"versions": {}}, "file attachment versions array"),
        ({"versions": [{"versionNumber": 0}]}, "positive integer attachment versionNumber"),
        (
            {"versions": [{"versionNumber": 1}, {"versionNumber": 1}]},
            "unique attachment versionNumber values",
        ),
        (
            {"versions": [{"versionNumber": 1, "fileName": 42}]},
            "string attachment fileName",
        ),
    ],
)
def test_latest_attachment_rejects_malformed_metadata(value, message):
    with pytest.raises(QuickbaseResponseError, match=message):
        latest_attachment(value, table_id="table", record_id=101, field_id=8)


def test_latest_attachment_builds_stable_fallback_name():
    result = latest_attachment(
        {"versions": [{"versionNumber": 2}]},
        table_id="table",
        record_id=101,
        field_id=8,
    )

    assert result == LatestAttachment(version_number=2, file_name="fid8_v2.bin")


def test_single_field_discovery_continues_after_intelligent_short_page(client, tmp_path):
    where = "{6.EX.'active'}OR{7.EX.'priority'}"
    responses = [
        attachment_query_response(
            [
                {
                    "3": {"value": 10},
                    "8": {"value": {"versions": [{"versionNumber": 2, "fileName": "first.pdf"}]}},
                }
            ],
            field_ids=[3, 8],
            total_records=2,
        ),
        attachment_query_response(
            [
                {
                    "3": {"value": 20},
                    "8": {"value": {"versions": [{"versionNumber": 1, "fileName": "second.pdf"}]}},
                }
            ],
            field_ids=[3, 8],
            total_records=1,
        ),
    ]
    downloader = configure_attachment_client(
        client,
        field_map={"Attachment": {"id": 8, "type": "file"}},
        responses=responses,
    )

    results = client.download_attachments_async(
        "Sandbox",
        "Records",
        "Attachment",
        str(tmp_path),
        where=where,
        page_size=1000,
    )

    assert [result["record_id"] for result in results] == [10, 20]
    assert client._query_records_by_ids.call_args_list == [
        call(
            "table",
            select_fields=(3, 8),
            where=where,
            sort_by=((3, "ASC"),),
            top=1000,
        ),
        call(
            "table",
            select_fields=(3, 8),
            where=f"({where})AND{{3.GT.10}}",
            sort_by=((3, "ASC"),),
            top=1000,
        ),
    ]
    jobs = downloader.await_args.args[0]
    assert [job["url"] for job in jobs] == [
        "https://api.quickbase.com/v1/files/table/10/8/2",
        "https://api.quickbase.com/v1/files/table/20/8/1",
    ]


def test_whole_table_discovery_scans_every_page_and_file_field(client, tmp_path):
    responses = [
        attachment_query_response(
            [
                {
                    "3": {"value": 10},
                    "8": {"value": {"versions": []}},
                    "9": {"value": {"versions": [{"versionNumber": 1, "fileName": "photo.jpg"}]}},
                }
            ],
            field_ids=[3, 8, 9],
            total_records=2,
        ),
        attachment_query_response(
            [
                {
                    "3": {"value": 20},
                    "8": {"value": {"versions": [{"versionNumber": 3, "fileName": "invoice.pdf"}]}},
                    "9": {"value": None},
                }
            ],
            field_ids=[3, 8, 9],
            total_records=1,
        ),
    ]
    downloader = configure_attachment_client(
        client,
        field_map={
            "Record ID#": {"id": 3, "type": "recordid"},
            "Invoice": {"id": 8, "type": "file"},
            "Photo": {"id": 9, "type": "file"},
        },
        responses=responses,
    )

    results = client.download_table_attachments_async(
        "Sandbox", "Records", str(tmp_path), page_size=500
    )

    assert [result["record_id"] for result in results] == [10, 20]
    assert client._query_records_by_ids.call_count == 2
    jobs = downloader.await_args.args[0]
    assert [(job["record_id"], job["field_id"], job["file_name"]) for job in jobs] == [
        (10, 9, "photo.jpg"),
        (20, 8, "invoice.pdf"),
    ]
    assert all(job["include_field_id"] is True for job in jobs)


def test_batch_download_writes_documented_binary_response_exactly(client, monkeypatch, tmp_path):
    url = "https://api.quickbase.com/v1/files/table/101/8/1"
    transport = FakeAsyncTransport({url: b"SGVsbG8="})
    install_async_transport(monkeypatch, transport)

    results = asyncio.run(
        client._async_download_attachments(
            [{"record_id": 101, "file_name": "report.txt", "url": url}],
            str(tmp_path),
            max_concurrency=2,
        )
    )

    saved_path = tmp_path / "101_report.txt"
    assert saved_path.read_bytes() == b"SGVsbG8="
    assert results == [
        {
            "record_id": 101,
            "file_name": "report.txt",
            "saved_path": str(saved_path),
            "status": "downloaded",
            "bytes_written": 8,
        }
    ]


def test_batch_download_sanitizes_filename_and_skips_existing_file(client, monkeypatch, tmp_path):
    url = "https://api.quickbase.com/v1/files/table/101/8/1"
    existing_path = tmp_path / "101_....report.txt"
    existing_path.write_bytes(b"existing")
    transport = FakeAsyncTransport({url: b"replacement"})
    install_async_transport(monkeypatch, transport)

    results = asyncio.run(
        client._async_download_attachments(
            [{"record_id": 101, "file_name": "../../report?.txt", "url": url}],
            str(tmp_path),
        )
    )

    assert results[0]["status"] == "skipped"
    assert Path(results[0]["saved_path"]).parent == tmp_path
    assert existing_path.read_bytes() == b"existing"
    assert transport.calls == []


def test_batch_download_reports_partial_failure_without_discarding_success(
    client, monkeypatch, tmp_path
):
    good_url = "https://api.quickbase.com/v1/files/table/101/8/1"
    bad_url = "https://api.quickbase.com/v1/files/table/102/8/1"
    failure = QuickbaseTimeoutError("GET", "files/table/102/8/1", 5)
    transport = FakeAsyncTransport({good_url: b"complete", bad_url: failure})
    install_async_transport(monkeypatch, transport)

    with pytest.raises(QuickbaseBatchError) as caught:
        asyncio.run(
            client._async_download_attachments(
                [
                    {"record_id": 101, "file_name": "good.bin", "url": good_url},
                    {"record_id": 102, "file_name": "bad.bin", "url": bad_url},
                ],
                str(tmp_path),
            )
        )

    assert (tmp_path / "101_good.bin").read_bytes() == b"complete"
    assert not (tmp_path / "102_bad.bin").exists()
    assert [result["status"] for result in caught.value.results] == ["downloaded", "failed"]
    assert caught.value.errors == [failure]
    assert list(tmp_path.glob("*.part")) == []


def test_whole_table_download_paths_include_field_id_to_prevent_collisions(
    client, monkeypatch, tmp_path
):
    first_url = "https://api.quickbase.com/v1/files/table/101/8/1"
    second_url = "https://api.quickbase.com/v1/files/table/101/9/1"
    transport = FakeAsyncTransport({first_url: b"first", second_url: b"second"})
    install_async_transport(monkeypatch, transport)
    jobs: list[dict[str, Any]] = [
        {
            "record_id": 101,
            "field_id": 8,
            "include_field_id": True,
            "file_name": "report.txt",
            "url": first_url,
        },
        {
            "record_id": 101,
            "field_id": 9,
            "include_field_id": True,
            "file_name": "report.txt",
            "url": second_url,
        },
    ]

    results = asyncio.run(client._async_download_attachments(jobs, str(tmp_path)))

    assert (tmp_path / "101_8_report.txt").read_bytes() == b"first"
    assert (tmp_path / "101_9_report.txt").read_bytes() == b"second"
    assert len({result["saved_path"] for result in results}) == 2


def test_single_attachment_base64_encodes_raw_binary_response(client):
    client._ids = Mock(return_value=("app", "table"))
    client.meta = Mock()
    client.meta.get_field_map.return_value = {"Attachment": {"id": 8, "type": "file"}}
    client._request = Mock(
        return_value={
            "data": [
                {"8": {"value": {"versions": [{"versionNumber": 2, "fileName": "payload.bin"}]}}}
            ]
        }
    )
    client.transport = Mock()
    client.transport.get_file.return_value = b"\xff\x00binary"

    result = client.download_attachment_base64("Sandbox", "Records", 101, "Attachment")

    assert result == base64.b64encode(b"\xff\x00binary").decode("ascii")
    client.transport.get_file.assert_called_once_with("files/table/101/8/2")


def test_single_attachment_returns_none_only_when_no_version_exists(client):
    client._ids = Mock(return_value=("app", "table"))
    client.meta = Mock()
    client.meta.get_field_map.return_value = {"Attachment": {"id": 8, "type": "file"}}
    client._request = Mock(return_value={"data": [{"8": {"value": {"versions": []}}}]})
    client.transport = Mock()

    result = client.download_attachment_base64("Sandbox", "Records", 101, "Attachment")

    assert result is None
    client.transport.get_file.assert_not_called()
