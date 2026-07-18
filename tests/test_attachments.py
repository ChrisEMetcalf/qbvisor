import asyncio
import base64
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

import qbvisor.client as client_module
from qbvisor.client import QuickBaseClient
from qbvisor.exceptions import QuickbaseBatchError, QuickbaseTimeoutError


class FakeAsyncTransport:
    def __init__(self, outcomes: dict[str, bytes | Exception]):
        self.outcomes = outcomes
        self.calls: list[str] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def get_bytes(self, path: str) -> bytes:
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
    client.transport.get_bytes.return_value = b"\xff\x00binary"

    result = client.download_attachment_base64("Sandbox", "Records", 101, "Attachment")

    assert result == base64.b64encode(b"\xff\x00binary").decode("ascii")
    client.transport.get_bytes.assert_called_once_with("files/table/101/8/2")


def test_single_attachment_returns_none_only_when_no_version_exists(client):
    client._ids = Mock(return_value=("app", "table"))
    client.meta = Mock()
    client.meta.get_field_map.return_value = {"Attachment": {"id": 8, "type": "file"}}
    client._request = Mock(return_value={"data": [{"8": {"value": {"versions": []}}}]})
    client.transport = Mock()

    result = client.download_attachment_base64("Sandbox", "Records", 101, "Attachment")

    assert result is None
    client.transport.get_bytes.assert_not_called()
