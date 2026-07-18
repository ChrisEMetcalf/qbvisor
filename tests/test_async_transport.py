import asyncio
import json
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import aiohttp
import pytest
import requests

from qbvisor.async_transport import AsyncQuickBaseTransport
from qbvisor.exceptions import (
    QuickbaseRateLimitError,
    QuickbaseResponseError,
    QuickbaseTimeoutError,
)
from qbvisor.transport import QuickBaseTransport, RetryPolicy


class FakeResponse:
    def __init__(
        self,
        status: int,
        payload: object | None = None,
        *,
        headers: dict[str, str] | None = None,
        content: bytes | None = None,
    ):
        self.status = status
        self.headers = headers or {}
        self._content = (
            content
            if content is not None
            else (json.dumps(payload).encode() if payload is not None else b"")
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def read(self) -> bytes:
        return self._content


class FakeSession:
    def __init__(self, *outcomes: FakeResponse | Exception):
        self.outcomes = list(outcomes)
        self.requests: list[tuple[str, str, dict[str, Any]]] = []
        self.closed = False

    def request(self, method: str, url: str, **kwargs: Any) -> FakeResponse:
        self.requests.append((method, url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def close(self) -> None:
        self.closed = True


def async_transport(
    session: FakeSession,
    *,
    max_attempts: int = 5,
    sleep: AsyncMock | None = None,
) -> AsyncQuickBaseTransport:
    sync_transport = QuickBaseTransport(
        realm_hostname="example.quickbase.com",
        auth_token="secret-token",
        session=Mock(spec=requests.Session),
        max_attempts=max_attempts,
        jitter=lambda _low, _high: 1.25,
    )
    return AsyncQuickBaseTransport(
        sync_transport,
        session=cast(aiohttp.ClientSession, session),
        sleep=sleep or AsyncMock(),
    )


def test_post_json_reuses_sync_configuration_and_returns_documented_shape():
    session = FakeSession(FakeResponse(200, {"data": []}))
    transport = async_transport(session)

    result = asyncio.run(
        transport.post_json(
            "records/query",
            json_body={"from": "table"},
            headers={"Accept-Encoding": "gzip"},
            retry_policy=RetryPolicy.SAFE,
        )
    )

    assert result == {"data": []}
    method, url, kwargs = session.requests[0]
    assert method == "POST"
    assert url == "https://api.quickbase.com/v1/records/query"
    assert kwargs["headers"]["Authorization"] == "secret-token"
    assert kwargs["headers"]["Accept-Encoding"] == "gzip"
    assert kwargs["timeout"].connect == 10.0
    assert kwargs["timeout"].sock_read == 120.0


def test_safe_async_request_uses_shared_retry_after_policy():
    session = FakeSession(
        FakeResponse(503, headers={"Retry-After": "2", "qb-api-ray": "ray-retry"}),
        FakeResponse(200, {"ok": True}),
    )
    sleep = AsyncMock()
    transport = async_transport(session, sleep=sleep)

    result = asyncio.run(
        transport.post_json(
            "records/query",
            json_body={"from": "table"},
            retry_policy=RetryPolicy.SAFE,
        )
    )

    assert result == {"ok": True}
    assert len(session.requests) == 2
    sleep.assert_awaited_once_with(2.0)


def test_async_rate_limit_exposes_quickbase_diagnostics():
    session = FakeSession(
        FakeResponse(
            429,
            {"message": "Too many requests", "description": "Wait for Quickbase"},
            headers={"qb-api-ray": "ray-final"},
        )
    )
    transport = async_transport(session)

    with pytest.raises(QuickbaseRateLimitError) as caught:
        asyncio.run(transport.post_json("records/query", retry_policy=RetryPolicy.SAFE))

    assert caught.value.message == "Too many requests"
    assert caught.value.description == "Wait for Quickbase"
    assert caught.value.qb_api_ray == "ray-final"
    assert "secret-token" not in str(caught.value)


def test_safe_async_timeout_reports_exhausted_attempts():
    session = FakeSession(TimeoutError(), TimeoutError())
    transport = async_transport(session, max_attempts=2)

    with pytest.raises(QuickbaseTimeoutError) as caught:
        asyncio.run(transport.post_json("records/query", retry_policy=RetryPolicy.SAFE))

    assert caught.value.attempts == 2
    assert len(session.requests) == 2


def test_async_binary_response_is_returned_without_json_decoding():
    session = FakeSession(FakeResponse(200, content=b"raw-file-bytes"))
    transport = async_transport(session)

    result = asyncio.run(transport.get_bytes("files/table/1/6/1"))

    assert result == b"raw-file-bytes"
    headers = session.requests[0][2]["headers"]
    assert "Content-Type" not in headers
    assert headers["Accept"] == "application/octet-stream"


def test_invalid_async_json_raises_response_error_with_ray():
    session = FakeSession(
        FakeResponse(200, headers={"qb-api-ray": "ray-invalid"}, content=b"not-json")
    )
    transport = async_transport(session)

    with pytest.raises(QuickbaseResponseError) as caught:
        asyncio.run(transport.post_json("records/query", retry_policy=RetryPolicy.SAFE))

    assert caught.value.qb_api_ray == "ray-invalid"
