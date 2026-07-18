import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import aiohttp

from .exceptions import (
    QuickbaseConnectionError,
    QuickbaseResponseError,
    QuickbaseTimeoutError,
)
from .log_runner import get_logger
from .transport import (
    RETRYABLE_STATUS_CODES,
    JSONValue,
    QuickBaseTransport,
    RetryPolicy,
    _decode_file_response,
    _http_error,
)

logger = get_logger(__name__)


class AsyncQuickBaseTransport:
    """Internal asynchronous transport using a ``QuickBaseTransport`` policy."""

    def __init__(
        self,
        transport: QuickBaseTransport,
        *,
        session: aiohttp.ClientSession | None = None,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.base_url = transport.base_url
        self.headers = dict(transport.headers)
        self.timeout = self._client_timeout(transport.timeout)
        self._request_policy = transport._request_policy
        self._sleep = sleep
        self._owns_session = session is None
        self._session = session

    async def __aenter__(self) -> "AsyncQuickBaseTransport":
        await self._get_session()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        """Close the internally created session, if this transport owns it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def post_json(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        headers: Mapping[str, str] | None = None,
        retry_policy: RetryPolicy = RetryPolicy.NEVER,
    ) -> JSONValue:
        response = await self._request(
            "POST",
            path,
            params=params,
            json_body=json_body,
            headers=headers,
            retry_policy=retry_policy,
            response_kind="json",
        )
        if isinstance(response, bytes):
            raise AssertionError("JSON transport returned a bytes response")
        return response

    async def get_bytes(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> bytes:
        response = await self._request(
            "GET",
            path,
            params=params,
            headers=headers,
            retry_policy=RetryPolicy.SAFE,
            response_kind="bytes",
        )
        if not isinstance(response, bytes):
            raise AssertionError("Binary transport returned a non-bytes response")
        return response

    async def get_file(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> bytes:
        response = await self._request(
            "GET",
            path,
            params=params,
            retry_policy=RetryPolicy.SAFE,
            response_kind="file",
        )
        if not isinstance(response, bytes):
            raise AssertionError("File transport returned a non-bytes response")
        return response

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        headers: Mapping[str, str] | None = None,
        retry_policy: RetryPolicy,
        response_kind: str,
    ) -> JSONValue | bytes:
        normalized_method = method.upper()
        normalized_path = self._normalized_path(path)
        url = (
            path
            if path.startswith(("https://", "http://"))
            else f"{self.base_url}/{normalized_path}"
        )
        request_headers = dict(self.headers)
        if response_kind in {"bytes", "file"}:
            request_headers = {
                key: value
                for key, value in request_headers.items()
                if key.lower() != "content-type"
            }
            request_headers["Accept"] = "application/octet-stream"
        if headers:
            request_headers.update(headers)
        session = await self._get_session()

        for attempt in range(1, self._request_policy.max_attempts + 1):
            try:
                async with session.request(
                    normalized_method,
                    url,
                    headers=request_headers,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                ) as response:
                    raw = await response.read()
                    status_code = response.status
                    response_headers = response.headers
            except TimeoutError:
                if self._request_policy.should_retry_exception(retry_policy, attempt):
                    await self._wait_before_retry(normalized_method, normalized_path, attempt)
                    continue
                raise QuickbaseTimeoutError(normalized_method, normalized_path, attempt) from None
            except aiohttp.ClientConnectionError:
                if self._request_policy.should_retry_exception(retry_policy, attempt):
                    await self._wait_before_retry(normalized_method, normalized_path, attempt)
                    continue
                raise QuickbaseConnectionError(
                    normalized_method, normalized_path, attempt
                ) from None
            except aiohttp.ClientError:
                raise QuickbaseConnectionError(
                    normalized_method, normalized_path, attempt
                ) from None

            qb_api_ray = self._header(response_headers, "qb-api-ray")
            retry_after = self._header(response_headers, "retry-after")
            logger.debug(
                "%s %s returned %s on attempt %s (qb-api-ray=%s)",
                normalized_method,
                normalized_path,
                status_code,
                attempt,
                qb_api_ray or "unavailable",
            )
            if status_code == 429:
                retry_after_seconds = self._request_policy.retry_after_seconds(retry_after)
                if retry_after_seconds is not None and attempt < self._request_policy.max_attempts:
                    await self._wait_before_retry(
                        normalized_method,
                        normalized_path,
                        attempt,
                        wait=retry_after_seconds,
                        qb_api_ray=qb_api_ray,
                    )
                    continue
                raise _http_error(
                    method=normalized_method,
                    path=normalized_path,
                    status_code=status_code,
                    payload=self._json_or_none(raw),
                    qb_api_ray=qb_api_ray,
                    retry_after=retry_after,
                )
            if (
                status_code in RETRYABLE_STATUS_CODES
                and retry_policy is RetryPolicy.SAFE
                and attempt < self._request_policy.max_attempts
            ):
                await self._wait_before_retry(
                    normalized_method,
                    normalized_path,
                    attempt,
                    retry_after=retry_after,
                    qb_api_ray=qb_api_ray,
                )
                continue
            if not 200 <= status_code < 300:
                raise _http_error(
                    method=normalized_method,
                    path=normalized_path,
                    status_code=status_code,
                    payload=self._json_or_none(raw),
                    qb_api_ray=qb_api_ray,
                    retry_after=retry_after,
                )

            if response_kind == "bytes":
                return raw
            if response_kind == "file":
                return _decode_file_response(
                    raw,
                    self._header(response_headers, "content-type"),
                    method=normalized_method,
                    path=normalized_path,
                    qb_api_ray=qb_api_ray,
                )
            if status_code == 204 or not raw:
                return {}
            payload = self._json_or_none(raw)
            if payload is None:
                raise QuickbaseResponseError(normalized_method, normalized_path, qb_api_ray)
            return payload

        raise AssertionError("Request retry loop exited unexpectedly")

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _wait_before_retry(
        self,
        method: str,
        path: str,
        attempt: int,
        *,
        wait: float | None = None,
        retry_after: str | None = None,
        qb_api_ray: str | None = None,
    ) -> None:
        if wait is None:
            wait = self._request_policy.retry_delay(attempt, retry_after)
        logger.warning(
            "Retrying %s %s after attempt %s in %.1fs (qb-api-ray=%s)",
            method,
            path,
            attempt,
            wait,
            qb_api_ray or "unavailable",
        )
        await self._sleep(wait)

    def _normalized_path(self, path: str) -> str:
        if path.startswith(f"{self.base_url}/"):
            return path.removeprefix(f"{self.base_url}/")
        return path.lstrip("/")

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str | None:
        return next((value for key, value in headers.items() if key.lower() == name.lower()), None)

    @staticmethod
    def _json_or_none(raw: bytes) -> JSONValue:
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    @staticmethod
    def _client_timeout(timeout: float | tuple[float, float]) -> aiohttp.ClientTimeout:
        if isinstance(timeout, tuple):
            connect_timeout, read_timeout = timeout
        else:
            connect_timeout = read_timeout = timeout
        return aiohttp.ClientTimeout(
            total=None,
            connect=connect_timeout,
            sock_read=read_timeout,
        )
