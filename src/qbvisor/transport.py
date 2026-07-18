import os
import random
import time
from collections.abc import Callable, Mapping
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any

import requests

from .exceptions import (
    QuickbaseConfigurationError,
    QuickbaseConnectionError,
    QuickbaseHTTPError,
    QuickbaseRateLimitError,
    QuickbaseResponseError,
    QuickbaseTimeoutError,
)
from .log_runner import get_logger

logger = get_logger(__name__)

DEFAULT_TIMEOUT = (10.0, 120.0)
RETRYABLE_STATUS_CODES = frozenset({502, 503, 504})


class RetryPolicy(Enum):
    """Controls whether a request can be replayed after an uncertain failure."""

    NEVER = "never"
    SAFE = "safe"


class QuickBaseTransport:
    """Synchronous HTTP transport for the Quickbase JSON API."""

    def __init__(
        self,
        realm_hostname: str | None = None,
        auth_token: str | None = None,
        *,
        session: requests.Session | None = None,
        timeout: float | tuple[float, float] = DEFAULT_TIMEOUT,
        max_attempts: int = 5,
        base_delay: float = 1.0,
        max_delay: float = 64.0,
        sleep: Callable[[float], None] = time.sleep,
        jitter: Callable[[float, float], float] = random.uniform,
        clock: Callable[[], float] = time.time,
    ):
        self.realm_hostname = realm_hostname or os.getenv("QB_REALM_HOSTNAME")
        self.auth_token = auth_token or os.getenv("QB_REALM_API_KEY")
        if not self.realm_hostname or not self.auth_token:
            raise QuickbaseConfigurationError(
                "Both QB_REALM_HOSTNAME and QB_REALM_API_KEY must be set."
            )
        if max_attempts < 1:
            raise QuickbaseConfigurationError("max_attempts must be at least 1.")

        self.base_url = "https://api.quickbase.com/v1"
        self.headers = {
            "QB-Realm-Hostname": self.realm_hostname,
            "Authorization": self.auth_token,
            "Content-Type": "application/json",
            "User-Agent": "qbvisor/0.2",
        }
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._sleep = sleep
        self._jitter = jitter
        self._clock = clock
        self._owns_session = session is None
        self.session = session if session is not None else requests.Session()

    def __enter__(self) -> "QuickBaseTransport":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the internally created session, if this transport owns it."""
        if self._owns_session:
            self.session.close()

    def _make_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        *,
        retry_policy: RetryPolicy,
    ) -> dict[str, Any]:
        normalized_method = method.upper()
        normalized_path = path.lstrip("/")
        url = f"{self.base_url}/{normalized_path}"

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.request(
                    normalized_method,
                    url,
                    headers=self.headers,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
            except requests.Timeout as exc:
                if self._should_retry_exception(retry_policy, attempt):
                    self._wait_before_retry(normalized_method, normalized_path, attempt)
                    continue
                raise QuickbaseTimeoutError(normalized_method, normalized_path, attempt) from exc
            except requests.ConnectionError as exc:
                if self._should_retry_exception(retry_policy, attempt):
                    self._wait_before_retry(normalized_method, normalized_path, attempt)
                    continue
                raise QuickbaseConnectionError(normalized_method, normalized_path, attempt) from exc
            except requests.RequestException as exc:
                raise QuickbaseConnectionError(normalized_method, normalized_path, attempt) from exc

            qb_api_ray = self._header(response.headers, "qb-api-ray")
            retry_after = self._header(response.headers, "retry-after")
            logger.debug(
                "%s %s returned %s on attempt %s (qb-api-ray=%s)",
                normalized_method,
                normalized_path,
                response.status_code,
                attempt,
                qb_api_ray or "unavailable",
            )
            if response.status_code == 429 and attempt < self.max_attempts:
                self._wait_before_retry(
                    normalized_method,
                    normalized_path,
                    attempt,
                    retry_after=retry_after,
                    qb_api_ray=qb_api_ray,
                )
                continue
            if (
                response.status_code in RETRYABLE_STATUS_CODES
                and retry_policy is RetryPolicy.SAFE
                and attempt < self.max_attempts
            ):
                self._wait_before_retry(
                    normalized_method,
                    normalized_path,
                    attempt,
                    retry_after=retry_after,
                    qb_api_ray=qb_api_ray,
                )
                continue
            if not 200 <= response.status_code < 300:
                self._raise_http_error(
                    response,
                    normalized_method,
                    normalized_path,
                    qb_api_ray,
                    retry_after,
                )

            if response.status_code == 204 or not response.content:
                return {}
            try:
                payload = response.json()
            except (requests.JSONDecodeError, ValueError) as exc:
                raise QuickbaseResponseError(
                    normalized_method, normalized_path, qb_api_ray
                ) from exc
            if not isinstance(payload, dict):
                raise QuickbaseResponseError(normalized_method, normalized_path, qb_api_ray)
            return payload

        raise AssertionError("Request retry loop exited unexpectedly")

    def _should_retry_exception(self, retry_policy: RetryPolicy, attempt: int) -> bool:
        return retry_policy is RetryPolicy.SAFE and attempt < self.max_attempts

    def _wait_before_retry(
        self,
        method: str,
        path: str,
        attempt: int,
        *,
        retry_after: str | None = None,
        qb_api_ray: str | None = None,
    ) -> None:
        wait = self._retry_after_seconds(retry_after)
        if wait is None:
            backoff = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
            wait = self._jitter(backoff * 0.5, backoff * 1.5)
        logger.warning(
            "Retrying %s %s after attempt %s in %.1fs (qb-api-ray=%s)",
            method,
            path,
            attempt,
            wait,
            qb_api_ray or "unavailable",
        )
        self._sleep(wait)

    def _retry_after_seconds(self, value: str | None) -> float | None:
        if value is None:
            return None
        try:
            return max(0.0, float(value))
        except ValueError:
            try:
                return max(0.0, parsedate_to_datetime(value).timestamp() - self._clock())
            except (TypeError, ValueError, OverflowError):
                return None

    @staticmethod
    def _header(headers: Mapping[str, str], name: str) -> str | None:
        return next((value for key, value in headers.items() if key.lower() == name.lower()), None)

    @staticmethod
    def _raise_http_error(
        response: requests.Response,
        method: str,
        path: str,
        qb_api_ray: str | None,
        retry_after: str | None,
    ) -> None:
        message: str | None = None
        description: str | None = None
        try:
            payload = response.json()
        except (requests.JSONDecodeError, ValueError):
            payload = None
        if isinstance(payload, dict):
            raw_message = payload.get("message")
            raw_description = payload.get("description")
            message = raw_message if isinstance(raw_message, str) else None
            description = raw_description if isinstance(raw_description, str) else None

        error_type = QuickbaseRateLimitError if response.status_code == 429 else QuickbaseHTTPError
        raise error_type(
            method=method,
            path=path,
            status_code=response.status_code,
            message=message,
            description=description,
            qb_api_ray=qb_api_ray,
            retry_after=retry_after,
        )

    def get(
        self, path: str, params: dict[str, Any] | None = None, json_body: Any | None = None
    ) -> dict[str, Any]:
        return self._make_request(
            "GET", path, params=params, json_body=json_body, retry_policy=RetryPolicy.SAFE
        )

    def post(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        *,
        retry_policy: RetryPolicy = RetryPolicy.NEVER,
    ) -> dict[str, Any]:
        return self._make_request(
            "POST", path, params=params, json_body=json_body, retry_policy=retry_policy
        )

    def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        *,
        retry_policy: RetryPolicy = RetryPolicy.NEVER,
    ) -> dict[str, Any]:
        return self._make_request(
            "DELETE", path, params=params, json_body=json_body, retry_policy=retry_policy
        )
