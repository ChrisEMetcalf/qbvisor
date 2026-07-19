"""Shared dependencies for private client resource services."""

from __future__ import annotations

from typing import Any, Protocol, cast, overload

from ..metadata import QuickBaseMetaCache
from ..transport import QuickBaseTransport, RetryPolicy


class ClientContext(Protocol):
    """The narrow QuickBaseClient surface available to endpoint resources."""

    meta: QuickBaseMetaCache
    transport: QuickBaseTransport

    def _ids(
        self,
        app_name: str,
        table_name: str | None = None,
    ) -> tuple[str, str | None]: ...

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        retry_policy: RetryPolicy | None = None,
        *,
        response_type: type[Any] = dict,
    ) -> Any: ...


class BaseResource:
    """Provide endpoint resources with typed access to shared client behavior."""

    def __init__(self, client: ClientContext):
        self._client = client

    @property
    def meta(self) -> QuickBaseMetaCache:
        return self._client.meta

    @property
    def transport(self) -> QuickBaseTransport:
        return self._client.transport

    @overload
    def _ids(self, app_name: str, table_name: None = None) -> tuple[str, None]: ...

    @overload
    def _ids(self, app_name: str, table_name: str) -> tuple[str, str]: ...

    def _ids(
        self,
        app_name: str,
        table_name: str | None = None,
    ) -> tuple[str, str | None]:
        return self._client._ids(app_name, table_name)

    def _request_object(self, *, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Delegate an object response without adding omitted request arguments."""
        return cast(dict[str, Any], self._client._request(method=method, path=path, **kwargs))

    def _request_array(self, *, method: str, path: str, **kwargs: Any) -> list[dict[str, Any]]:
        """Delegate an array response while enforcing its documented top-level shape."""
        return cast(
            list[dict[str, Any]],
            self._client._request(method=method, path=path, response_type=list, **kwargs),
        )
