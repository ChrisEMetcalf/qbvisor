"""Private Quickbase application endpoint operations."""

from typing import Any

from .base import BaseResource


class AppResource(BaseResource):
    """Build and execute application requests for QuickBaseClient."""

    def create(
        self,
        name: str,
        description: str | None = None,
        assign_token: bool = False,
        variables: list[dict[str, str]] | None = None,
        security_properties: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        body = {"name": name, "assignToken": assign_token}
        if description:
            body["description"] = description
        if variables:
            body["variables"] = variables
        if security_properties:
            body["securityProperties"] = security_properties
        return self._request_object(method="POST", path="apps", json_body=body)

    def get(self, app_name: str) -> dict[str, Any]:
        app_id, _ = self._ids(app_name)
        return self._request_object(
            method="GET",
            path=f"apps/{app_id}",
            params={"appId": app_id},
        )

    def events(self, app_name: str) -> list[dict[str, Any]]:
        app_id, _ = self._ids(app_name)
        return self._request_array(method="GET", path=f"apps/{app_id}/events")

    def roles(self, app_name: str) -> list[dict[str, Any]]:
        app_id, _ = self._ids(app_name)
        return self._request_array(method="GET", path=f"apps/{app_id}/roles")

    def update(
        self,
        app_name: str,
        new_name: str | None = None,
        description: str | None = None,
        variables: list[dict[str, str]] | None = None,
        security_properties: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
        app_id, _ = self._ids(app_name)
        body: dict[str, Any] = {}
        if new_name:
            body["name"] = new_name
        if description:
            body["description"] = description
        if variables:
            body["variables"] = variables
        if security_properties:
            body["securityProperties"] = security_properties
        if not body:
            raise ValueError("No update parameters provided.")
        return self._request_object(method="POST", path=f"apps/{app_id}", json_body=body)

    def delete(self, app_name: str) -> dict[str, Any]:
        app_id, _ = self._ids(app_name)
        return self._request_object(
            method="DELETE",
            path=f"apps/{app_id}",
            params={"appId": app_id},
        )

    def copy(
        self,
        app_name: str,
        new_app_name: str,
        description: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        app_id, _ = self._ids(app_name)
        body = {"name": new_app_name, "description": description, "properties": properties}
        return self._request_object(
            method="POST",
            path=f"apps/{app_id}/copy",
            json_body=body,
        )
