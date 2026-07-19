"""Private Quickbase table endpoint operations."""

from typing import Any

from .base import BaseResource


class TableResource(BaseResource):
    """Build and execute table requests for QuickBaseClient."""

    def create(
        self,
        app_name: str,
        table_name: str,
        description: str | None = None,
        singular_record_name: str | None = None,
        plural_record_name: str | None = None,
    ) -> dict[str, Any]:
        app_id, _ = self._ids(app_name)
        body = {"name": table_name}
        if description is not None:
            body["description"] = description
        if singular_record_name is not None:
            body["singleRecordName"] = singular_record_name
        if plural_record_name is not None:
            body["pluralRecordName"] = plural_record_name
        return self._request_object(
            method="POST",
            path="tables",
            params={"appId": app_id},
            json_body=body,
        )

    def list(self, app_name: str) -> list[dict[str, Any]]:
        app_id, _ = self._ids(app_name)
        return self._request_array(
            method="GET",
            path="tables",
            params={"appId": app_id},
        )

    def get(self, app_name: str, table_name: str) -> dict[str, Any]:
        app_id, table_id = self._ids(app_name, table_name)
        return self._request_object(
            method="GET",
            path=f"tables/{table_id}",
            params={"appId": app_id},
        )

    def update(
        self,
        app_name: str,
        table_name: str,
        new_table_name: str | None = None,
        singular_record_name: str | None = None,
        plural_record_name: str | None = None,
    ) -> dict[str, Any]:
        app_id, table_id = self._ids(app_name, table_name)
        body: dict[str, Any] = {}
        if new_table_name:
            body["name"] = new_table_name
        if singular_record_name:
            body["singleRecordName"] = singular_record_name
        if plural_record_name:
            body["pluralRecordName"] = plural_record_name
        if not body:
            raise ValueError(
                "Must specify at least one field to update "
                "(new_table_name, singular_record_name, or plural_record_name)."
            )
        return self._request_object(
            method="POST",
            path=f"tables/{table_id}",
            params={"appId": app_id},
            json_body=body,
        )

    def delete(self, app_name: str, table_name: str) -> dict[str, Any]:
        app_id, table_id = self._ids(app_name, table_name)
        return self._request_object(
            method="DELETE",
            path=f"tables/{table_id}",
            params={"appId": app_id},
        )

    def get_id(self, app_id: str, table_id: str) -> str:
        return self.meta.get_table_id(app_id, table_id)
