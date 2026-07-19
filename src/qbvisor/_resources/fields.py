"""Private Quickbase field endpoint operations."""

from typing import Any

from .base import BaseResource


class FieldResource(BaseResource):
    """Build and execute field requests for QuickBaseClient."""

    def create(
        self,
        app_name: str,
        table_name: str,
        label: str,
        field_type: str,
    ) -> dict[str, Any]:
        _, table_id = self._ids(app_name, table_name)
        return self._request_object(
            method="POST",
            path="fields",
            params={"tableId": table_id},
            json_body={"label": label, "fieldType": field_type},
        )

    def delete(
        self,
        app_name: str,
        table_name: str,
        field_labels: list[str],
    ) -> dict[str, Any]:
        app_id, table_id = self._ids(app_name, table_name)
        field_map = self.meta.get_field_map(app_id, table_id)
        field_ids = [field_map[label]["id"] for label in field_labels]
        return self._request_object(
            method="DELETE",
            path="fields",
            params={"tableId": table_id},
            json_body={"fieldIds": field_ids},
        )

    def usage(
        self,
        app_name: str,
        table_name: str,
        *,
        skip: int | None = None,
        top: int | None = None,
    ) -> list[dict[str, Any]]:
        if skip is not None and skip < 0:
            raise ValueError("skip cannot be negative")
        if top is not None and top < 1:
            raise ValueError("top must be at least 1")

        _, table_id = self._ids(app_name, table_name)
        params: dict[str, Any] = {"tableId": table_id}
        if skip is not None:
            params["skip"] = skip
        if top is not None:
            params["top"] = top
        return self._request_array(
            method="GET",
            path="fields/usage",
            params=params,
        )

    def usage_for_field(
        self,
        app_name: str,
        table_name: str,
        field: str | int,
    ) -> list[dict[str, Any]]:
        app_id, table_id = self._ids(app_name, table_name)
        field_id = (
            field if isinstance(field, int) else self.meta.get_field_id(app_id, table_id, field)
        )
        return self._request_array(
            method="GET",
            path=f"fields/usage/{field_id}",
            params={"tableId": table_id},
        )

    def get_id(self, app_id: str, table_id: str, field_label: str) -> int:
        return self.meta.get_field_id(app_id, table_id, field_label)

    def get(self, app_id: Any, table_id: Any, field_id: Any) -> Any:
        return self.transport.get(
            f"fields/{field_id}",
            params={"tableId": self.meta.get_table_id(app_id, table_id)},
        )
