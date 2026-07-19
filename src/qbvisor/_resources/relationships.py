"""Private Quickbase relationship endpoint operations."""

from collections.abc import Sequence
from typing import Any, cast

from ..models import RelationshipSummary
from .base import BaseResource


class RelationshipResource(BaseResource):
    """Build and execute relationship requests for QuickBaseClient."""

    def get_all(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        _, table_id = self._ids(app_name, table_name)
        response = self._request_object(
            method="GET",
            path=f"tables/{table_id}/relationships",
        )
        return response.get("relationships", [])

    def create(
        self,
        app_name: str,
        table_name: str,
        parent_table_name: str,
        foreign_key_label: str | None = None,
        lookup_field_ids: list[int] | None = None,
        summary_fields: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        app_id, table_id = self._ids(app_name, table_name)
        parent_id = self.meta.get_table_id(app_id, parent_table_name)
        body: dict[str, Any] = {"parentTableId": parent_id}
        if foreign_key_label:
            body["foreignKeyField"] = {"label": foreign_key_label}
        if lookup_field_ids:
            body["lookupFieldIds"] = lookup_field_ids
        if summary_fields:
            body["summaryFields"] = summary_fields
        return self._request_object(
            method="POST",
            path=f"tables/{table_id}/relationship",
            json_body=body,
        )

    def update(
        self,
        app_name: str,
        table_name: str,
        relationship: str | int,
        *,
        lookup_fields: Sequence[str | int] | None = None,
        summary_fields: Sequence[RelationshipSummary] | None = None,
    ) -> dict[str, Any]:
        if not lookup_fields and not summary_fields:
            raise ValueError("Provide at least one lookup field or summary field")

        app_id, child_table_id = self._ids(app_name, table_name)
        relationship_id = (
            relationship
            if isinstance(relationship, int)
            else self.meta.get_field_id(app_id, child_table_id, relationship)
        )
        body: dict[str, Any] = {}

        if lookup_fields:
            parent_table_id: str | None = None
            if any(isinstance(field, str) for field in lookup_fields):
                relationships = self.get_all(app_name, table_name)
                match = next(
                    (item for item in relationships if item.get("id") == relationship_id),
                    None,
                )
                if match is None or not isinstance(match.get("parentTableId"), str):
                    raise ValueError(f"Relationship {relationship_id} was not found")
                parent_table_id = match["parentTableId"]
            body["lookupFieldIds"] = [
                field
                if isinstance(field, int)
                else self.meta.get_field_id(app_id, cast(str, parent_table_id), field)
                for field in lookup_fields
            ]

        if summary_fields:
            summaries: list[dict[str, Any]] = []
            for summary in summary_fields:
                definition: dict[str, Any] = {
                    "accumulationType": summary.accumulation_type,
                }
                if summary.field is not None:
                    definition["summaryFid"] = (
                        summary.field
                        if isinstance(summary.field, int)
                        else self.meta.get_field_id(app_id, child_table_id, summary.field)
                    )
                if summary.label is not None:
                    definition["label"] = summary.label
                if summary.where is not None:
                    definition["where"] = summary.where
                summaries.append(definition)
            body["summaryFields"] = summaries

        response = self._request_object(
            method="POST",
            path=f"tables/{child_table_id}/relationship/{relationship_id}",
            json_body=body,
        )
        self.meta.invalidate_fields(app_id, child_table_id)
        parent_id = response.get("parentTableId")
        if isinstance(parent_id, str):
            self.meta.invalidate_fields(app_id, parent_id)
        return response

    def delete(
        self,
        app_name: str,
        table_name: str,
        related_field: str,
    ) -> Any | None:
        app_id, table_id = self._ids(app_name, table_name)
        relationship_id = self.meta.get_field_id(app_id, table_id, related_field)
        response = self._request_object(
            method="DELETE",
            path=f"tables/{table_id}/relationship/{relationship_id}",
        )
        return response.get("relationshipId", None)
