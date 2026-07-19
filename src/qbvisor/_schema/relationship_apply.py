"""Apply relationships and bind the generated Quickbase fields by observed metadata."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from functools import partial
from typing import TYPE_CHECKING, Any, cast

from ..exceptions import QuickbaseSchemaApplyError
from ..schema import (
    AppSpec,
    RelationshipSpec,
    SchemaChange,
    StateResource,
    SummaryFieldSpec,
)

if TYPE_CHECKING:
    from ..client import QuickBaseClient


def _required_integer(payload: Mapping[str, Any], key: str, context: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise QuickbaseSchemaApplyError(f"{context} did not include an integer {key}")
    return value


def _required_string(payload: Mapping[str, Any], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise QuickbaseSchemaApplyError(f"{context} did not include a string {key}")
    return value


def _objects(value: Any, context: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise QuickbaseSchemaApplyError(f"{context} was not an array of objects")
    return cast(list[dict[str, Any]], value)


def _properties(field: Mapping[str, Any]) -> Mapping[str, Any]:
    properties = field.get("properties", {})
    if not isinstance(properties, dict):
        raise QuickbaseSchemaApplyError("Generated field properties were not an object")
    return properties


def _summary_body(
    summary: SummaryFieldSpec,
    child_field_ids: Mapping[str, int],
) -> dict[str, Any]:
    body: dict[str, Any] = {"accumulationType": summary.accumulation_type}
    if summary.field is not None:
        body["summaryFid"] = child_field_ids[summary.field]
    if summary.label is not None:
        body["label"] = summary.label
    if summary.where is not None:
        body["where"] = summary.where
    return body


def _summary_matches(
    item: Mapping[str, Any],
    spec: SummaryFieldSpec,
    target_id: int,
) -> bool:
    label = item.get("label")
    return (
        _properties(item).get("summaryFunction") == spec.accumulation_type
        and _properties(item).get("summaryTargetFieldId") == target_id
        and (
            spec.label is None
            or (isinstance(label, str) and label.casefold() == spec.label.casefold())
        )
    )


def _lookup_matches(item: Mapping[str, Any], target_id: int) -> bool:
    return _properties(item).get("lookupTargetFieldId") == target_id


class RelationshipApplier:
    """Execute relationship mutations after their table and field dependencies exist."""

    def __init__(
        self,
        client: QuickBaseClient,
        spec: AppSpec,
        changes: Mapping[str, SchemaChange],
        resources: dict[str, StateResource],
        table_ids: Mapping[str, str],
        field_ids: Mapping[tuple[str, str], int],
    ):
        self.client = client
        self.spec = spec
        self.changes = changes
        self.resources = resources
        self.table_ids = table_ids
        self.field_ids = field_ids

    def apply(self) -> None:
        for relationship in self.spec.relationships:
            self._apply_relationship(relationship)

    def _apply_relationship(self, relationship: RelationshipSpec) -> None:
        address = relationship.address(self.spec.key)
        change = self.changes[address]
        parent_id = self.table_ids[relationship.parent_table]
        child_id = self.table_ids[relationship.child_table]
        lookup_specs = [
            field_key
            for field_key in relationship.lookup_fields
            if self.changes[relationship.lookup_address(self.spec.key, field_key)].action
            == "create"
        ]
        summary_specs = [
            summary
            for summary in relationship.summary_fields
            if self.changes[summary.address(self.spec.key, relationship.key)].action == "create"
        ]

        if change.action == "create":
            body: dict[str, Any] = {"parentTableId": parent_id}
            if relationship.foreign_key_label is not None:
                body["foreignKeyField"] = {"label": relationship.foreign_key_label}
            if relationship.lookup_fields:
                body["lookupFieldIds"] = [
                    self.field_ids[(relationship.parent_table, field_key)]
                    for field_key in relationship.lookup_fields
                ]
            if relationship.summary_fields:
                child_fields = self._table_field_ids(relationship.child_table)
                body["summaryFields"] = [
                    _summary_body(summary, child_fields) for summary in relationship.summary_fields
                ]
            response = self.client._request(
                method="POST",
                path=f"tables/{child_id}/relationship",
                json_body=body,
            )
            relationship_id = _required_integer(response, "id", "Create relationship response")
        else:
            relationship_id = cast(int, change.remote_id)
            changed_names = {attribute.name for attribute in change.attributes}
            if "foreign_key_label" in changed_names:
                self.client._request(
                    method="POST",
                    path=f"fields/{relationship_id}",
                    params={"tableId": child_id},
                    json_body={"label": relationship.foreign_key_label},
                )
            update_body: dict[str, Any] = {}
            if lookup_specs:
                update_body["lookupFieldIds"] = [
                    self.field_ids[(relationship.parent_table, field_key)]
                    for field_key in lookup_specs
                ]
            if summary_specs:
                child_fields = self._table_field_ids(relationship.child_table)
                update_body["summaryFields"] = [
                    _summary_body(summary, child_fields) for summary in summary_specs
                ]
            if update_body:
                self.client._request(
                    method="POST",
                    path=f"tables/{child_id}/relationship/{relationship_id}",
                    json_body=update_body,
                )

        for summary in relationship.summary_fields:
            summary_change = self.changes[summary.address(self.spec.key, relationship.key)]
            if summary_change.action == "update":
                self.client._request(
                    method="POST",
                    path=f"fields/{summary_change.remote_id}",
                    params={"tableId": parent_id},
                    json_body={"label": summary.label},
                )

        self._bind_observed_resources(
            relationship,
            relationship_id=relationship_id,
            parent_id=parent_id,
            child_id=child_id,
        )

    def _bind_observed_resources(
        self,
        relationship: RelationshipSpec,
        *,
        relationship_id: int,
        parent_id: str,
        child_id: str,
    ) -> None:
        path = f"tables/{child_id}/relationships"
        response = self.client._request(method="GET", path=path)
        relationships = _objects(response.get("relationships"), "Relationship response")
        observed = next((item for item in relationships if item.get("id") == relationship_id), None)
        if observed is None:
            raise QuickbaseSchemaApplyError(
                f"Applied relationship {relationship_id} was not returned by Quickbase"
            )
        parent_fields = self._fields(parent_id)
        child_fields = self._fields(child_id)
        parent_by_id = {
            _required_integer(item, "id", "Parent field"): item for item in parent_fields
        }
        child_by_id = {_required_integer(item, "id", "Child field"): item for item in child_fields}

        foreign_key = observed.get("foreignKeyField", {})
        if not isinstance(foreign_key, dict):
            raise QuickbaseSchemaApplyError("Relationship foreignKeyField was not an object")
        foreign_label = foreign_key.get("label")
        if not isinstance(foreign_label, str) or not foreign_label:
            foreign_field = child_by_id.get(relationship_id)
            if foreign_field is None:
                raise QuickbaseSchemaApplyError("Relationship foreign key metadata was missing")
            foreign_label = _required_string(foreign_field, "label", "Foreign key field")
        relationship_address = relationship.address(self.spec.key)
        self.resources[relationship_address] = StateResource(
            address=relationship_address,
            kind="relationship",
            remote_id=relationship_id,
            name=foreign_label,
        )

        observed_lookup_ids = {
            _required_integer(item, "id", "Lookup field")
            for item in _objects(observed.get("lookupFields", []), "lookupFields")
        }
        used_lookup_ids: set[int] = set()
        for field_key in relationship.lookup_fields:
            lookup_address = relationship.lookup_address(self.spec.key, field_key)
            lookup_change = self.changes[lookup_address]
            source_id = self.field_ids[(relationship.parent_table, field_key)]
            lookup_id = self._bound_or_matching_field(
                change=lookup_change,
                observed_ids=observed_lookup_ids,
                fields_by_id=child_by_id,
                used_ids=used_lookup_ids,
                matches=partial(_lookup_matches, target_id=source_id),
                context=f"lookup {relationship.key}.{field_key}",
            )
            used_lookup_ids.add(lookup_id)
            self.resources[lookup_address] = StateResource(
                address=lookup_address,
                kind="lookup",
                remote_id=lookup_id,
                name=_required_string(child_by_id[lookup_id], "label", "Lookup field"),
                attributes={"source_field": field_key},
            )

        observed_summary_ids = {
            _required_integer(item, "id", "Summary field")
            for item in _objects(observed.get("summaryFields", []), "summaryFields")
        }
        used_summary_ids: set[int] = set()
        for summary in relationship.summary_fields:
            summary_address = summary.address(self.spec.key, relationship.key)
            summary_change = self.changes[summary_address]
            target_id = (
                0
                if summary.field is None
                else self.field_ids[(relationship.child_table, summary.field)]
            )
            summary_id = self._bound_or_matching_field(
                change=summary_change,
                observed_ids=observed_summary_ids,
                fields_by_id=parent_by_id,
                used_ids=used_summary_ids,
                matches=partial(_summary_matches, spec=summary, target_id=target_id),
                context=f"summary {relationship.key}.{summary.key}",
            )
            used_summary_ids.add(summary_id)
            self.resources[summary_address] = StateResource(
                address=summary_address,
                kind="summary",
                remote_id=summary_id,
                name=_required_string(parent_by_id[summary_id], "label", "Summary field"),
                attributes={
                    "accumulation_type": summary.accumulation_type,
                    "field": summary.field,
                    "where": summary.where,
                },
            )

    @staticmethod
    def _bound_or_matching_field(
        *,
        change: SchemaChange,
        observed_ids: set[int],
        fields_by_id: Mapping[int, dict[str, Any]],
        used_ids: set[int],
        matches: Callable[[Mapping[str, Any]], bool],
        context: str,
    ) -> int:
        if change.remote_id is not None:
            remote_id = cast(int, change.remote_id)
            if remote_id not in observed_ids or remote_id not in fields_by_id:
                raise QuickbaseSchemaApplyError(
                    f"Bound {context} field {remote_id} was not returned after apply"
                )
            return remote_id
        candidates = [
            field_id
            for field_id in observed_ids - used_ids
            if field_id in fields_by_id and matches(fields_by_id[field_id])
        ]
        if len(candidates) != 1:
            raise QuickbaseSchemaApplyError(
                f"Expected one generated field for {context}, found {len(candidates)}"
            )
        return candidates[0]

    def _fields(self, table_id: str) -> list[dict[str, Any]]:
        return self.client._request(
            method="GET",
            path="fields",
            params={"tableId": table_id, "includeFieldPerms": "true"},
            response_type=list,
        )

    def _table_field_ids(self, table_key: str) -> dict[str, int]:
        return {
            field_key: field_id
            for (resource_table, field_key), field_id in self.field_ids.items()
            if resource_table == table_key
        }


def apply_relationships(
    client: QuickBaseClient,
    spec: AppSpec,
    changes: Mapping[str, SchemaChange],
    resources: dict[str, StateResource],
    table_ids: Mapping[str, str],
    field_ids: Mapping[tuple[str, str], int],
) -> None:
    """Apply every declared relationship and update the candidate state resources."""
    RelationshipApplier(
        client,
        spec,
        changes,
        resources,
        table_ids,
        field_ids,
    ).apply()
