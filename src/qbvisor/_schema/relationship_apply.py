"""Apply relationship resources when their declared dependencies are ready."""

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


def _summary_body(summary: SummaryFieldSpec, target_id: int) -> dict[str, Any]:
    body: dict[str, Any] = {"accumulationType": summary.accumulation_type}
    if summary.field is not None:
        body["summaryFid"] = target_id
    if summary.label is not None:
        body["label"] = summary.label
    if summary.where is not None:
        body["where"] = summary.where
    return body


def _summary_matches(
    item: Mapping[str, Any],
    *,
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


def _lookup_matches(item: Mapping[str, Any], *, target_id: int) -> bool:
    return _properties(item).get("lookupTargetFieldId") == target_id


class RelationshipResourceApplier:
    """Apply one relationship, lookup, or summary node at a time."""

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
        self.relationship_ids: dict[str, int] = {}
        self.claimed_lookup_ids: dict[str, set[int]] = {}
        self.claimed_summary_ids: dict[str, set[int]] = {}
        self.operations: dict[str, Callable[[], None]] = {}
        for relationship in spec.relationships:
            self.operations[relationship.address(spec.key)] = partial(
                self._apply_relationship,
                relationship,
            )
            for field_key in relationship.lookup_fields:
                self.operations[relationship.lookup_address(spec.key, field_key)] = partial(
                    self._apply_lookup,
                    relationship,
                    field_key,
                )
            for summary in relationship.summary_fields:
                self.operations[summary.address(spec.key, relationship.key)] = partial(
                    self._apply_summary,
                    relationship,
                    summary,
                )

    def apply(self, address: str) -> None:
        operation = self.operations.get(address)
        if operation is None:
            raise QuickbaseSchemaApplyError(f"No schema resource operation exists for {address}")
        operation()

    def _apply_relationship(self, relationship: RelationshipSpec) -> None:
        address = relationship.address(self.spec.key)
        change = self.changes[address]
        parent_id = self.table_ids[relationship.parent_table]
        child_id = self.table_ids[relationship.child_table]
        if change.action == "create":
            body: dict[str, Any] = {"parentTableId": parent_id}
            if relationship.foreign_key_label is not None:
                body["foreignKeyField"] = {"label": relationship.foreign_key_label}
            response = self.client._request(
                method="POST",
                path=f"tables/{child_id}/relationship",
                json_body=body,
            )
            relationship_id = _required_integer(
                response,
                "id",
                "Create relationship response",
            )
        else:
            relationship_id = cast(int, change.remote_id)
            changed = {attribute.name for attribute in change.attributes}
            if "foreign_key_label" in changed:
                self.client._request(
                    method="POST",
                    path=f"fields/{relationship_id}",
                    params={"tableId": child_id},
                    json_body={"label": relationship.foreign_key_label},
                )

        observed = self._relationship(child_id, relationship_id)
        child_fields = self._fields_by_id(child_id)
        foreign_key = observed.get("foreignKeyField", {})
        if not isinstance(foreign_key, dict):
            raise QuickbaseSchemaApplyError("Relationship foreignKeyField was not an object")
        foreign_label = foreign_key.get("label")
        if not isinstance(foreign_label, str) or not foreign_label:
            foreign_field = child_fields.get(relationship_id)
            if foreign_field is None:
                raise QuickbaseSchemaApplyError("Relationship foreign key metadata was missing")
            foreign_label = _required_string(foreign_field, "label", "Foreign key field")
        self.relationship_ids[relationship.key] = relationship_id
        self.resources[address] = StateResource(
            address=address,
            kind="relationship",
            remote_id=relationship_id,
            name=foreign_label,
        )

    def _apply_lookup(self, relationship: RelationshipSpec, field_key: str) -> None:
        address = relationship.lookup_address(self.spec.key, field_key)
        change = self.changes[address]
        relationship_id = self._relationship_id(relationship)
        child_id = self.table_ids[relationship.child_table]
        source_id = self.field_ids[(relationship.parent_table, field_key)]
        if change.action == "create":
            self.client._request(
                method="POST",
                path=f"tables/{child_id}/relationship/{relationship_id}",
                json_body={"lookupFieldIds": [source_id]},
            )

        observed = self._relationship(child_id, relationship_id)
        observed_ids = {
            _required_integer(item, "id", "Lookup field")
            for item in _objects(observed.get("lookupFields", []), "lookupFields")
        }
        fields = self._fields_by_id(child_id)
        claimed = self.claimed_lookup_ids.setdefault(relationship.key, set())
        lookup_id = self._bound_or_matching_field(
            change=change,
            observed_ids=observed_ids,
            fields_by_id=fields,
            claimed_ids=claimed,
            matches=partial(_lookup_matches, target_id=source_id),
            context=f"lookup {relationship.key}.{field_key}",
        )
        claimed.add(lookup_id)
        self.resources[address] = StateResource(
            address=address,
            kind="lookup",
            remote_id=lookup_id,
            name=_required_string(fields[lookup_id], "label", "Lookup field"),
            attributes={"source_field": field_key},
        )

    def _apply_summary(
        self,
        relationship: RelationshipSpec,
        summary: SummaryFieldSpec,
    ) -> None:
        address = summary.address(self.spec.key, relationship.key)
        change = self.changes[address]
        relationship_id = self._relationship_id(relationship)
        parent_id = self.table_ids[relationship.parent_table]
        child_id = self.table_ids[relationship.child_table]
        target_id = (
            0
            if summary.field is None
            else self.field_ids[(relationship.child_table, summary.field)]
        )
        if change.action == "create":
            self.client._request(
                method="POST",
                path=f"tables/{child_id}/relationship/{relationship_id}",
                json_body={"summaryFields": [_summary_body(summary, target_id)]},
            )
        elif change.action == "update":
            self.client._request(
                method="POST",
                path=f"fields/{change.remote_id}",
                params={"tableId": parent_id},
                json_body={"label": summary.label},
            )

        observed = self._relationship(child_id, relationship_id)
        observed_ids = {
            _required_integer(item, "id", "Summary field")
            for item in _objects(observed.get("summaryFields", []), "summaryFields")
        }
        fields = self._fields_by_id(parent_id)
        claimed = self.claimed_summary_ids.setdefault(relationship.key, set())
        summary_id = self._bound_or_matching_field(
            change=change,
            observed_ids=observed_ids,
            fields_by_id=fields,
            claimed_ids=claimed,
            matches=partial(_summary_matches, spec=summary, target_id=target_id),
            context=f"summary {relationship.key}.{summary.key}",
        )
        claimed.add(summary_id)
        self.resources[address] = StateResource(
            address=address,
            kind="summary",
            remote_id=summary_id,
            name=_required_string(fields[summary_id], "label", "Summary field"),
            attributes={
                "accumulation_type": summary.accumulation_type,
                "field": summary.field,
                "where": summary.where,
            },
        )

    def _relationship_id(self, relationship: RelationshipSpec) -> int:
        relationship_id = self.relationship_ids.get(relationship.key)
        if relationship_id is None:
            raise QuickbaseSchemaApplyError(
                f"Relationship {relationship.key} was not applied before its generated fields"
            )
        return relationship_id

    def _relationship(self, child_id: str, relationship_id: int) -> dict[str, Any]:
        path = f"tables/{child_id}/relationships"
        response = self.client._request(method="GET", path=path)
        relationships = _objects(response.get("relationships"), "Relationship response")
        observed = next(
            (item for item in relationships if item.get("id") == relationship_id),
            None,
        )
        if observed is None:
            raise QuickbaseSchemaApplyError(
                f"Applied relationship {relationship_id} was not returned by Quickbase"
            )
        return observed

    def _fields_by_id(self, table_id: str) -> dict[int, dict[str, Any]]:
        fields = self.client._request(
            method="GET",
            path="fields",
            params={"tableId": table_id, "includeFieldPerms": "true"},
            response_type=list,
        )
        return {_required_integer(item, "id", "Field"): item for item in fields}

    @staticmethod
    def _bound_or_matching_field(
        *,
        change: SchemaChange,
        observed_ids: set[int],
        fields_by_id: Mapping[int, dict[str, Any]],
        claimed_ids: set[int],
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
            for field_id in observed_ids - claimed_ids
            if field_id in fields_by_id and matches(fields_by_id[field_id])
        ]
        if len(candidates) != 1:
            raise QuickbaseSchemaApplyError(
                f"Expected one generated field for {context}, found {len(candidates)}"
            )
        return candidates[0]
