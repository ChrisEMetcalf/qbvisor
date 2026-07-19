"""Build deterministic, read-only plans for declarative Quickbase schemas."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from ..exceptions import QuickbaseHTTPError, QuickbaseResponseError
from ..schema import (
    AppSpec,
    RelationshipSpec,
    SchemaAction,
    SchemaAttributeChange,
    SchemaChange,
    SchemaPlan,
    SchemaResourceKind,
    SchemaState,
    StateResource,
    SummaryFieldSpec,
    TableSpec,
)
from .state import DEFAULT_SCHEMA_STATE_PATH, load_schema_state

if TYPE_CHECKING:
    from ..client import QuickBaseClient

_ObservationStatus = Literal["existing", "create", "conflict"]


@dataclass(slots=True)
class _TableObservation:
    spec: TableSpec
    status: _ObservationStatus
    remote_id: str | None = None
    payload: dict[str, Any] | None = None
    fields_by_id: dict[int, dict[str, Any]] = field(default_factory=dict)
    field_ids_by_key: dict[str, int | None] = field(default_factory=dict)
    conflict_reason: str | None = None


def _response_error(path: str, expected: str, actual: Any) -> QuickbaseResponseError:
    return QuickbaseResponseError(
        "GET",
        path,
        expected=expected,
        actual=type(actual).__name__,
    )


def _required_string(payload: Mapping[str, Any], key: str, path: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise _response_error(path, f"non-empty string property {key}", value)
    return value


def _required_integer(payload: Mapping[str, Any], key: str, path: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise _response_error(path, f"integer property {key}", value)
    return value


def _objects(value: Any, path: str, property_name: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise _response_error(path, f"{property_name} array of objects", value)
    return cast(list[dict[str, Any]], value)


def _properties(payload: Mapping[str, Any], path: str) -> dict[str, Any]:
    value = payload.get("properties")
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise _response_error(path, "object property properties", value)
    return cast(dict[str, Any], value)


def _canonical_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(sorted((key, _canonical_json(item)) for key, item in value.items()))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(_canonical_json(item) for item in value)
    return value


def _attribute(name: str, before: Any, after: Any) -> SchemaAttributeChange | None:
    if _canonical_json(before) == _canonical_json(after):
        return None
    return SchemaAttributeChange(name=name, before=before, after=after)


def _present(changes: Sequence[SchemaAttributeChange | None]) -> tuple[SchemaAttributeChange, ...]:
    return tuple(change for change in changes if change is not None)


def _variables(payload: Mapping[str, Any], path: str) -> dict[str, str]:
    value = payload.get("variables", [])
    if not isinstance(value, list):
        raise _response_error(path, "variables array", value)
    result: dict[str, str] = {}
    for item in value:
        if not isinstance(item, dict):
            raise _response_error(path, "variables array of objects", item)
        name = item.get("name")
        variable_value = item.get("value")
        if not isinstance(name, str) or not isinstance(variable_value, str):
            raise _response_error(path, "variables with string name and value", item)
        if name in result:
            raise _response_error(path, "variables with unique names", item)
        result[name] = variable_value
    return result


class SchemaPlanner:
    """Observe Quickbase through the existing client and produce a mutation-free plan."""

    def __init__(self, client: QuickBaseClient):
        self.client = client
        self.spec: AppSpec
        self.state: SchemaState | None
        self.changes: list[SchemaChange]

    def plan(
        self,
        spec: AppSpec,
        *,
        state_path: str | Path = DEFAULT_SCHEMA_STATE_PATH,
    ) -> SchemaPlan:
        resolved_state_path = Path(state_path).resolve()
        self.spec = spec
        self.state = load_schema_state(resolved_state_path)
        self.changes = []

        app_id, app_status, app_reason = self._plan_app()
        if app_status == "existing":
            assert app_id is not None
            tables = self._plan_existing_tables(app_id)
            self._plan_relationships(app_id, tables)
        elif app_status == "create":
            tables = self._plan_new_tables()
            self._plan_relationships(None, tables)
        else:
            assert app_reason is not None
            self._block_descendants(app_reason)

        return SchemaPlan(
            spec=spec,
            state_path=resolved_state_path,
            state=self.state,
            changes=self.changes,
        )

    def _binding(self, address: str) -> StateResource | None:
        return self.state.resource(address) if self.state is not None else None

    def _append_conflict(
        self,
        *,
        address: str,
        kind: SchemaResourceKind,
        summary: str,
        reason: str,
        remote_id: str | int | None = None,
    ) -> None:
        self.changes.append(
            SchemaChange(
                address=address,
                kind=kind,
                action="conflict",
                summary=summary,
                reason=reason,
                remote_id=remote_id,
            )
        )

    def _append_create_or_stale(
        self,
        *,
        address: str,
        kind: SchemaResourceKind,
        summary: str,
    ) -> bool:
        binding = self._binding(address)
        if binding is not None:
            self._append_conflict(
                address=address,
                kind=kind,
                summary=f"Cannot {summary.lower()}",
                reason=(
                    f"state binds this resource to Quickbase ID {binding.remote_id}, "
                    "but its parent resource does not exist"
                ),
                remote_id=binding.remote_id,
            )
            return False
        self.changes.append(
            SchemaChange(address=address, kind=kind, action="create", summary=summary)
        )
        return True

    def _plan_app(self) -> tuple[str | None, _ObservationStatus, str | None]:
        address = self.spec.address
        binding = self._binding(address)
        app_id: str | None = None
        state_action: Literal["none", "bind"] = "none"

        if binding is not None:
            app_id = cast(str, binding.remote_id)
        else:
            configured = getattr(self.client.meta, "app_ids", {})
            if not isinstance(configured, dict):
                raise ValueError("Quickbase app configuration must be an object")
            matches = sorted(
                {
                    value
                    for name, value in configured.items()
                    if isinstance(name, str)
                    and isinstance(value, str)
                    and name.casefold() in {self.spec.key.casefold(), self.spec.name.casefold()}
                }
            )
            if len(matches) > 1:
                reason = (
                    "multiple QB_APP_IDS entries match the declarative app key or name "
                    "ignoring case"
                )
                self._append_conflict(
                    address=address,
                    kind="app",
                    summary=f"Cannot identify app {self.spec.name}",
                    reason=reason,
                )
                return None, "conflict", reason
            if matches:
                app_id = matches[0]
                state_action = "bind"

        if app_id is None:
            self._append_create_or_stale(
                address=address,
                kind="app",
                summary=f"Create app {self.spec.name}",
            )
            return None, "create", None

        path = f"apps/{app_id}"
        try:
            remote = self.client._request(method="GET", path=path, params={"appId": app_id})
        except QuickbaseHTTPError as error:
            if error.status_code != 404:
                raise
            reason = (
                f"state or QB_APP_IDS binds this resource to {app_id}, but Quickbase returned 404"
            )
            self._append_conflict(
                address=address,
                kind="app",
                summary=f"Cannot reconcile app {self.spec.name}",
                reason=reason,
                remote_id=app_id,
            )
            return app_id, "conflict", reason

        remote_name = _required_string(remote, "name", path)
        attribute_changes: list[SchemaAttributeChange | None] = [
            _attribute("name", remote_name, self.spec.name)
        ]
        if self.spec.description is not None:
            attribute_changes.append(
                _attribute("description", remote.get("description", ""), self.spec.description)
            )
        if self.spec.variables is not None:
            attribute_changes.append(
                _attribute("variables", _variables(remote, path), self.spec.variables)
            )
        if self.spec.security_properties is not None:
            security = remote.get("securityProperties", {})
            if not isinstance(security, dict):
                raise _response_error(path, "object property securityProperties", security)
            attribute_changes.extend(
                _attribute(
                    f"security_properties.{name}",
                    security.get(name),
                    desired,
                )
                for name, desired in self.spec.security_properties.items()
            )
        attributes = _present(attribute_changes)
        action: SchemaAction = "update" if attributes else "unchanged"
        self.changes.append(
            SchemaChange(
                address=address,
                kind="app",
                action=action,
                summary=(
                    f"Update app {remote_name}"
                    if attributes
                    else f"App {remote_name} matches desired state"
                ),
                remote_id=app_id,
                state_action=state_action,
                attributes=attributes,
            )
        )
        return app_id, "existing", None

    def _plan_new_tables(self) -> dict[str, _TableObservation]:
        observations: dict[str, _TableObservation] = {}
        for table in self.spec.tables:
            created = self._append_create_or_stale(
                address=table.address(self.spec.key),
                kind="table",
                summary=f"Create table {table.name}",
            )
            observation = _TableObservation(
                spec=table,
                status="create" if created else "conflict",
                conflict_reason=None if created else "table has a stale state binding",
            )
            for field_spec in table.fields:
                field_created = self._append_create_or_stale(
                    address=field_spec.address(self.spec.key, table.key),
                    kind="field",
                    summary=f"Create field {table.name}.{field_spec.label}",
                )
                observation.field_ids_by_key[field_spec.key] = None
                if not field_created:
                    observation.status = "conflict"
                    observation.conflict_reason = "a field has a stale state binding"
            observations[table.key] = observation
        return observations

    def _plan_existing_tables(self, app_id: str) -> dict[str, _TableObservation]:
        tables = self.client._request(
            method="GET",
            path="tables",
            params={"appId": app_id},
            response_type=list,
        )
        observations: dict[str, _TableObservation] = {}
        for table_spec in self.spec.tables:
            address = table_spec.address(self.spec.key)
            binding = self._binding(address)
            remote: dict[str, Any] | None = None
            state_action: Literal["none", "bind"] = "none"
            if binding is not None:
                remote = next(
                    (item for item in tables if item.get("id") == binding.remote_id), None
                )
                if remote is None:
                    reason = (
                        f"state binds this table to {binding.remote_id}, "
                        "but Quickbase no longer returns that ID"
                    )
                    self._append_conflict(
                        address=address,
                        kind="table",
                        summary=f"Cannot reconcile table {table_spec.name}",
                        reason=reason,
                        remote_id=binding.remote_id,
                    )
                    observation = _TableObservation(
                        spec=table_spec,
                        status="conflict",
                        conflict_reason=reason,
                    )
                    self._block_table_fields(observation, reason)
                    observations[table_spec.key] = observation
                    continue
            else:
                matches = [
                    item
                    for item in tables
                    if isinstance(item.get("name"), str)
                    and item["name"].casefold() == table_spec.name.casefold()
                ]
                if len(matches) > 1:
                    reason = f"multiple remote tables match {table_spec.name!r} ignoring case"
                    self._append_conflict(
                        address=address,
                        kind="table",
                        summary=f"Cannot identify table {table_spec.name}",
                        reason=reason,
                    )
                    observation = _TableObservation(
                        spec=table_spec,
                        status="conflict",
                        conflict_reason=reason,
                    )
                    self._block_table_fields(observation, reason)
                    observations[table_spec.key] = observation
                    continue
                if matches:
                    remote = matches[0]
                    state_action = "bind"

            if remote is None:
                created = self._append_create_or_stale(
                    address=address,
                    kind="table",
                    summary=f"Create table {table_spec.name}",
                )
                observation = _TableObservation(
                    spec=table_spec,
                    status="create" if created else "conflict",
                    conflict_reason=None if created else "table has a stale state binding",
                )
                for field_spec in table_spec.fields:
                    field_created = self._append_create_or_stale(
                        address=field_spec.address(self.spec.key, table_spec.key),
                        kind="field",
                        summary=f"Create field {table_spec.name}.{field_spec.label}",
                    )
                    observation.field_ids_by_key[field_spec.key] = None
                    if not field_created:
                        observation.status = "conflict"
                        observation.conflict_reason = "a field has a stale state binding"
                observations[table_spec.key] = observation
                continue

            remote_id = _required_string(remote, "id", "tables")
            remote_name = _required_string(remote, "name", "tables")
            attributes = _present(
                [
                    _attribute("name", remote_name, table_spec.name),
                    _attribute("description", remote.get("description", ""), table_spec.description)
                    if table_spec.description is not None
                    else None,
                    _attribute(
                        "singular_record_name",
                        remote.get("singleRecordName"),
                        table_spec.singular_record_name,
                    )
                    if table_spec.singular_record_name is not None
                    else None,
                    _attribute(
                        "plural_record_name",
                        remote.get("pluralRecordName"),
                        table_spec.plural_record_name,
                    )
                    if table_spec.plural_record_name is not None
                    else None,
                ]
            )
            self.changes.append(
                SchemaChange(
                    address=address,
                    kind="table",
                    action="update" if attributes else "unchanged",
                    summary=(
                        f"Update table {remote_name}"
                        if attributes
                        else f"Table {remote_name} matches desired state"
                    ),
                    remote_id=remote_id,
                    state_action=state_action,
                    attributes=attributes,
                )
            )
            fields = self.client._request(
                method="GET",
                path="fields",
                params={"tableId": remote_id, "includeFieldPerms": "true"},
                response_type=list,
            )
            fields_by_id = {_required_integer(item, "id", "fields"): item for item in fields}
            observation = _TableObservation(
                spec=table_spec,
                status="existing",
                remote_id=remote_id,
                payload=remote,
                fields_by_id=fields_by_id,
            )
            self._plan_fields(observation, fields)
            observations[table_spec.key] = observation
        return observations

    def _plan_fields(
        self,
        table: _TableObservation,
        remote_fields: list[dict[str, Any]],
    ) -> None:
        for field_spec in table.spec.fields:
            address = field_spec.address(self.spec.key, table.spec.key)
            binding = self._binding(address)
            remote: dict[str, Any] | None = None
            state_action: Literal["none", "bind"] = "none"
            if binding is not None:
                remote = table.fields_by_id.get(cast(int, binding.remote_id))
                if remote is None:
                    reason = (
                        f"state binds this field to {binding.remote_id}, "
                        "but Quickbase no longer returns that ID"
                    )
                    self._append_conflict(
                        address=address,
                        kind="field",
                        summary=f"Cannot reconcile field {table.spec.name}.{field_spec.label}",
                        reason=reason,
                        remote_id=binding.remote_id,
                    )
                    table.field_ids_by_key[field_spec.key] = None
                    continue
            else:
                matches = [
                    item
                    for item in remote_fields
                    if isinstance(item.get("label"), str)
                    and item["label"].casefold() == field_spec.label.casefold()
                ]
                if len(matches) > 1:
                    reason = f"multiple remote fields match {field_spec.label!r} ignoring case"
                    self._append_conflict(
                        address=address,
                        kind="field",
                        summary=f"Cannot identify field {table.spec.name}.{field_spec.label}",
                        reason=reason,
                    )
                    table.field_ids_by_key[field_spec.key] = None
                    continue
                if matches:
                    remote = matches[0]
                    state_action = "bind"

            if remote is None:
                created = self._append_create_or_stale(
                    address=address,
                    kind="field",
                    summary=f"Create field {table.spec.name}.{field_spec.label}",
                )
                table.field_ids_by_key[field_spec.key] = None
                if not created:
                    table.status = "conflict"
                    table.conflict_reason = "a field has a stale state binding"
                continue

            field_id = _required_integer(remote, "id", "fields")
            remote_label = _required_string(remote, "label", "fields")
            remote_type = _required_string(remote, "fieldType", "fields")
            table.field_ids_by_key[field_spec.key] = field_id
            if remote_type != field_spec.field_type:
                self._append_conflict(
                    address=address,
                    kind="field",
                    summary=f"Cannot change field type for {table.spec.name}.{remote_label}",
                    reason=(
                        f"Quickbase field type is {remote_type!r}, but desired type is "
                        f"{field_spec.field_type!r}; field types are immutable"
                    ),
                    remote_id=field_id,
                )
                continue

            top_level = {
                "help_text": "fieldHelp",
                "required": "required",
                "unique": "unique",
                "appears_by_default": "appearsByDefault",
                "find_enabled": "findEnabled",
                "audited": "audited",
                "add_to_forms": "addToForms",
                "bold": "bold",
            }
            candidates: list[SchemaAttributeChange | None] = [
                _attribute("label", remote_label, field_spec.label)
            ]
            for spec_name, response_name in top_level.items():
                desired = getattr(field_spec, spec_name)
                if desired is not None:
                    candidates.append(_attribute(spec_name, remote.get(response_name), desired))
            remote_properties = _properties(remote, "fields")
            if field_spec.properties is not None:
                candidates.extend(
                    _attribute(f"properties.{name}", remote_properties.get(name), desired)
                    for name, desired in field_spec.properties.items()
                )
            attributes = _present(candidates)
            self.changes.append(
                SchemaChange(
                    address=address,
                    kind="field",
                    action="update" if attributes else "unchanged",
                    summary=(
                        f"Update field {table.spec.name}.{remote_label}"
                        if attributes
                        else f"Field {table.spec.name}.{remote_label} matches desired state"
                    ),
                    remote_id=field_id,
                    state_action=state_action,
                    attributes=attributes,
                )
            )

    def _block_table_fields(self, table: _TableObservation, reason: str) -> None:
        for field_spec in table.spec.fields:
            self._append_conflict(
                address=field_spec.address(self.spec.key, table.spec.key),
                kind="field",
                summary=f"Cannot plan field {table.spec.name}.{field_spec.label}",
                reason=f"table dependency is unresolved: {reason}",
            )
            table.field_ids_by_key[field_spec.key] = None

    def _block_descendants(self, reason: str) -> None:
        for table in self.spec.tables:
            self._append_conflict(
                address=table.address(self.spec.key),
                kind="table",
                summary=f"Cannot plan table {table.name}",
                reason=f"app dependency is unresolved: {reason}",
            )
            observation = _TableObservation(spec=table, status="conflict")
            self._block_table_fields(observation, reason)
        for relationship in self.spec.relationships:
            self._block_relationship(relationship, reason)

    def _block_relationship(self, relationship: RelationshipSpec, reason: str) -> None:
        self._append_conflict(
            address=relationship.address(self.spec.key),
            kind="relationship",
            summary=f"Cannot plan relationship {relationship.key}",
            reason=f"relationship dependency is unresolved: {reason}",
        )
        for lookup_key in relationship.lookup_fields:
            self._append_conflict(
                address=relationship.lookup_address(self.spec.key, lookup_key),
                kind="lookup",
                summary=f"Cannot plan lookup {relationship.key}.{lookup_key}",
                reason=f"relationship dependency is unresolved: {reason}",
            )
        for summary in relationship.summary_fields:
            self._append_conflict(
                address=summary.address(self.spec.key, relationship.key),
                kind="summary",
                summary=f"Cannot plan summary {relationship.key}.{summary.key}",
                reason=f"relationship dependency is unresolved: {reason}",
            )

    def _plan_relationships(
        self,
        app_id: str | None,
        tables: dict[str, _TableObservation],
    ) -> None:
        relationships_by_child: dict[str, list[dict[str, Any]]] = {}
        for relationship in self.spec.relationships:
            parent = tables[relationship.parent_table]
            child = tables[relationship.child_table]
            dependency_reason = parent.conflict_reason or child.conflict_reason
            if parent.status == "conflict" or child.status == "conflict":
                self._block_relationship(
                    relationship,
                    dependency_reason or "a managed table or field has a conflict",
                )
                continue
            if app_id is None or parent.status == "create" or child.status == "create":
                self._plan_new_relationship(relationship)
                continue
            assert parent.remote_id is not None and child.remote_id is not None
            remote_relationships = relationships_by_child.get(child.remote_id)
            if remote_relationships is None:
                path = f"tables/{child.remote_id}/relationships"
                payload = self.client._request(method="GET", path=path)
                remote_relationships = _objects(payload.get("relationships"), path, "relationships")
                relationships_by_child[child.remote_id] = remote_relationships
            self._plan_existing_relationship(
                relationship,
                parent,
                child,
                remote_relationships,
            )

    def _plan_new_relationship(self, relationship: RelationshipSpec) -> None:
        created = self._append_create_or_stale(
            address=relationship.address(self.spec.key),
            kind="relationship",
            summary=(
                f"Create relationship {relationship.parent_table} -> {relationship.child_table}"
            ),
        )
        if not created:
            self._block_generated_relationship_fields(
                relationship, "relationship has a stale state binding"
            )
            return
        for lookup_key in relationship.lookup_fields:
            self._append_create_or_stale(
                address=relationship.lookup_address(self.spec.key, lookup_key),
                kind="lookup",
                summary=f"Create lookup {relationship.key}.{lookup_key}",
            )
        for summary in relationship.summary_fields:
            self._append_create_or_stale(
                address=summary.address(self.spec.key, relationship.key),
                kind="summary",
                summary=f"Create summary {relationship.key}.{summary.key}",
            )

    def _block_generated_relationship_fields(
        self, relationship: RelationshipSpec, reason: str
    ) -> None:
        for lookup_key in relationship.lookup_fields:
            self._append_conflict(
                address=relationship.lookup_address(self.spec.key, lookup_key),
                kind="lookup",
                summary=f"Cannot plan lookup {relationship.key}.{lookup_key}",
                reason=reason,
            )
        for summary in relationship.summary_fields:
            self._append_conflict(
                address=summary.address(self.spec.key, relationship.key),
                kind="summary",
                summary=f"Cannot plan summary {relationship.key}.{summary.key}",
                reason=reason,
            )

    def _plan_existing_relationship(
        self,
        relationship: RelationshipSpec,
        parent: _TableObservation,
        child: _TableObservation,
        remote_relationships: list[dict[str, Any]],
    ) -> None:
        address = relationship.address(self.spec.key)
        binding = self._binding(address)
        remote: dict[str, Any] | None = None
        state_action: Literal["none", "bind"] = "none"
        if binding is not None:
            remote = next(
                (item for item in remote_relationships if item.get("id") == binding.remote_id),
                None,
            )
            if remote is None:
                reason = (
                    f"state binds this relationship to {binding.remote_id}, "
                    "but Quickbase no longer returns that ID"
                )
                self._append_conflict(
                    address=address,
                    kind="relationship",
                    summary=f"Cannot reconcile relationship {relationship.key}",
                    reason=reason,
                    remote_id=binding.remote_id,
                )
                self._block_generated_relationship_fields(relationship, reason)
                return
        else:
            candidates = [
                item
                for item in remote_relationships
                if item.get("parentTableId") == parent.remote_id
                and item.get("childTableId") == child.remote_id
            ]
            if len(candidates) > 1:
                reason = "multiple remote relationships connect the declared parent and child"
                self._append_conflict(
                    address=address,
                    kind="relationship",
                    summary=f"Cannot identify relationship {relationship.key}",
                    reason=reason,
                )
                self._block_generated_relationship_fields(relationship, reason)
                return
            if candidates:
                remote = candidates[0]
                state_action = "bind"

        if remote is None:
            self._plan_new_relationship(relationship)
            return

        relationship_id = _required_integer(remote, "id", "relationships")
        if (
            remote.get("parentTableId") != parent.remote_id
            or remote.get("childTableId") != child.remote_id
        ):
            reason = "the bound relationship connects different parent or child tables"
            self._append_conflict(
                address=address,
                kind="relationship",
                summary=f"Cannot repoint relationship {relationship.key}",
                reason=reason,
                remote_id=relationship_id,
            )
            self._block_generated_relationship_fields(relationship, reason)
            return

        foreign_key = remote.get("foreignKeyField", {})
        if not isinstance(foreign_key, dict):
            raise _response_error("relationships", "object property foreignKeyField", foreign_key)
        relation_attributes: list[SchemaAttributeChange] = []
        if relationship.foreign_key_label is not None:
            foreign_label = foreign_key.get("label")
            if not isinstance(foreign_label, str):
                raise _response_error(
                    "relationships", "foreignKeyField string label", foreign_label
                )
            label_change = _attribute(
                "foreign_key_label", foreign_label, relationship.foreign_key_label
            )
            if label_change is not None:
                relation_attributes.append(label_change)

        lookup_creates = self._plan_lookups(relationship, parent, child, remote)
        summary_creates = self._plan_summaries(relationship, parent, child, remote)
        if lookup_creates:
            relation_attributes.append(
                SchemaAttributeChange(
                    name="lookup_fields.add",
                    before=[],
                    after=lookup_creates,
                )
            )
        if summary_creates:
            relation_attributes.append(
                SchemaAttributeChange(
                    name="summary_fields.add",
                    before=[],
                    after=summary_creates,
                )
            )
        action: SchemaAction = "update" if relation_attributes else "unchanged"
        self.changes.append(
            SchemaChange(
                address=address,
                kind="relationship",
                action=action,
                summary=(
                    f"Update relationship {relationship.key}"
                    if relation_attributes
                    else f"Relationship {relationship.key} matches desired state"
                ),
                remote_id=relationship_id,
                state_action=state_action,
                attributes=relation_attributes,
            )
        )

    def _plan_lookups(
        self,
        relationship: RelationshipSpec,
        parent: _TableObservation,
        child: _TableObservation,
        remote: Mapping[str, Any],
    ) -> list[str]:
        remote_lookups = _objects(remote.get("lookupFields", []), "relationships", "lookupFields")
        lookup_ids = {_required_integer(item, "id", "relationships") for item in remote_lookups}
        creates: list[str] = []
        for field_key in relationship.lookup_fields:
            address = relationship.lookup_address(self.spec.key, field_key)
            desired_target = parent.field_ids_by_key.get(field_key)
            binding = self._binding(address)
            if desired_target is None:
                if binding is not None:
                    self._append_conflict(
                        address=address,
                        kind="lookup",
                        summary=f"Cannot reconcile lookup {relationship.key}.{field_key}",
                        reason="lookup source field is not yet bound, but lookup state already exists",
                        remote_id=binding.remote_id,
                    )
                else:
                    self.changes.append(
                        SchemaChange(
                            address=address,
                            kind="lookup",
                            action="create",
                            summary=f"Create lookup {relationship.key}.{field_key}",
                        )
                    )
                    creates.append(field_key)
                continue

            candidates = [
                field_id
                for field_id in lookup_ids
                if field_id in child.fields_by_id
                and _properties(child.fields_by_id[field_id], "fields").get("lookupTargetFieldId")
                == desired_target
            ]
            remote_id: int | None = None
            state_action: Literal["none", "bind"] = "none"
            if binding is not None:
                bound_id = cast(int, binding.remote_id)
                if bound_id not in lookup_ids:
                    self._append_conflict(
                        address=address,
                        kind="lookup",
                        summary=f"Cannot reconcile lookup {relationship.key}.{field_key}",
                        reason=(
                            f"state binds this lookup to {bound_id}, but the relationship "
                            "no longer returns that field"
                        ),
                        remote_id=bound_id,
                    )
                    continue
                if bound_id not in candidates:
                    self._append_conflict(
                        address=address,
                        kind="lookup",
                        summary=f"Cannot repoint lookup {relationship.key}.{field_key}",
                        reason="the bound lookup targets a different parent field",
                        remote_id=bound_id,
                    )
                    continue
                remote_id = bound_id
            elif len(candidates) > 1:
                self._append_conflict(
                    address=address,
                    kind="lookup",
                    summary=f"Cannot identify lookup {relationship.key}.{field_key}",
                    reason="multiple lookup fields target the declared parent field",
                )
                continue
            elif candidates:
                remote_id = candidates[0]
                state_action = "bind"

            if remote_id is None:
                self.changes.append(
                    SchemaChange(
                        address=address,
                        kind="lookup",
                        action="create",
                        summary=f"Create lookup {relationship.key}.{field_key}",
                    )
                )
                creates.append(field_key)
            else:
                label = _required_string(child.fields_by_id[remote_id], "label", "fields")
                self.changes.append(
                    SchemaChange(
                        address=address,
                        kind="lookup",
                        action="unchanged",
                        summary=f"Lookup {relationship.key}.{field_key} matches {label}",
                        remote_id=remote_id,
                        state_action=state_action,
                    )
                )
        return creates

    def _plan_summaries(
        self,
        relationship: RelationshipSpec,
        parent: _TableObservation,
        child: _TableObservation,
        remote: Mapping[str, Any],
    ) -> list[str]:
        remote_summaries = _objects(
            remote.get("summaryFields", []), "relationships", "summaryFields"
        )
        summary_ids = {_required_integer(item, "id", "relationships") for item in remote_summaries}
        creates: list[str] = []
        for summary in relationship.summary_fields:
            address = summary.address(self.spec.key, relationship.key)
            desired_target = (
                0 if summary.field is None else child.field_ids_by_key.get(summary.field)
            )
            binding = self._binding(address)
            if desired_target is None:
                if binding is not None:
                    self._append_conflict(
                        address=address,
                        kind="summary",
                        summary=f"Cannot reconcile summary {relationship.key}.{summary.key}",
                        reason="summary source field is not yet bound, but summary state exists",
                        remote_id=binding.remote_id,
                    )
                else:
                    self._append_summary_create(relationship, summary)
                    creates.append(summary.key)
                continue

            candidates = [
                field_id
                for field_id in summary_ids
                if field_id in parent.fields_by_id
                and self._summary_definition_matches(
                    parent.fields_by_id[field_id], summary, desired_target
                )
            ]
            remote_id: int | None = None
            state_action: Literal["none", "bind"] = "none"
            if binding is not None:
                bound_id = cast(int, binding.remote_id)
                if bound_id not in summary_ids:
                    self._append_conflict(
                        address=address,
                        kind="summary",
                        summary=f"Cannot reconcile summary {relationship.key}.{summary.key}",
                        reason=(
                            f"state binds this summary to {bound_id}, but the relationship "
                            "no longer returns that field"
                        ),
                        remote_id=bound_id,
                    )
                    continue
                if bound_id not in candidates:
                    self._append_conflict(
                        address=address,
                        kind="summary",
                        summary=f"Cannot repoint summary {relationship.key}.{summary.key}",
                        reason="the bound summary has a different function or source field",
                        remote_id=bound_id,
                    )
                    continue
                remote_id = bound_id
                stored_where = binding.attributes.get("where")
                if stored_where != summary.where:
                    self._append_conflict(
                        address=address,
                        kind="summary",
                        summary=f"Cannot update summary filter {relationship.key}.{summary.key}",
                        reason=(
                            "Quickbase does not expose summary filters for verification; "
                            "changing one requires explicit replacement support"
                        ),
                        remote_id=bound_id,
                    )
                    continue
            else:
                label_matches = candidates
                if summary.label is not None:
                    label_matches = [
                        field_id
                        for field_id in candidates
                        if isinstance(parent.fields_by_id[field_id].get("label"), str)
                        and parent.fields_by_id[field_id]["label"].casefold()
                        == summary.label.casefold()
                    ]
                if len(label_matches) == 1:
                    remote_id = label_matches[0]
                elif not label_matches and len(candidates) == 1:
                    remote_id = candidates[0]
                elif len(candidates) > 1:
                    self._append_conflict(
                        address=address,
                        kind="summary",
                        summary=f"Cannot identify summary {relationship.key}.{summary.key}",
                        reason="multiple summary fields match the declared function and source",
                    )
                    continue
                if remote_id is not None:
                    if summary.where is not None:
                        self._append_conflict(
                            address=address,
                            kind="summary",
                            summary=f"Cannot import summary {relationship.key}.{summary.key}",
                            reason=(
                                "Quickbase does not expose the existing summary filter; "
                                "the declared filter cannot be verified during import"
                            ),
                            remote_id=remote_id,
                        )
                        continue
                    state_action = "bind"

            if remote_id is None:
                self._append_summary_create(relationship, summary)
                creates.append(summary.key)
                continue

            remote_label = _required_string(parent.fields_by_id[remote_id], "label", "fields")
            label_change = (
                _attribute("label", remote_label, summary.label)
                if summary.label is not None
                else None
            )
            attributes = _present([label_change])
            self.changes.append(
                SchemaChange(
                    address=address,
                    kind="summary",
                    action="update" if attributes else "unchanged",
                    summary=(
                        f"Update summary field {remote_label}"
                        if attributes
                        else f"Summary {relationship.key}.{summary.key} matches desired state"
                    ),
                    remote_id=remote_id,
                    state_action=state_action,
                    attributes=attributes,
                )
            )
        return creates

    @staticmethod
    def _summary_definition_matches(
        remote_field: Mapping[str, Any],
        summary: SummaryFieldSpec,
        desired_target: int,
    ) -> bool:
        properties = remote_field.get("properties")
        return (
            isinstance(properties, dict)
            and properties.get("summaryFunction") == summary.accumulation_type
            and properties.get("summaryTargetFieldId") == desired_target
        )

    def _append_summary_create(
        self, relationship: RelationshipSpec, summary: SummaryFieldSpec
    ) -> None:
        self.changes.append(
            SchemaChange(
                address=summary.address(self.spec.key, relationship.key),
                kind="summary",
                action="create",
                summary=f"Create summary {relationship.key}.{summary.key}",
            )
        )


def plan_application_schema(
    client: QuickBaseClient,
    spec: AppSpec,
    *,
    state_path: str | Path = DEFAULT_SCHEMA_STATE_PATH,
) -> SchemaPlan:
    """Plan one application without writing state or changing Quickbase."""
    return SchemaPlanner(client).plan(spec, state_path=state_path)
