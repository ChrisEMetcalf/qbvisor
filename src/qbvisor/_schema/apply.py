"""Apply reviewed app, table, and field plans with verified state publication."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, cast

from ..exceptions import (
    QuickbaseSchemaApplyError,
    QuickbaseSchemaConflictError,
    QuickbaseSchemaStalePlanError,
)
from ..schema import (
    AppSpec,
    FieldSpec,
    SchemaApplyResult,
    SchemaChange,
    SchemaPlan,
    SchemaState,
    StateResource,
    TableSpec,
    _thaw_json,
)
from .state import (
    SchemaStateLock,
    publish_schema_state,
    write_schema_state_candidate,
)

if TYPE_CHECKING:
    from ..client import QuickBaseClient

_RELATIONSHIP_KINDS = frozenset({"relationship", "lookup", "summary"})


def _required_string(payload: Mapping[str, Any], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise QuickbaseSchemaApplyError(
            f"{context} response did not include a non-empty string {key}"
        )
    return value


def _required_integer(payload: Mapping[str, Any], key: str, context: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise QuickbaseSchemaApplyError(f"{context} response did not include an integer {key}")
    return value


def _variables(value: Mapping[str, str]) -> list[dict[str, str]]:
    return [{"name": name, "value": item} for name, item in sorted(value.items())]


def _field_body(spec: FieldSpec, *, create: bool) -> dict[str, Any]:
    body: dict[str, Any] = {"label": spec.label}
    if create:
        body["fieldType"] = spec.field_type
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
    for spec_name, request_name in top_level.items():
        value = getattr(spec, spec_name)
        if value is not None:
            body[request_name] = value
    if spec.properties is not None:
        body["properties"] = _thaw_json(spec.properties)
    return body


class SchemaApplier:
    """Execute only the mutations represented by a previously reviewed plan."""

    def __init__(self, client: QuickBaseClient):
        self.client = client

    def apply(self, plan: SchemaPlan) -> SchemaApplyResult:
        if not isinstance(plan, SchemaPlan):
            raise TypeError("plan must be a SchemaPlan")
        if plan.has_conflicts:
            raise QuickbaseSchemaConflictError(
                "Schema plan contains conflicts; resolve them and create a new plan"
            )
        unsupported = [
            change
            for change in plan.changes
            if change.kind in _RELATIONSHIP_KINDS
            and (change.mutates_quickbase or change.mutates_state)
        ]
        if unsupported:
            raise QuickbaseSchemaApplyError(
                "Relationship changes are not supported by this apply stage"
            )

        with SchemaStateLock(plan.state_path):
            current = self.client.plan_app(plan.spec, state_path=plan.state_path)
            if current != plan:
                raise QuickbaseSchemaStalePlanError(
                    "Quickbase or schema state changed after this plan was created; review a new plan"
                )
            if plan.quickbase_change_count == 0 and plan.state_change_count == 0:
                if plan.state is None:
                    raise QuickbaseSchemaApplyError("A no-op plan must have existing schema state")
                return SchemaApplyResult(
                    plan=plan,
                    verification=current,
                    state=plan.state,
                    state_written=False,
                )

            next_state = self._execute(plan)
            candidate = write_schema_state_candidate(plan.state_path, next_state)
            try:
                verification = self.client.plan_app(plan.spec, state_path=candidate)
                self._verify(verification)
                publish_schema_state(candidate, plan.state_path)
            finally:
                candidate.unlink(missing_ok=True)

            published_verification = SchemaPlan(
                spec=verification.spec,
                state_path=plan.state_path,
                state=verification.state,
                changes=verification.changes,
            )
            return SchemaApplyResult(
                plan=plan,
                verification=published_verification,
                state=next_state,
                state_written=True,
            )

    def _execute(self, plan: SchemaPlan) -> SchemaState:
        changes = {change.address: change for change in plan.changes}
        resources = {
            resource.address: resource
            for resource in (plan.state.resources if plan.state is not None else ())
        }

        app_change = changes[plan.spec.address]
        app_id = self._apply_app(plan.spec, app_change)
        resources[plan.spec.address] = StateResource(
            address=plan.spec.address,
            kind="app",
            remote_id=app_id,
            name=plan.spec.name,
        )

        for table_spec in plan.spec.tables:
            table_address = table_spec.address(plan.spec.key)
            table_change = changes[table_address]
            table_id = self._apply_table(app_id, table_spec, table_change)
            resources[table_address] = StateResource(
                address=table_address,
                kind="table",
                remote_id=table_id,
                name=table_spec.name,
            )
            for field_spec in table_spec.fields:
                field_address = field_spec.address(plan.spec.key, table_spec.key)
                field_change = changes[field_address]
                field_id = self._apply_field(table_id, field_spec, field_change)
                resources[field_address] = StateResource(
                    address=field_address,
                    kind="field",
                    remote_id=field_id,
                    name=field_spec.label,
                    attributes={"field_type": field_spec.field_type},
                )

        return SchemaState(
            lineage=plan.state.lineage if plan.state is not None else SchemaState().lineage,
            serial=(plan.state.serial if plan.state is not None else 0) + 1,
            resources=tuple(resources.values()),
        )

    def _apply_app(self, spec: AppSpec, change: SchemaChange) -> str:
        if change.action == "create":
            body: dict[str, Any] = {"name": spec.name, "assignToken": False}
            if spec.description is not None:
                body["description"] = spec.description
            if spec.variables is not None:
                body["variables"] = _variables(spec.variables)
            if spec.security_properties is not None:
                body["securityProperties"] = dict(spec.security_properties)
            response = self.client._request(method="POST", path="apps", json_body=body)
            return _required_string(response, "id", "Create app")

        app_id = cast(str, change.remote_id)
        if change.action == "update":
            changed = {attribute.name for attribute in change.attributes}
            update_body: dict[str, Any] = {}
            if "name" in changed:
                update_body["name"] = spec.name
            if "description" in changed:
                update_body["description"] = spec.description
            if "variables" in changed and spec.variables is not None:
                update_body["variables"] = _variables(spec.variables)
            security_names = {
                name.removeprefix("security_properties.")
                for name in changed
                if name.startswith("security_properties.")
            }
            if security_names and spec.security_properties is not None:
                update_body["securityProperties"] = {
                    name: spec.security_properties[name] for name in sorted(security_names)
                }
            self.client._request(method="POST", path=f"apps/{app_id}", json_body=update_body)
        return app_id

    def _apply_table(
        self,
        app_id: str,
        spec: TableSpec,
        change: SchemaChange,
    ) -> str:
        if change.action == "create":
            body: dict[str, Any] = {"name": spec.name}
            if spec.description is not None:
                body["description"] = spec.description
            if spec.singular_record_name is not None:
                body["singleRecordName"] = spec.singular_record_name
            if spec.plural_record_name is not None:
                body["pluralRecordName"] = spec.plural_record_name
            response = self.client._request(
                method="POST",
                path="tables",
                params={"appId": app_id},
                json_body=body,
            )
            return _required_string(response, "id", "Create table")

        table_id = cast(str, change.remote_id)
        if change.action == "update":
            changed = {attribute.name for attribute in change.attributes}
            update_body: dict[str, Any] = {}
            if "name" in changed:
                update_body["name"] = spec.name
            if "description" in changed:
                update_body["description"] = spec.description
            if "singular_record_name" in changed:
                update_body["singleRecordName"] = spec.singular_record_name
            if "plural_record_name" in changed:
                update_body["pluralRecordName"] = spec.plural_record_name
            self.client._request(
                method="POST",
                path=f"tables/{table_id}",
                params={"appId": app_id},
                json_body=update_body,
            )
        return table_id

    def _apply_field(
        self,
        table_id: str,
        spec: FieldSpec,
        change: SchemaChange,
    ) -> int:
        if change.action == "create":
            response = self.client._request(
                method="POST",
                path="fields",
                params={"tableId": table_id},
                json_body=_field_body(spec, create=True),
            )
            return _required_integer(response, "id", "Create field")

        field_id = cast(int, change.remote_id)
        if change.action == "update":
            changed = {attribute.name for attribute in change.attributes}
            complete_body = _field_body(spec, create=False)
            property_names = {
                name.removeprefix("properties.")
                for name in changed
                if name.startswith("properties.")
            }
            body = {
                request_name: value
                for request_name, value in complete_body.items()
                if self._field_request_name_is_changed(request_name, changed)
            }
            if property_names and spec.properties is not None:
                body["properties"] = {
                    name: _thaw_json(spec.properties[name]) for name in sorted(property_names)
                }
            self.client._request(
                method="POST",
                path=f"fields/{field_id}",
                params={"tableId": table_id},
                json_body=body,
            )
        return field_id

    @staticmethod
    def _field_request_name_is_changed(request_name: str, changed: set[str]) -> bool:
        mapping = {
            "label": "label",
            "fieldHelp": "help_text",
            "required": "required",
            "unique": "unique",
            "appearsByDefault": "appears_by_default",
            "findEnabled": "find_enabled",
            "audited": "audited",
            "addToForms": "add_to_forms",
            "bold": "bold",
        }
        return request_name != "properties" and mapping.get(request_name) in changed

    @staticmethod
    def _verify(plan: SchemaPlan) -> None:
        if plan.has_conflicts or plan.quickbase_change_count or plan.state_change_count:
            raise QuickbaseSchemaApplyError(
                "Applied schema did not converge to an unchanged, fully bound plan; "
                "the previous state file was preserved"
            )


def apply_application_schema(client: QuickBaseClient, plan: SchemaPlan) -> SchemaApplyResult:
    """Apply a reviewed plan and atomically publish state after verification."""
    return SchemaApplier(client).apply(plan)
