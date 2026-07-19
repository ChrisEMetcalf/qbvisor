import json
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from qbvisor import (
    AppSpec,
    FieldSpec,
    FormulaSpec,
    QuickBaseClient,
    QuickbaseSchemaApplyError,
    QuickbaseSchemaConflictError,
    QuickbaseSchemaLockError,
    QuickbaseSchemaStalePlanError,
    SchemaState,
    StateResource,
    TableSpec,
)
from qbvisor._schema.state import SchemaStateLock

APP_ID = "app_operations"
TABLE_ID = "tbl_projects"


def application_spec(
    *,
    description: str | None = None,
    table_description: str | None = None,
    required: bool | None = None,
) -> AppSpec:
    return AppSpec(
        key="operations",
        name="Operations",
        description=description,
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                description=table_description,
                fields=[
                    FieldSpec(
                        key="status",
                        label="Status",
                        field_type="text-multiple-choice",
                        required=required,
                        properties={"choices": ["Ready", "Complete"]},
                    )
                ],
            )
        ],
    )


class StatefulQuickbase:
    def __init__(self, *, existing: bool):
        self.app = {"id": APP_ID, "name": "Operations", "description": ""} if existing else None
        self.tables = [{"id": TABLE_ID, "name": "Projects", "description": ""}] if existing else []
        self.fields = (
            {
                TABLE_ID: [
                    {
                        "id": 6,
                        "label": "Status",
                        "fieldType": "text-multiple-choice",
                        "required": False,
                        "properties": {"choices": ["Ready", "Complete"]},
                    }
                ]
            }
            if existing
            else {}
        )
        self.ignore_updates = False
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        *,
        method,
        path,
        params=None,
        json_body=None,
        response_type=dict,
        **_,
    ):
        self.calls.append(
            {"method": method, "path": path, "params": params, "json_body": json_body}
        )
        if method == "GET":
            result = self._get(path, params)
        elif method == "POST":
            result = self._post(path, params, json_body)
        else:
            raise AssertionError(f"Unexpected request: {method} {path}")
        copied = deepcopy(result)
        assert isinstance(copied, response_type)
        return copied

    def _get(self, path, params):
        if path == f"apps/{APP_ID}":
            assert self.app is not None
            return self.app
        if path == "tables":
            assert params == {"appId": APP_ID}
            return self.tables
        if path == "fields":
            return self.fields[params["tableId"]]
        if path.endswith("/relationships"):
            return {"relationships": []}
        raise AssertionError(f"Unexpected GET {path}")

    def _post(self, path, params, body):
        if path == "apps":
            self.app = {"id": APP_ID, "name": body["name"], "description": ""}
            self._update_app(body)
            return self.app
        if path == f"apps/{APP_ID}":
            if not self.ignore_updates:
                self._update_app(body)
            return self.app
        if path == "tables":
            table = {"id": TABLE_ID, "name": body["name"], "description": ""}
            self._update_table(table, body)
            self.tables.append(table)
            self.fields[TABLE_ID] = []
            return table
        if path == f"tables/{TABLE_ID}":
            table = self.tables[0]
            if not self.ignore_updates:
                self._update_table(table, body)
            return table
        if path == "fields":
            field_id = 6 + len(self.fields[TABLE_ID])
            field = {"id": field_id, "properties": {}, **body}
            if "formula" in field["properties"]:
                field["mode"] = "formula"
                if field["fieldType"] == "datetime":
                    field["fieldType"] = "timestamp"
            self.fields[TABLE_ID].append(field)
            return field
        if path.startswith("fields/"):
            field_id = int(path.split("/")[1])
            field = next(item for item in self.fields[TABLE_ID] if item["id"] == field_id)
            if not self.ignore_updates:
                properties = body.get("properties")
                field.update({name: value for name, value in body.items() if name != "properties"})
                if properties is not None:
                    field["properties"].update(properties)
            return field
        raise AssertionError(f"Unexpected POST {path}")

    def _update_app(self, body):
        assert self.app is not None
        for request_name, response_name in (
            ("name", "name"),
            ("description", "description"),
            ("variables", "variables"),
            ("securityProperties", "securityProperties"),
        ):
            if request_name in body:
                self.app[response_name] = deepcopy(body[request_name])

    @staticmethod
    def _update_table(table, body):
        for request_name, response_name in (
            ("name", "name"),
            ("description", "description"),
            ("singleRecordName", "singleRecordName"),
            ("pluralRecordName", "pluralRecordName"),
        ):
            if request_name in body:
                table[response_name] = body[request_name]


def client_for(api: StatefulQuickbase, *, configured: bool) -> QuickBaseClient:
    client = QuickBaseClient.__new__(QuickBaseClient)
    client.meta = SimpleNamespace(
        app_ids={"Operations": APP_ID} if configured else {},
        invalidate_tables=Mock(),
    )
    client._request = Mock(side_effect=api.request)
    return client


def write_bound_state(path: Path, *, serial: int = 2) -> SchemaState:
    state = SchemaState(
        lineage="12345678-1234-5678-1234-567812345678",
        serial=serial,
        resources=[
            StateResource(
                address="apps.operations", kind="app", remote_id=APP_ID, name="Operations"
            ),
            StateResource(
                address="apps.operations.tables.projects",
                kind="table",
                remote_id=TABLE_ID,
                name="Projects",
            ),
            StateResource(
                address="apps.operations.tables.projects.fields.status",
                kind="field",
                remote_id=6,
                name="Status",
                attributes={"field_type": "text-multiple-choice"},
            ),
        ],
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state.to_dict()), encoding="utf-8")
    return state


def mutation_calls(api: StatefulQuickbase) -> list[dict[str, object]]:
    return [call for call in api.calls if call["method"] != "GET"]


def test_apply_creates_app_table_and_field_then_publishes_verified_state(tmp_path):
    api = StatefulQuickbase(existing=False)
    client = client_for(api, configured=False)
    state_path = tmp_path / ".qbvisor" / "state.json"
    spec = application_spec(required=True)
    plan = client.plan_app(spec, state_path=state_path)

    result = client.apply_app(plan)

    assert [call["path"] for call in mutation_calls(api)] == [
        "apps",
        "tables",
        "fields",
        "fields/6",
    ]
    assert mutation_calls(api)[2]["json_body"] == {
        "label": "Status",
        "fieldType": "text-multiple-choice",
        "properties": {"choices": ["Ready", "Complete"]},
    }
    assert mutation_calls(api)[3]["json_body"] == {"required": True}
    assert result.quickbase_change_count == 3
    assert result.state_written
    assert result.state.serial == 1
    assert len(result.state.resources) == 3
    assert result.verification.quickbase_change_count == 0
    assert result.verification.state_change_count == 0
    assert SchemaState.from_dict(json.loads(state_path.read_text())) == result.state
    assert not list(state_path.parent.glob("*.tmp"))


def test_apply_imports_exact_matches_without_posting_to_quickbase(tmp_path):
    api = StatefulQuickbase(existing=True)
    client = client_for(api, configured=True)
    state_path = tmp_path / "state.json"
    plan = client.plan_app(application_spec(required=False), state_path=state_path)

    result = client.apply_app(plan)

    assert mutation_calls(api) == []
    assert result.state_written
    assert result.state.serial == 1
    assert result.verification.action_counts["unchanged"] == 3
    client.meta.invalidate_tables.assert_not_called()


def test_apply_updates_only_planned_managed_attributes_and_advances_state(tmp_path):
    api = StatefulQuickbase(existing=True)
    client = client_for(api, configured=True)
    state_path = tmp_path / "state.json"
    previous = write_bound_state(state_path)
    spec = application_spec(
        description="Managed app",
        table_description="Managed table",
        required=True,
    )
    plan = client.plan_app(spec, state_path=state_path)

    result = client.apply_app(plan)

    posts = mutation_calls(api)
    assert [(call["path"], call["json_body"]) for call in posts] == [
        (f"apps/{APP_ID}", {"description": "Managed app"}),
        (f"tables/{TABLE_ID}", {"description": "Managed table"}),
        ("fields/6", {"required": True}),
    ]
    assert result.state.lineage == previous.lineage
    assert result.state.serial == previous.serial + 1
    assert result.verification.can_apply
    client.meta.invalidate_tables.assert_called_once_with(APP_ID)


def test_apply_creates_and_updates_formula_fields_with_quickbase_syntax(tmp_path):
    api = StatefulQuickbase(existing=False)
    client = client_for(api, configured=False)
    state_path = tmp_path / "state.json"

    def formula_spec(expression: str) -> AppSpec:
        return AppSpec(
            key="operations",
            name="Operations",
            tables=[
                TableSpec(
                    key="projects",
                    name="Projects",
                    fields=[
                        FieldSpec(key="quantity", label="Quantity", field_type="numeric"),
                        FieldSpec(key="rate", label="Rate", field_type="currency"),
                        FieldSpec(
                            key="total",
                            label="Total",
                            field_type="currency",
                            formula=FormulaSpec(
                                expression=expression,
                                depends_on=(
                                    "tables.projects.fields.quantity",
                                    "tables.projects.fields.rate",
                                ),
                            ),
                            properties={"decimalPlaces": 2},
                        ),
                    ],
                )
            ],
        )

    initial = client.apply_app(
        client.plan_app(formula_spec("[Quantity] * [Rate]"), state_path=state_path)
    )
    create_calls = [call for call in mutation_calls(api) if call["path"] == "fields"]

    assert [call["json_body"]["label"] for call in create_calls] == [
        "Quantity",
        "Rate",
        "Total",
    ]
    assert create_calls[-1]["json_body"] == {
        "label": "Total",
        "fieldType": "currency",
        "properties": {
            "decimalPlaces": 2,
            "formula": "[Quantity] * [Rate]",
        },
    }
    total_resource = initial.state.resource("apps.operations.tables.projects.fields.total")
    assert total_resource is not None
    assert total_resource.attributes == {"field_type": "currency", "mode": "formula"}

    api.calls.clear()
    updated = client.apply_app(
        client.plan_app(formula_spec("[Quantity] * [Rate] * 2"), state_path=state_path)
    )

    assert mutation_calls(api) == [
        {
            "method": "POST",
            "path": "fields/8",
            "params": {"tableId": TABLE_ID},
            "json_body": {"properties": {"formula": "[Quantity] * [Rate] * 2"}},
        }
    ]
    assert updated.verification.quickbase_change_count == 0


def test_apply_rejects_a_stale_plan_before_mutating_or_rewriting_state(tmp_path):
    api = StatefulQuickbase(existing=True)
    client = client_for(api, configured=False)
    state_path = tmp_path / "state.json"
    before = write_bound_state(state_path)
    plan = client.plan_app(application_spec(), state_path=state_path)
    api.app["name"] = "Changed Outside Plan"

    with pytest.raises(QuickbaseSchemaStalePlanError, match="review a new plan"):
        client.apply_app(plan)

    assert mutation_calls(api) == []
    assert SchemaState.from_dict(json.loads(state_path.read_text())) == before


def test_failed_convergence_preserves_previous_state_file(tmp_path):
    api = StatefulQuickbase(existing=True)
    api.ignore_updates = True
    client = client_for(api, configured=False)
    state_path = tmp_path / "state.json"
    previous = write_bound_state(state_path)
    plan = client.plan_app(application_spec(required=True), state_path=state_path)

    with pytest.raises(QuickbaseSchemaApplyError, match="did not converge"):
        client.apply_app(plan)

    assert [call["path"] for call in mutation_calls(api)] == ["fields/6"]
    assert SchemaState.from_dict(json.loads(state_path.read_text())) == previous
    assert not list(tmp_path.glob("*.tmp"))


def test_no_op_apply_does_not_rewrite_or_increment_existing_state(tmp_path):
    api = StatefulQuickbase(existing=True)
    client = client_for(api, configured=False)
    state_path = tmp_path / "state.json"
    previous = write_bound_state(state_path)
    before_bytes = state_path.read_bytes()
    plan = client.plan_app(application_spec(required=False), state_path=state_path)

    result = client.apply_app(plan)

    assert not result.state_written
    assert result.state == previous
    assert state_path.read_bytes() == before_bytes
    assert mutation_calls(api) == []


def test_conflicted_plan_is_rejected_before_creating_state_artifacts(tmp_path):
    api = StatefulQuickbase(existing=True)
    api.fields[TABLE_ID][0]["fieldType"] = "numeric"
    client = client_for(api, configured=True)
    state_path = tmp_path / ".qbvisor" / "state.json"
    plan = client.plan_app(application_spec(), state_path=state_path)

    with pytest.raises(QuickbaseSchemaConflictError, match="contains conflicts"):
        client.apply_app(plan)

    assert not state_path.parent.exists()
    assert mutation_calls(api) == []


def test_apply_rejects_concurrent_use_of_the_same_state_file(tmp_path):
    api = StatefulQuickbase(existing=True)
    client = client_for(api, configured=True)
    state_path = tmp_path / "state.json"
    plan = client.plan_app(application_spec(required=False), state_path=state_path)

    with SchemaStateLock(state_path):
        with pytest.raises(QuickbaseSchemaLockError, match="Could not acquire"):
            client.apply_app(plan)

    assert mutation_calls(api) == []
    assert not state_path.exists()
