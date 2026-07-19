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
    QuickbaseSchemaStateError,
    RelationshipSpec,
    SchemaState,
    StateResource,
    SummaryFieldSpec,
    TableSpec,
)

APP_ID = "app_operations"
PROJECTS_ID = "tbl_projects"
DETAILS_ID = "tbl_details"


def application_spec(*, summary_where: str | None = None) -> AppSpec:
    return AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[
                    FieldSpec(key="name", label="Project Name", field_type="text"),
                    FieldSpec(key="budget", label="Budget", field_type="numeric"),
                ],
            ),
            TableSpec(
                key="details",
                name="Project Details",
                fields=[FieldSpec(key="hours", label="Hours", field_type="numeric")],
            ),
        ],
        relationships=[
            RelationshipSpec(
                key="project_details",
                parent_table="projects",
                child_table="details",
                foreign_key_label="Related Project",
                lookup_fields=["name"],
                summary_fields=[
                    SummaryFieldSpec(
                        key="total_hours",
                        accumulation_type="SUM",
                        field="hours",
                        label="Total Hours",
                        where=summary_where,
                    )
                ],
            )
        ],
    )


def formula_dependency_spec() -> AppSpec:
    return AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="parents",
                name="Parents",
                fields=[
                    FieldSpec(key="base", label="Base", field_type="numeric"),
                    FieldSpec(
                        key="rate",
                        label="Rate",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression="[Base] * 2",
                            depends_on=("tables.parents.fields.base",),
                        ),
                    ),
                    FieldSpec(
                        key="final",
                        label="Final",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression="[Total Derived] + 1",
                            depends_on=("relationships.parent_children.summaries.total_derived",),
                        ),
                    ),
                ],
            ),
            TableSpec(
                key="children",
                name="Children",
                fields=[
                    FieldSpec(key="amount", label="Amount", field_type="numeric"),
                    FieldSpec(
                        key="derived_amount",
                        label="Derived Amount",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression="[Amount] * 2",
                            depends_on=("tables.children.fields.amount",),
                        ),
                    ),
                    FieldSpec(
                        key="lookup_plus_one",
                        label="Lookup Plus One",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression="[Parents - Rate] + 1",
                            depends_on=("relationships.parent_children.lookups.rate",),
                        ),
                    ),
                ],
            ),
        ],
        relationships=[
            RelationshipSpec(
                key="parent_children",
                parent_table="parents",
                child_table="children",
                foreign_key_label="Related Parent",
                lookup_fields=("rate",),
                summary_fields=(
                    SummaryFieldSpec(
                        key="total_derived",
                        accumulation_type="SUM",
                        field="derived_amount",
                        label="Total Derived",
                    ),
                ),
            )
        ],
    )


def remote_schema() -> dict[str, object]:
    return {
        "app": {"id": APP_ID, "name": "Operations", "description": ""},
        "tables": [
            {"id": PROJECTS_ID, "name": "Projects", "description": ""},
            {"id": DETAILS_ID, "name": "Project Details", "description": ""},
        ],
        "fields": {
            PROJECTS_ID: [
                {"id": 6, "label": "Project Name", "fieldType": "text", "properties": {}},
                {"id": 7, "label": "Budget", "fieldType": "numeric", "properties": {}},
                {
                    "id": 10,
                    "label": "Total Hours",
                    "fieldType": "numeric",
                    "mode": "summary",
                    "properties": {
                        "summaryFunction": "SUM",
                        "summaryTargetFieldId": 8,
                    },
                },
            ],
            DETAILS_ID: [
                {"id": 8, "label": "Hours", "fieldType": "numeric", "properties": {}},
                {
                    "id": 9,
                    "label": "Related Project",
                    "fieldType": "numeric",
                    "properties": {"foreignKey": True},
                },
                {
                    "id": 11,
                    "label": "Projects - Project Name",
                    "fieldType": "text",
                    "mode": "lookup",
                    "properties": {"lookupTargetFieldId": 6},
                },
            ],
        },
        "relationships": {
            DETAILS_ID: [
                {
                    "id": 9,
                    "parentTableId": PROJECTS_ID,
                    "childTableId": DETAILS_ID,
                    "isCrossApp": False,
                    "foreignKeyField": {"id": 9, "label": "Related Project"},
                    "lookupFields": [{"id": 11, "label": "Projects - Project Name"}],
                    "summaryFields": [{"id": 10, "label": "Total Hours"}],
                }
            ]
        },
    }


def fake_client(
    schema: dict[str, object] | None = None,
    *,
    app_ids: dict[str, str] | None = None,
) -> QuickBaseClient:
    remote = schema or remote_schema()
    client = QuickBaseClient.__new__(QuickBaseClient)
    client.meta = SimpleNamespace(app_ids={"Operations": APP_ID} if app_ids is None else app_ids)

    def request(*, method, path, params=None, response_type=dict, **_):
        assert method == "GET"
        if path == f"apps/{APP_ID}":
            result = remote["app"]
        elif path == "tables":
            assert params == {"appId": APP_ID}
            result = remote["tables"]
        elif path == "fields":
            result = remote["fields"][params["tableId"]]
        elif path.endswith("/relationships"):
            table_id = path.split("/")[1]
            result = {"relationships": remote["relationships"].get(table_id, [])}
        else:
            raise AssertionError(f"Unexpected request: {method} {path}")
        copied = deepcopy(result)
        assert isinstance(copied, response_type)
        return copied

    client._request = Mock(side_effect=request)
    return client


def write_state(path: Path, resources: list[StateResource]) -> SchemaState:
    state = SchemaState(
        lineage="12345678-1234-5678-1234-567812345678",
        serial=4,
        resources=resources,
    )
    path.write_text(json.dumps(state.to_dict()), encoding="utf-8")
    return state


def complete_state(path: Path) -> SchemaState:
    return write_state(
        path,
        [
            StateResource(
                address="apps.operations", kind="app", remote_id=APP_ID, name="Operations"
            ),
            StateResource(
                address="apps.operations.tables.projects",
                kind="table",
                remote_id=PROJECTS_ID,
                name="Projects",
            ),
            StateResource(
                address="apps.operations.tables.details",
                kind="table",
                remote_id=DETAILS_ID,
                name="Project Details",
            ),
            StateResource(
                address="apps.operations.tables.projects.fields.name",
                kind="field",
                remote_id=6,
                name="Project Name",
            ),
            StateResource(
                address="apps.operations.tables.projects.fields.budget",
                kind="field",
                remote_id=7,
                name="Budget",
            ),
            StateResource(
                address="apps.operations.tables.details.fields.hours",
                kind="field",
                remote_id=8,
                name="Hours",
            ),
            StateResource(
                address="apps.operations.relationships.project_details",
                kind="relationship",
                remote_id=9,
                name="Related Project",
            ),
            StateResource(
                address="apps.operations.relationships.project_details.lookups.name",
                kind="lookup",
                remote_id=11,
                name="Projects - Project Name",
            ),
            StateResource(
                address="apps.operations.relationships.project_details.summaries.total_hours",
                kind="summary",
                remote_id=10,
                name="Total Hours",
                attributes={"where": None},
            ),
        ],
    )


def test_first_plan_imports_exact_matches_without_mutating_quickbase_or_state(tmp_path):
    state_path = tmp_path / "state.json"
    client = fake_client()

    first = client.plan_app(application_spec(), state_path=state_path)
    second = client.plan_app(application_spec(), state_path=state_path)

    assert first.to_dict() == second.to_dict()
    assert first.can_apply
    assert first.quickbase_change_count == 0
    assert first.state_change_count == 9
    assert first.action_counts == {
        "create": 0,
        "update": 0,
        "unchanged": 9,
        "conflict": 0,
    }
    assert all(change.state_action == "bind" for change in first.changes)
    assert not state_path.exists()
    assert all(call.kwargs["method"] == "GET" for call in client._request.call_args_list)


def test_state_ids_remain_authoritative_across_app_table_and_field_renames(tmp_path):
    state_path = tmp_path / "state.json"
    write_state(
        state_path,
        [
            StateResource(
                address="apps.operations", kind="app", remote_id=APP_ID, name="Old Operations"
            ),
            StateResource(
                address="apps.operations.tables.projects",
                kind="table",
                remote_id=PROJECTS_ID,
                name="Old Projects",
            ),
            StateResource(
                address="apps.operations.tables.projects.fields.name",
                kind="field",
                remote_id=6,
                name="Old Project Name",
            ),
        ],
    )
    remote = remote_schema()
    remote["app"]["name"] = "Old Operations"
    remote["tables"] = [{"id": PROJECTS_ID, "name": "Old Projects"}]
    remote["fields"] = {
        PROJECTS_ID: [{"id": 6, "label": "Old Project Name", "fieldType": "text", "properties": {}}]
    }
    remote["relationships"] = {}
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[FieldSpec(key="name", label="Project Name", field_type="text")],
            )
        ],
    )

    plan = fake_client(remote, app_ids={}).plan_app(spec, state_path=state_path)

    changes = {change.address: change for change in plan.changes}
    assert changes["apps.operations"].action == "update"
    assert changes["apps.operations.tables.projects"].action == "update"
    assert changes["apps.operations.tables.projects.fields.name"].action == "update"
    assert all(change.state_action == "none" for change in changes.values())
    assert plan.can_apply


def test_app_key_can_bind_a_friendly_qb_app_ids_alias(tmp_path):
    remote = remote_schema()
    remote["app"]["name"] = "Dev Sandbox - QBVisor Test"
    spec = AppSpec(key="sandbox", name="Dev Sandbox - QBVisor Test")

    plan = fake_client(remote, app_ids={"Sandbox": APP_ID}).plan_app(
        spec,
        state_path=tmp_path / "missing.json",
    )

    assert len(plan.changes) == 1
    assert plan.changes[0].action == "unchanged"
    assert plan.changes[0].state_action == "bind"
    assert plan.changes[0].remote_id == APP_ID


def test_missing_state_bound_field_is_a_drift_conflict_not_a_replacement(tmp_path):
    state_path = tmp_path / "state.json"
    write_state(
        state_path,
        [
            StateResource(
                address="apps.operations", kind="app", remote_id=APP_ID, name="Operations"
            ),
            StateResource(
                address="apps.operations.tables.projects",
                kind="table",
                remote_id=PROJECTS_ID,
                name="Projects",
            ),
            StateResource(
                address="apps.operations.tables.projects.fields.name",
                kind="field",
                remote_id=99,
                name="Project Name",
            ),
        ],
    )
    remote = remote_schema()
    remote["tables"] = [{"id": PROJECTS_ID, "name": "Projects"}]
    remote["fields"] = {PROJECTS_ID: []}
    remote["relationships"] = {}
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[FieldSpec(key="name", label="Project Name", field_type="text")],
            )
        ],
    )

    plan = fake_client(remote).plan_app(spec, state_path=state_path)
    field_change = next(change for change in plan.changes if change.kind == "field")

    assert field_change.action == "conflict"
    assert "no longer returns that ID" in field_change.reason
    assert not plan.can_apply
    assert not any(change.action == "create" for change in plan.changes)


def test_field_type_changes_are_explicit_conflicts(tmp_path):
    remote = remote_schema()
    remote["tables"] = [{"id": PROJECTS_ID, "name": "Projects"}]
    remote["fields"] = {
        PROJECTS_ID: [{"id": 6, "label": "Project Name", "fieldType": "numeric", "properties": {}}]
    }
    remote["relationships"] = {}
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[FieldSpec(key="name", label="Project Name", field_type="text")],
            )
        ],
    )

    plan = fake_client(remote).plan_app(spec, state_path=tmp_path / "missing.json")

    field_change = next(change for change in plan.changes if change.kind == "field")
    assert field_change.action == "conflict"
    assert "field types are immutable" in field_change.reason


def test_json_array_properties_compare_idempotently_and_unmanaged_values_are_ignored(
    tmp_path,
):
    remote = remote_schema()
    remote["tables"] = [{"id": PROJECTS_ID, "name": "Projects"}]
    remote["fields"] = {
        PROJECTS_ID: [
            {
                "id": 6,
                "label": "Status",
                "fieldType": "text-multiple-choice",
                "required": False,
                "properties": {
                    "choices": ["Ready", "Complete"],
                    "allowNewChoices": True,
                },
            }
        ]
    }
    remote["relationships"] = {}
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[
                    FieldSpec(
                        key="status",
                        label="Status",
                        field_type="text-multiple-choice",
                        required=False,
                        properties={"choices": ["Ready", "Complete"]},
                    )
                ],
            )
        ],
    )

    plan = fake_client(remote).plan_app(spec, state_path=tmp_path / "missing.json")
    field_change = next(change for change in plan.changes if change.kind == "field")

    assert field_change.action == "unchanged"
    assert field_change.attributes == ()
    assert field_change.state_action == "bind"


def test_missing_app_plans_the_complete_schema_without_network_calls(tmp_path):
    client = fake_client(app_ids={})

    plan = client.plan_app(application_spec(), state_path=tmp_path / "missing.json")

    assert plan.can_apply
    assert plan.action_counts == {
        "create": 9,
        "update": 0,
        "unchanged": 0,
        "conflict": 0,
    }
    client._request.assert_not_called()


def test_formula_fields_plan_remote_mode_type_normalization_and_exact_text(tmp_path):
    remote = remote_schema()
    remote["tables"] = [{"id": PROJECTS_ID, "name": "Projects"}]
    remote["fields"] = {
        PROJECTS_ID: [
            {"id": 6, "label": "Quantity", "fieldType": "numeric", "properties": {}},
            {
                "id": 7,
                "label": "Calculated At",
                "fieldType": "timestamp",
                "mode": "formula",
                "properties": {"formula": "Now()"},
            },
        ]
    }
    remote["relationships"] = {}
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[
                    FieldSpec(key="quantity", label="Quantity", field_type="numeric"),
                    FieldSpec(
                        key="calculated_at",
                        label="Calculated At",
                        field_type="datetime",
                        formula=FormulaSpec(expression="Now()\n"),
                    ),
                ],
            )
        ],
    )

    plan = fake_client(remote).plan_app(spec, state_path=tmp_path / "missing.json")
    formula_change = next(
        change for change in plan.changes if change.address.endswith("calculated_at")
    )

    assert formula_change.action == "unchanged"
    assert formula_change.state_action == "bind"
    assert plan.execution_order == (
        "apps.operations.tables.projects.fields.quantity",
        "apps.operations.tables.projects.fields.calculated_at",
    )


def test_formula_drift_is_reviewable_and_formula_queries_warn_about_performance(tmp_path):
    remote = remote_schema()
    remote["tables"] = [{"id": PROJECTS_ID, "name": "Projects"}]
    remote["fields"] = {
        PROJECTS_ID: [
            {
                "id": 6,
                "label": "Related Count",
                "fieldType": "numeric",
                "mode": "formula",
                "properties": {"formula": "1"},
            }
        ]
    }
    remote["relationships"] = {}
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[
                    FieldSpec(
                        key="related_count",
                        label="Related Count",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression='Size(GetRecords("{3.GT.0}"))',
                        ),
                    )
                ],
            )
        ],
    )

    plan = fake_client(remote).plan_app(spec, state_path=tmp_path / "missing.json")
    formula_change = next(change for change in plan.changes if change.kind == "field")

    assert formula_change.action == "update"
    assert formula_change.attributes[0].name == "formula.expression"
    assert formula_change.attributes[0].before == "1"
    assert formula_change.attributes[0].after == 'Size(GetRecords("{3.GT.0}"))'
    assert "performance" in formula_change.warnings[0]
    assert "warning:" in str(plan)


@pytest.mark.parametrize(
    ("remote_mode", "formula"),
    [
        ("", FormulaSpec(expression="1")),
        ("formula", None),
    ],
)
def test_scalar_and_formula_modes_cannot_be_repurposed(tmp_path, remote_mode, formula):
    remote = remote_schema()
    remote["tables"] = [{"id": PROJECTS_ID, "name": "Projects"}]
    remote["fields"] = {
        PROJECTS_ID: [
            {
                "id": 6,
                "label": "Amount",
                "fieldType": "numeric",
                "mode": remote_mode,
                "properties": {"formula": "1"} if remote_mode == "formula" else {},
            }
        ]
    }
    remote["relationships"] = {}
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[
                    FieldSpec(
                        key="amount",
                        label="Amount",
                        field_type="numeric",
                        formula=formula,
                    )
                ],
            )
        ],
    )

    plan = fake_client(remote).plan_app(spec, state_path=tmp_path / "missing.json")
    field_change = next(change for change in plan.changes if change.kind == "field")

    assert field_change.action == "conflict"
    assert "field modes cannot be converted" in field_change.reason


def test_formula_relationship_dependencies_have_a_deterministic_safe_order(tmp_path):
    plan = fake_client(app_ids={}).plan_app(
        formula_dependency_spec(),
        state_path=tmp_path / "missing.json",
    )
    positions = {address: index for index, address in enumerate(plan.execution_order)}
    relationship = "apps.operations.relationships.parent_children"
    rate = "apps.operations.tables.parents.fields.rate"
    lookup = f"{relationship}.lookups.rate"
    lookup_formula = "apps.operations.tables.children.fields.lookup_plus_one"
    derived = "apps.operations.tables.children.fields.derived_amount"
    summary = f"{relationship}.summaries.total_derived"
    final = "apps.operations.tables.parents.fields.final"

    assert plan.can_apply
    assert positions["apps.operations.tables.parents.fields.base"] < positions[rate]
    assert positions[relationship] < positions[lookup]
    assert positions[rate] < positions[lookup] < positions[lookup_formula]
    assert positions[derived] < positions[summary] < positions[final]


def test_formula_dependency_cycles_are_conflicts_before_mutation(tmp_path):
    spec = AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="parents",
                name="Parents",
                fields=[
                    FieldSpec(
                        key="rate",
                        label="Rate",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression="[Children - Rate] + 1",
                            depends_on=("relationships.parent_children.lookups.rate",),
                        ),
                    )
                ],
            ),
            TableSpec(key="children", name="Children"),
        ],
        relationships=[
            RelationshipSpec(
                key="parent_children",
                parent_table="parents",
                child_table="children",
                lookup_fields=("rate",),
            )
        ],
    )

    plan = fake_client(app_ids={}).plan_app(spec, state_path=tmp_path / "missing.json")
    conflicts = [change for change in plan.changes if change.action == "conflict"]

    assert not plan.can_apply
    assert {change.kind for change in conflicts} == {"field", "lookup"}
    assert all("dependency cycle" in change.reason for change in conflicts)


def test_filtered_summary_import_is_blocked_when_quickbase_cannot_verify_filter(tmp_path):
    plan = fake_client().plan_app(
        application_spec(summary_where="{8.GT.0}"),
        state_path=tmp_path / "missing.json",
    )

    summary = next(change for change in plan.changes if change.kind == "summary")
    assert summary.action == "conflict"
    assert "does not expose the existing summary filter" in summary.reason
    assert not plan.can_apply


def test_existing_state_preserves_filtered_summary_idempotency(tmp_path):
    state_path = tmp_path / "state.json"
    state = complete_state(state_path)
    resources = [
        StateResource(
            address=resource.address,
            kind=resource.kind,
            remote_id=resource.remote_id,
            name=resource.name,
            attributes={"where": "{8.GT.0}"} if resource.kind == "summary" else resource.attributes,
        )
        for resource in state.resources
    ]
    write_state(state_path, resources)

    plan = fake_client().plan_app(
        application_spec(summary_where="{8.GT.0}"),
        state_path=state_path,
    )

    summary = next(change for change in plan.changes if change.kind == "summary")
    assert summary.action == "unchanged"
    assert plan.can_apply


def test_invalid_state_fails_before_any_quickbase_request(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text("not json", encoding="utf-8")
    client = fake_client()

    with pytest.raises(QuickbaseSchemaStateError, match="Invalid schema state"):
        client.plan_app(application_spec(), state_path=state_path)

    client._request.assert_not_called()
