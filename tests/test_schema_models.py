from copy import deepcopy

import pytest

from qbvisor import (
    AppSpec,
    FieldSpec,
    FormulaSpec,
    RelationshipSpec,
    SchemaState,
    StateResource,
    SummaryFieldSpec,
    TableSpec,
)


def application_spec() -> AppSpec:
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
                fields=[
                    FieldSpec(key="hours", label="Hours", field_type="numeric"),
                ],
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
                    )
                ],
            )
        ],
    )


def test_declarative_specs_normalize_sequences_and_build_stable_addresses():
    spec = application_spec()

    assert isinstance(spec.tables, tuple)
    assert isinstance(spec.tables[0].fields, tuple)
    assert spec.address == "apps.operations"
    assert spec.tables[0].address(spec.key) == "apps.operations.tables.projects"
    assert spec.tables[0].fields[0].address(spec.key, spec.tables[0].key) == (
        "apps.operations.tables.projects.fields.name"
    )
    assert spec.relationships[0].address(spec.key) == (
        "apps.operations.relationships.project_details"
    )
    assert spec.relationships[0].lookup_address(spec.key, "name") == (
        "apps.operations.relationships.project_details.lookups.name"
    )
    assert (
        spec.relationships[0].summary_fields[0].address(spec.key, spec.relationships[0].key)
        == "apps.operations.relationships.project_details.summaries.total_hours"
    )


def test_field_properties_are_frozen_json_values():
    choices = ["Ready", "Complete"]
    field = FieldSpec(
        key="status",
        label="Status",
        field_type="text-multiple-choice",
        properties={"choices": choices},
    )
    choices.append("Cancelled")

    assert field.properties == {"choices": ("Ready", "Complete")}
    assert field.properties is not None
    with pytest.raises(TypeError):
        field.properties["choices"] = ()  # type: ignore[index]


def test_formula_specs_preserve_quickbase_syntax_and_normalize_dependencies():
    formula = FormulaSpec(
        expression="// calculate invoice amount\n[Quantity] * [Unit Price]\n",
        depends_on=(
            "tables.invoices.fields.quantity",
            "tables.invoices.fields.unit_price",
        ),
    )
    field = FieldSpec(
        key="amount",
        label="Amount",
        field_type="currency",
        formula=formula,
        properties={"decimalPlaces": 2},
    )

    assert formula.expression == "// calculate invoice amount\n[Quantity] * [Unit Price]"
    assert formula.depends_on == (
        "tables.invoices.fields.quantity",
        "tables.invoices.fields.unit_price",
    )
    assert field.formula is formula


@pytest.mark.parametrize(
    "field_type",
    [
        "text",
        "rich-text",
        "numeric",
        "currency",
        "rating",
        "percent",
        "date",
        "datetime",
        "timeofday",
        "duration",
        "checkbox",
        "phone",
        "email",
        "url",
        "user",
        "multitext",
    ],
)
def test_formula_specs_accept_live_verified_json_field_types(field_type):
    field = FieldSpec(
        key="derived",
        label="Derived",
        field_type=field_type,
        formula=FormulaSpec(expression="1"),
    )

    assert field.formula is not None


@pytest.mark.parametrize(
    "field_type",
    [
        "text-multiple-choice",
        "text-multi-line",
        "multiuser",
        "address",
        "file",
        "timestamp",
        "workdate",
    ],
)
def test_formula_specs_reject_field_types_not_supported_by_json_api(field_type):
    with pytest.raises(ValueError, match="JSON formula fields do not support"):
        FieldSpec(
            key="derived",
            label="Derived",
            field_type=field_type,
            formula=FormulaSpec(expression="1"),
        )


def test_formula_specs_reject_invalid_expressions_dependencies_and_property_escape_hatch():
    with pytest.raises(ValueError, match="non-empty"):
        FormulaSpec(expression="  \n")
    with pytest.raises(ValueError, match="102,400"):
        FormulaSpec(expression="x" * 102_401)
    with pytest.raises(ValueError, match="must be a sequence"):
        FormulaSpec(expression="1", depends_on="tables.items.fields.amount")
    with pytest.raises(ValueError, match="table field, relationship, lookup, or summary"):
        FormulaSpec(expression="1", depends_on=("fields.amount",))
    with pytest.raises(ValueError, match="duplicate"):
        FormulaSpec(
            expression="1",
            depends_on=("tables.items.fields.amount", "tables.items.fields.amount"),
        )
    with pytest.raises(ValueError, match="FormulaSpec"):
        FieldSpec(
            key="derived",
            label="Derived",
            field_type="numeric",
            formula="1 + 1",  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="not properties"):
        FieldSpec(
            key="derived",
            label="Derived",
            field_type="numeric",
            formula=FormulaSpec(expression="1 + 1"),
            properties={"formula": "1 + 1"},
        )


def test_app_specs_validate_formula_dependency_addresses():
    valid = AppSpec(
        key="billing",
        name="Billing",
        tables=[
            TableSpec(
                key="invoices",
                name="Invoices",
                fields=[
                    FieldSpec(key="quantity", label="Quantity", field_type="numeric"),
                    FieldSpec(
                        key="amount",
                        label="Amount",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression="[Quantity] * 2",
                            depends_on=("tables.invoices.fields.quantity",),
                        ),
                    ),
                ],
            )
        ],
    )
    assert valid.tables[0].fields[1].formula is not None

    with pytest.raises(ValueError, match="unknown schema dependencies"):
        AppSpec(
            key="billing",
            name="Billing",
            tables=[
                TableSpec(
                    key="invoices",
                    name="Invoices",
                    fields=[
                        FieldSpec(
                            key="amount",
                            label="Amount",
                            field_type="numeric",
                            formula=FormulaSpec(
                                expression="[Missing] * 2",
                                depends_on=("tables.invoices.fields.missing",),
                            ),
                        )
                    ],
                )
            ],
        )


@pytest.mark.parametrize(
    "key",
    ["Operations", "project-status", "2projects", "project status", ""],
)
def test_resource_keys_are_safe_stable_identifiers(key):
    with pytest.raises(ValueError, match="lowercase letter"):
        FieldSpec(key=key, label="Status", field_type="text")


def test_specs_reject_ambiguous_names_and_invalid_relationship_references():
    duplicate_names = [
        TableSpec(key="projects", name="Projects"),
        TableSpec(key="archived", name="projects"),
    ]
    with pytest.raises(ValueError, match="names must be unique ignoring case"):
        AppSpec(key="operations", name="Operations", tables=duplicate_names)

    with pytest.raises(ValueError, match="unknown parent lookup fields"):
        AppSpec(
            key="operations",
            name="Operations",
            tables=application_spec().tables,
            relationships=[
                RelationshipSpec(
                    key="project_details",
                    parent_table="projects",
                    child_table="details",
                    lookup_fields=["missing"],
                )
            ],
        )


def test_relationship_generated_fields_cannot_collide_with_declared_child_fields():
    projects, details = application_spec().tables
    conflicting_details = TableSpec(
        key=details.key,
        name=details.name,
        fields=[
            *details.fields,
            FieldSpec(key="related_project", label="Related Project", field_type="text"),
        ],
    )

    with pytest.raises(ValueError, match="generated field labels collide"):
        AppSpec(
            key="operations",
            name="Operations",
            tables=[projects, conflicting_details],
            relationships=application_spec().relationships,
        )


def test_summary_fields_enforce_quickbase_accumulation_requirements():
    with pytest.raises(ValueError, match="SUM summary fields require"):
        SummaryFieldSpec(key="total_hours", accumulation_type="SUM")
    with pytest.raises(ValueError, match="COUNT summary fields must omit"):
        SummaryFieldSpec(key="detail_count", accumulation_type="COUNT", field="hours")


def test_schema_state_round_trips_stable_resource_bindings():
    state = SchemaState(
        lineage="12345678-1234-5678-1234-567812345678",
        serial=3,
        resources=[
            StateResource(
                address="apps.operations.tables.projects.fields.status",
                kind="field",
                remote_id=12,
                name="Project Status",
                attributes={"field_type": "text", "properties": {"choices": ["Active"]}},
            ),
            StateResource(
                address="apps.operations",
                kind="app",
                remote_id="bp7example",
                name="Operations",
            ),
        ],
    )

    restored = SchemaState.from_dict(state.to_dict())

    assert restored == state
    assert restored.resources[0].address == "apps.operations"
    assert restored.resource("apps.operations.tables.projects.fields.status").remote_id == 12
    assert restored.resource("apps.operations.tables.projects.fields.status").attributes == {
        "field_type": "text",
        "properties": {"choices": ("Active",)},
    }


def test_schema_state_rejects_version_drift_and_mismatched_resource_addresses():
    payload = SchemaState().to_dict()
    future = deepcopy(payload)
    future["format_version"] = 2
    with pytest.raises(ValueError, match="Unsupported schema state version"):
        SchemaState.from_dict(future)

    with pytest.raises(ValueError, match="does not match kind field"):
        StateResource(
            address="apps.operations.tables.projects",
            kind="field",
            remote_id=6,
            name="Project Name",
        )
