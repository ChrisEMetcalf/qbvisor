from __future__ import annotations

import os
import uuid
from pathlib import Path
from typing import Any, cast

import pytest
from conftest import (
    APP_NAME,
    DETAILS_TABLE_NAME,
    MUTATION_ENV,
    RECORDS_TABLE_NAME,
    SandboxConfig,
    SandboxContract,
)

from qbvisor import (
    AppSpec,
    FieldSpec,
    FormulaSpec,
    QuickBaseClient,
    RelationshipSpec,
    SummaryFieldSpec,
    TableSpec,
)
from qbvisor.transport import JSONValue, QuickBaseTransport

pytestmark = pytest.mark.integration


def _objects(payload: JSONValue) -> list[dict[str, Any]]:
    assert isinstance(payload, list)
    assert all(isinstance(item, dict) for item in payload)
    return cast(list[dict[str, Any]], payload)


def _object(payload: JSONValue) -> dict[str, Any]:
    assert isinstance(payload, dict)
    return cast(dict[str, Any], payload)


def _app_name(client: QuickBaseClient) -> str:
    app = client.get_app(APP_NAME)
    name = app.get("name")
    assert isinstance(name, str) and name
    return name


def persistent_spec(client: QuickBaseClient) -> AppSpec:
    return AppSpec(
        key="sandbox",
        name=_app_name(client),
        tables=[
            TableSpec(
                key="contract_records",
                name=RECORDS_TABLE_NAME,
                fields=[
                    FieldSpec(key="fixture_key", label="Fixture Key", field_type="text"),
                    FieldSpec(key="name", label="Name", field_type="text"),
                ],
            ),
            TableSpec(
                key="contract_details",
                name=DETAILS_TABLE_NAME,
                fields=[
                    FieldSpec(key="detail_key", label="Detail Key", field_type="text"),
                    FieldSpec(key="note", label="Note", field_type="text"),
                ],
            ),
        ],
        relationships=[
            RelationshipSpec(
                key="record_details",
                parent_table="contract_records",
                child_table="contract_details",
                foreign_key_label="Related Contract Record",
            )
        ],
    )


def test_persistent_schema_import_converges_without_quickbase_mutations(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    tmp_path: Path,
):
    state_path = tmp_path / ".qbvisor" / "state.json"
    spec = persistent_spec(sandbox_client)

    plan = sandbox_client.plan_app(spec, state_path=state_path)
    result = sandbox_client.apply_app(plan)
    second = sandbox_client.plan_app(spec, state_path=state_path)

    assert plan.quickbase_change_count == 0
    assert plan.state_change_count == 8
    assert result.quickbase_change_count == 0
    assert result.state_written
    assert result.state.serial == 1
    assert second.quickbase_change_count == 0
    assert second.state_change_count == 0
    resources = {resource.address: resource for resource in result.state.resources}
    assert resources["apps.sandbox"].remote_id == sandbox_contract.app_id
    assert resources["apps.sandbox.tables.contract_records"].remote_id == (
        sandbox_contract.records_table_id
    )
    assert resources["apps.sandbox.relationships.record_details"].remote_id == (
        sandbox_contract.relationship_id
    )


@pytest.mark.sandbox_mutation
def test_declarative_schema_create_and_second_apply_are_idempotent(
    sandbox_client: QuickBaseClient,
    sandbox_transport: QuickBaseTransport,
    sandbox_contract: SandboxContract,
    tmp_path: Path,
):
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to run mutation contract tests")

    suffix = uuid.uuid4().hex[:10]
    parent_name = f"qbvisor_decl_parent_{suffix}"
    child_name = f"qbvisor_decl_child_{suffix}"
    state_path = tmp_path / ".qbvisor" / "state.json"
    spec = AppSpec(
        key="sandbox",
        name=_app_name(sandbox_client),
        tables=[
            TableSpec(
                key="parent",
                name=parent_name,
                fields=[FieldSpec(key="name", label="Name", field_type="text")],
            ),
            TableSpec(
                key="child",
                name=child_name,
                fields=[FieldSpec(key="hours", label="Hours", field_type="numeric")],
            ),
        ],
        relationships=[
            RelationshipSpec(
                key="parent_children",
                parent_table="parent",
                child_table="child",
                foreign_key_label="Related Parent",
                lookup_fields=["name"],
                summary_fields=[
                    SummaryFieldSpec(
                        key="child_count",
                        accumulation_type="COUNT",
                        label="Child Count",
                    )
                ],
            )
        ],
    )

    try:
        plan = sandbox_client.plan_app(spec, state_path=state_path)
        result = sandbox_client.apply_app(plan)
        second_plan = sandbox_client.plan_app(spec, state_path=state_path)
        second_result = sandbox_client.apply_app(second_plan)

        assert result.quickbase_change_count == 7
        assert result.verification.quickbase_change_count == 0
        assert result.verification.state_change_count == 0
        assert not second_result.state_written
        assert second_result.state.serial == 1
        assert second_plan.quickbase_change_count == 0
        assert second_plan.state_change_count == 0
    finally:
        tables = _objects(
            sandbox_transport.get("tables", params={"appId": sandbox_contract.app_id})
        )
        table_ids = {
            str(table.get("name")): str(table["id"])
            for table in tables
            if table.get("name") in {parent_name, child_name}
        }
        for table_name in (child_name, parent_name):
            table_id = table_ids.get(table_name)
            if table_id is not None:
                sandbox_transport.delete(
                    f"tables/{table_id}",
                    params={"appId": sandbox_contract.app_id},
                )


@pytest.mark.sandbox_mutation
def test_declarative_formulas_calculate_across_relationship_resources(
    sandbox_client: QuickBaseClient,
    sandbox_transport: QuickBaseTransport,
    sandbox_config: SandboxConfig,
    tmp_path: Path,
):
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to run mutation contract tests")

    suffix = uuid.uuid4().hex[:10]
    parent_name = f"qbvisor_formula_parent_{suffix}"
    child_name = f"qbvisor_formula_child_{suffix}"
    state_path = tmp_path / ".qbvisor" / "state.json"
    spec = AppSpec(
        key="sandbox",
        name=_app_name(sandbox_client),
        tables=[
            TableSpec(
                key="parents",
                name=parent_name,
                fields=[
                    FieldSpec(key="quantity", label="Quantity", field_type="numeric"),
                    FieldSpec(key="rate", label="Rate", field_type="currency"),
                    FieldSpec(
                        key="line_total",
                        label="Line Total",
                        field_type="currency",
                        formula=FormulaSpec(
                            expression="[Quantity] * [Rate]",
                            depends_on=(
                                "tables.parents.fields.quantity",
                                "tables.parents.fields.rate",
                            ),
                        ),
                        properties={"decimalPlaces": 2},
                    ),
                    FieldSpec(
                        key="final_total",
                        label="Final Total",
                        field_type="numeric",
                        formula=FormulaSpec(
                            expression="[Line Total] + [Total Derived Amount]",
                            depends_on=(
                                "tables.parents.fields.line_total",
                                "relationships.parent_children.summaries.total_derived",
                            ),
                        ),
                    ),
                ],
            ),
            TableSpec(
                key="children",
                name=child_name,
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
                            expression="[Related Formula Parent - Line Total] + 1",
                            depends_on=("relationships.parent_children.lookups.line_total",),
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
                foreign_key_label="Related Formula Parent",
                lookup_fields=("line_total",),
                summary_fields=(
                    SummaryFieldSpec(
                        key="total_derived",
                        accumulation_type="SUM",
                        field="derived_amount",
                        label="Total Derived Amount",
                    ),
                ),
            )
        ],
    )

    try:
        result = sandbox_client.apply_app(sandbox_client.plan_app(spec, state_path=state_path))
        second = sandbox_client.plan_app(spec, state_path=state_path)
        resources = {resource.address: resource for resource in result.state.resources}
        parent_id = str(resources["apps.sandbox.tables.parents"].remote_id)
        child_id = str(resources["apps.sandbox.tables.children"].remote_id)
        quantity_id = int(resources["apps.sandbox.tables.parents.fields.quantity"].remote_id)
        rate_id = int(resources["apps.sandbox.tables.parents.fields.rate"].remote_id)
        line_total_id = int(resources["apps.sandbox.tables.parents.fields.line_total"].remote_id)
        final_total_id = int(resources["apps.sandbox.tables.parents.fields.final_total"].remote_id)
        amount_id = int(resources["apps.sandbox.tables.children.fields.amount"].remote_id)
        derived_amount_id = int(
            resources["apps.sandbox.tables.children.fields.derived_amount"].remote_id
        )
        lookup_plus_one_id = int(
            resources["apps.sandbox.tables.children.fields.lookup_plus_one"].remote_id
        )
        foreign_key_id = int(resources["apps.sandbox.relationships.parent_children"].remote_id)
        summary_id = int(
            resources[
                "apps.sandbox.relationships.parent_children.summaries.total_derived"
            ].remote_id
        )

        assert second.quickbase_change_count == 0
        assert second.state_change_count == 0

        parent_upsert = _object(
            sandbox_transport.post(
                "records",
                json_body={
                    "to": parent_id,
                    "data": [
                        {
                            str(quantity_id): {"value": 3},
                            str(rate_id): {"value": 10.5},
                        }
                    ],
                },
            )
        )
        metadata = parent_upsert["metadata"]
        assert isinstance(metadata, dict)
        parent_record_id = int(metadata["createdRecordIds"][0])
        _object(
            sandbox_transport.post(
                "records",
                json_body={
                    "to": child_id,
                    "data": [
                        {
                            str(amount_id): {"value": 4},
                            str(foreign_key_id): {"value": parent_record_id},
                        }
                    ],
                },
            )
        )

        parent_query = _object(
            sandbox_transport.post(
                "records/query",
                json_body={
                    "from": parent_id,
                    "select": [line_total_id, summary_id, final_total_id],
                },
            )
        )
        child_query = _object(
            sandbox_transport.post(
                "records/query",
                json_body={
                    "from": child_id,
                    "select": [derived_amount_id, lookup_plus_one_id],
                },
            )
        )
        parent_row = parent_query["data"][0]
        child_row = child_query["data"][0]

        assert parent_row[str(line_total_id)]["value"] == 31.5
        assert parent_row[str(summary_id)]["value"] == 8
        assert parent_row[str(final_total_id)]["value"] == 39.5
        assert child_row[str(derived_amount_id)]["value"] == 8
        assert child_row[str(lookup_plus_one_id)]["value"] == 32.5
    finally:
        tables = _objects(sandbox_transport.get("tables", params={"appId": sandbox_config.app_id}))
        table_ids = {
            str(table.get("name")): str(table["id"])
            for table in tables
            if table.get("name") in {parent_name, child_name}
        }
        for table_name in (child_name, parent_name):
            table_id = table_ids.get(table_name)
            if table_id is not None:
                sandbox_transport.delete(
                    f"tables/{table_id}",
                    params={"appId": sandbox_config.app_id},
                )
