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
    SandboxContract,
)

from qbvisor import (
    AppSpec,
    FieldSpec,
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
