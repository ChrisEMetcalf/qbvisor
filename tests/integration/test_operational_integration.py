from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from pathlib import Path

import pytest
from conftest import (
    APP_NAME,
    DETAILS_TABLE_NAME,
    MUTATION_ENV,
    RECORDS_TABLE_NAME,
    SandboxContract,
)
from operational_support import (
    OPERATIONAL_PREFIX,
    OperationalDiagnostics,
    recover_operational_records,
    safe_run_id,
)

from qbvisor import (
    AppSpec,
    BackupOptions,
    FieldSpec,
    QueryHelper,
    QuickBaseClient,
    RelationshipSpec,
    TableSpec,
)

OPERATIONAL_ENV = "QBVISOR_RUN_OPERATIONAL"
FAIL_CLEANUP_ENV = "QBVISOR_OPERATIONAL_FAIL_CLEANUP_ONCE"
RESULTS_ENV = "QBVISOR_OPERATIONAL_RESULTS"
RUN_ID_ENV = "QBVISOR_OPERATIONAL_RUN_ID"
CANDIDATE_REF_ENV = "QBVISOR_CANDIDATE_REF"
ATTACHMENT_CONTENT = b"qbvisor scheduled operational attachment smoke\n"

pytestmark = [
    pytest.mark.integration,
    pytest.mark.operational,
    pytest.mark.skipif(
        os.getenv(OPERATIONAL_ENV) != "1",
        reason=f"Set {OPERATIONAL_ENV}=1 to run the scheduled operational smoke suite",
    ),
]


@dataclass(frozen=True)
class OperationalRun:
    run_id: str
    diagnostics: OperationalDiagnostics

    def key(self, check: str) -> str:
        return f"{OPERATIONAL_PREFIX}{self.run_id}-{check}"


def _matching_count(
    client: QuickBaseClient,
    contract: SandboxContract,
    where: str,
) -> int:
    frame = client.query_dataframe(
        APP_NAME,
        contract.records_table_id,
        ["Fixture Key"],
        where=where,
    )
    return len(frame)


@pytest.fixture(scope="session")
def operational_run(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
) -> OperationalRun:
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to allow operational cleanup and mutation checks")

    run_id = safe_run_id(os.getenv(RUN_ID_ENV))
    output_dir = Path(os.getenv(RESULTS_ENV, ".qbvisor/operational"))
    diagnostics = OperationalDiagnostics(
        output_dir / "summary.json",
        run_id=run_id,
        candidate_ref=os.getenv(CANDIDATE_REF_ENV) or "local",
    )
    query = QueryHelper(sandbox_client, APP_NAME, sandbox_contract.records_table_id)
    recovery_where = query.starts_with("Fixture Key", OPERATIONAL_PREFIX)
    try:
        with diagnostics.check("recovery"):
            recovered = recover_operational_records(
                delete_matching=lambda: sandbox_client.delete_records(
                    APP_NAME,
                    sandbox_contract.records_table_id,
                    recovery_where,
                ),
                count_remaining=lambda: _matching_count(
                    sandbox_client,
                    sandbox_contract,
                    recovery_where,
                ),
            )
            diagnostics.record_recovery(recovered)
    except BaseException:
        diagnostics.finish()
        raise

    try:
        yield OperationalRun(run_id=run_id, diagnostics=diagnostics)
    finally:
        diagnostics.finish(require_complete=True)


def _record_cleanup(
    client: QuickBaseClient,
    contract: SandboxContract,
    where: str,
) -> int:
    deleted = client.delete_records(APP_NAME, contract.records_table_id, where)
    remaining = _matching_count(client, contract, where)
    if remaining:
        raise AssertionError(f"Mutation cleanup left {remaining} owned record(s)")
    return deleted


def test_operational_read_smoke(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    operational_run: OperationalRun,
):
    with operational_run.diagnostics.check("read"):
        query = QueryHelper(sandbox_client, APP_NAME, sandbox_contract.records_table_id)
        where = query.starts_with("Fixture Key", "qbvisor-")
        frame = sandbox_client.query_dataframe(
            APP_NAME,
            sandbox_contract.records_table_id,
            ["Fixture Key", "Name", "Amount", "Status"],
            where=where,
        )

        assert {"qbvisor-alpha", "qbvisor-beta", "qbvisor-gamma"}.issubset(
            set(frame["Fixture Key"])
        )
        assert sandbox_client.get_app(APP_NAME)["id"] == sandbox_contract.app_id


@pytest.mark.sandbox_mutation
def test_operational_upsert_smoke_verifies_and_cleans_owned_record(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    operational_run: OperationalRun,
):
    key = operational_run.key("upsert")
    query = QueryHelper(sandbox_client, APP_NAME, sandbox_contract.records_table_id)
    where = query.eq("Fixture Key", key)
    created_confirmed = False

    with operational_run.diagnostics.mutating_check(
        "upsert",
        cleanup=lambda: _record_cleanup(sandbox_client, sandbox_contract, where),
        expected_deleted=lambda: 1 if created_confirmed else None,
        fail_cleanup=os.getenv(FAIL_CLEANUP_ENV) == "1",
    ):
        created = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [
                {
                    "Fixture Key": key,
                    "Name": "Scheduled operational upsert",
                    "Amount": 32,
                    "Status": "Ready",
                    "Active": True,
                    "Event Date": "2026-07-23",
                }
            ],
            merge_field_label="Fixture Key",
            fields_to_return=["Fixture Key", "Amount", "Status"],
        )
        assert created["success"] is True
        assert len(created["createdRecordIds"]) == 1
        assert created["updatedRecordIds"] == []
        assert created["unchangedRecordIds"] == []
        created_confirmed = True

        frame = sandbox_client.query_dataframe(
            APP_NAME,
            sandbox_contract.records_table_id,
            ["Fixture Key", "Amount", "Status"],
            where=where,
        )
        assert len(frame) == 1
        assert frame.iloc[0]["Fixture Key"] == key
        assert float(frame.iloc[0]["Amount"]) == 32

        updated = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [{"Fixture Key": key, "Amount": 36, "Status": "Complete"}],
            merge_field_label="Fixture Key",
            fields_to_return=["Fixture Key", "Amount", "Status"],
        )
        assert updated["success"] is True
        assert updated["createdRecordIds"] == []
        assert len(updated["updatedRecordIds"]) == 1


@pytest.mark.sandbox_mutation
def test_operational_attachment_smoke_round_trips_and_cleans_owned_record(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    operational_run: OperationalRun,
):
    key = operational_run.key("attachment")
    query = QueryHelper(sandbox_client, APP_NAME, sandbox_contract.records_table_id)
    where = query.eq("Fixture Key", key)
    created_confirmed = False

    with operational_run.diagnostics.mutating_check(
        "attachment",
        cleanup=lambda: _record_cleanup(sandbox_client, sandbox_contract, where),
        expected_deleted=lambda: 1 if created_confirmed else None,
    ):
        result = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [
                {
                    "Fixture Key": key,
                    "Name": "Scheduled operational attachment",
                    "Attachment": {
                        "fileName": "qbvisor-operational.txt",
                        "data": base64.b64encode(ATTACHMENT_CONTENT).decode("ascii"),
                    },
                }
            ],
            merge_field_label="Fixture Key",
            fields_to_return=["Fixture Key"],
        )
        assert result["success"] is True
        assert len(result["createdRecordIds"]) == 1
        record_id = int(result["createdRecordIds"][0])
        created_confirmed = True

        encoded = sandbox_client.download_attachment_base64(
            APP_NAME,
            sandbox_contract.records_table_id,
            record_id,
            "Attachment",
        )
        assert encoded is not None
        assert base64.b64decode(encoded) == ATTACHMENT_CONTENT


def test_operational_backup_smoke_verifies_persistent_fixture(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    operational_run: OperationalRun,
    tmp_path: Path,
):
    with operational_run.diagnostics.check("backup"):
        backup = sandbox_client.backup_app(
            APP_NAME,
            tmp_path / "backup",
            options=BackupOptions(
                attachment_versions="latest",
                page_size=100,
                fail_on_changes=True,
            ),
        )
        verification = backup.verify()
        records = backup.table_dataframe(sandbox_contract.records_table_id)

        assert backup.manifest.consistent is True
        assert verification.artifact_count == len(backup.manifest.artifacts)
        assert verification.total_bytes > 0
        assert {"qbvisor-alpha", "qbvisor-beta", "qbvisor-gamma"}.issubset(
            set(records["Fixture Key"])
        )
        assert sum(table.attachment_count for table in backup.manifest.tables) >= 1


def test_operational_schema_plan_smoke_is_read_only(
    sandbox_client: QuickBaseClient,
    operational_run: OperationalRun,
    tmp_path: Path,
):
    with operational_run.diagnostics.check("schema-plan"):
        app = sandbox_client.get_app(APP_NAME)
        app_name = app.get("name")
        assert isinstance(app_name, str) and app_name
        state_path = tmp_path / "state.json"
        spec = AppSpec(
            key="sandbox",
            name=app_name,
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

        plan = sandbox_client.plan_app(spec, state_path=state_path)

        assert plan.can_apply
        assert plan.quickbase_change_count == 0
        assert plan.state_change_count == 8
        assert not state_path.exists()
