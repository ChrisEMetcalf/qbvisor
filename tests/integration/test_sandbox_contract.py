from __future__ import annotations

import base64
import os
import uuid
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest
import requests
from conftest import (
    APP_NAME,
    ATTACHMENT_CONTENT,
    ATTACHMENT_FILE_NAME,
    MUTATION_ENV,
    SandboxConfig,
    SandboxContract,
)

from qbvisor.client import QuickBaseClient
from qbvisor.transport import JSONValue, QuickBaseTransport, RetryPolicy

pytestmark = pytest.mark.integration


def _object(payload: JSONValue) -> dict[str, Any]:
    assert isinstance(payload, dict)
    return cast(dict[str, Any], payload)


def _query_keys(
    transport: QuickBaseTransport, contract: SandboxContract
) -> dict[str, dict[str, Any]]:
    response = _object(
        transport.post(
            "records/query",
            json_body={
                "from": contract.records_table_id,
                "select": [
                    3,
                    contract.record_fields["Fixture Key"],
                    contract.record_fields["Name"],
                    contract.record_fields["Status"],
                ],
                "options": {"top": 100},
            },
            retry_policy=RetryPolicy.SAFE,
        )
    )
    records = response["data"]
    assert isinstance(records, list)
    key_fid = str(contract.record_fields["Fixture Key"])
    return {
        str(record[key_fid]["value"]): record
        for record in records
        if isinstance(record, dict) and key_fid in record
    }


def test_persistent_tables_and_fields_match_contract(
    sandbox_transport: QuickBaseTransport,
    sandbox_contract: SandboxContract,
):
    tables_payload = sandbox_transport.get("tables", params={"appId": sandbox_contract.app_id})
    assert isinstance(tables_payload, list)
    tables = cast(list[dict[str, Any]], tables_payload)
    assert {sandbox_contract.records_table_id, sandbox_contract.details_table_id}.issubset(
        {table["id"] for table in tables}
    )

    fields_payload = sandbox_transport.get(
        "fields", params={"tableId": sandbox_contract.records_table_id}
    )
    assert isinstance(fields_payload, list)
    fields = cast(list[dict[str, Any]], fields_payload)
    assert set(sandbox_contract.record_fields).issubset({field["label"] for field in fields})

    field = sandbox_transport.get(
        f"fields/{sandbox_contract.record_fields['Fixture Key']}",
        params={"tableId": sandbox_contract.records_table_id},
    )
    assert isinstance(field, dict)
    assert field["unique"] is True


def test_persistent_relationship_matches_contract(
    sandbox_transport: QuickBaseTransport,
    sandbox_contract: SandboxContract,
):
    payload = _object(
        sandbox_transport.get(f"tables/{sandbox_contract.details_table_id}/relationships")
    )
    relationships = payload["relationships"]
    assert isinstance(relationships, list)
    match = next(
        relationship
        for relationship in relationships
        if relationship["id"] == sandbox_contract.relationship_id
    )
    assert match["parentTableId"] == sandbox_contract.records_table_id
    assert match["childTableId"] == sandbox_contract.details_table_id


def test_seed_query_and_dataframe_preserve_developer_facing_labels(
    sandbox_transport: QuickBaseTransport,
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    records = _query_keys(sandbox_transport, sandbox_contract)
    assert {"qbvisor-alpha", "qbvisor-beta", "qbvisor-gamma"}.issubset(records)

    frame = sandbox_client.query_dataframe(
        APP_NAME,
        sandbox_contract.records_table_id,
        ["Fixture Key", "Name", "Amount", "Status", "Active", "Event Date"],
        sort_by=[("Fixture Key", "ASC")],
    )
    fixture_rows = frame[frame["Fixture Key"].str.startswith("qbvisor-")]
    assert list(fixture_rows["Fixture Key"][:3]) == [
        "qbvisor-alpha",
        "qbvisor-beta",
        "qbvisor-gamma",
    ]
    assert list(fixture_rows["Name"][:3]) == ["Alpha", "Beta", "Gamma"]
    assert list(fixture_rows["Status"][:3]) == ["Ready", "Running", "Complete"]
    assert list(fixture_rows["Active"][:3]) == [True, False, True]
    assert list(fixture_rows["Event Date"][:3]) == [
        "2026-01-15",
        "2026-02-15",
        "2026-03-15",
    ]
    assert pd.api.types.is_numeric_dtype(fixture_rows["Amount"])


def test_formula_response_matches_documented_object_shape(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    response = sandbox_client.run_formula(APP_NAME, sandbox_contract.records_table_id, "1 + 2")

    assert response == {"result": "3"}


def test_default_report_contract_when_table_has_reports(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    reports = sandbox_client.get_reports_for_table(APP_NAME, sandbox_contract.records_table_id)
    assert isinstance(reports, list)
    if not reports:
        pytest.skip("Quickbase did not create a default report for the fixture table")

    report = sandbox_client.get_report(
        APP_NAME, sandbox_contract.records_table_id, int(reports[0]["id"])
    )
    assert report["id"] == reports[0]["id"]

    frame = sandbox_client.run_report(
        APP_NAME, sandbox_contract.records_table_id, int(reports[0]["id"]), top=10
    )
    assert isinstance(frame, pd.DataFrame)
    assert not frame.empty


def test_concurrent_record_export_round_trips_persistent_rows(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    tmp_path: Path,
):
    key_fid = sandbox_contract.record_fields["Fixture Key"]
    where = "OR".join(
        f"{{{key_fid}.EX.'{key}'}}" for key in ("qbvisor-alpha", "qbvisor-beta", "qbvisor-gamma")
    )

    output = sandbox_client.download_records_to_csv(
        APP_NAME,
        sandbox_contract.records_table_id,
        str(tmp_path),
        where=where,
        chunk_size=2,
        max_concurrency=2,
    )

    frame = pd.read_csv(output)
    fixture_rows = frame[frame["Fixture Key"].str.startswith("qbvisor-")]
    assert set(fixture_rows["Fixture Key"]) == {
        "qbvisor-alpha",
        "qbvisor-beta",
        "qbvisor-gamma",
    }


def test_attachment_downloads_preserve_documented_binary_bytes_and_skip_existing(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    tmp_path: Path,
):
    record_id = sandbox_contract.record_ids["qbvisor-alpha"]
    encoded = sandbox_client.download_attachment_base64(
        APP_NAME,
        sandbox_contract.records_table_id,
        record_id,
        "Attachment",
    )
    assert encoded is not None
    assert base64.b64decode(encoded) == ATTACHMENT_CONTENT

    first = sandbox_client.download_attachments_async(
        APP_NAME,
        sandbox_contract.records_table_id,
        "Attachment",
        str(tmp_path),
        where=f"{{3.EX.'{record_id}'}}",
        max_concurrency=2,
    )
    assert len(first) == 1
    assert first[0]["status"] == "downloaded"
    assert first[0]["file_name"] == ATTACHMENT_FILE_NAME
    assert Path(first[0]["saved_path"]).read_bytes() == ATTACHMENT_CONTENT

    second = sandbox_client.download_attachments_async(
        APP_NAME,
        sandbox_contract.records_table_id,
        "Attachment",
        str(tmp_path),
        where=f"{{3.EX.'{record_id}'}}",
        max_concurrency=2,
    )
    assert len(second) == 1
    assert second[0]["status"] == "skipped"


class RecordingSession(requests.Session):
    def __init__(self):
        super().__init__()
        self.last_status_code: int | None = None

    def request(self, *args, **kwargs):
        response = super().request(*args, **kwargs)
        self.last_status_code = response.status_code
        return response


@pytest.mark.sandbox_mutation
def test_upsert_207_preserves_partial_record_errors_and_cleans_up(
    sandbox_config: SandboxConfig,
    sandbox_transport: QuickBaseTransport,
    sandbox_contract: SandboxContract,
):
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to run mutation contract tests")

    suffix = uuid.uuid4().hex[:10]
    valid_key = f"qbvisor-partial-valid-{suffix}"
    invalid_key = f"qbvisor-partial-invalid-{suffix}"
    key_fid = sandbox_contract.record_fields["Fixture Key"]
    status_fid = sandbox_contract.record_fields["Status"]
    session = RecordingSession()
    transport = QuickBaseTransport(
        realm_hostname=sandbox_config.realm,
        auth_token=sandbox_config.token,
        session=session,
    )
    try:
        response = _object(
            transport.post(
                "records",
                json_body={
                    "to": sandbox_contract.records_table_id,
                    "mergeFieldId": key_fid,
                    "data": [
                        {
                            str(key_fid): {"value": valid_key},
                            str(status_fid): {"value": "Ready"},
                        },
                        {
                            str(key_fid): {"value": invalid_key},
                            str(status_fid): {"value": "Not A Contract Choice"},
                        },
                    ],
                },
            )
        )
        assert session.last_status_code == 207
        metadata = response["metadata"]
        assert isinstance(metadata, dict)
        assert metadata["lineErrors"]
    finally:
        records = _query_keys(sandbox_transport, sandbox_contract)
        record_ids = [
            int(record["3"]["value"])
            for key, record in records.items()
            if key in {valid_key, invalid_key}
        ]
        if record_ids:
            sandbox_transport.delete(
                "records",
                json_body={"from": sandbox_contract.records_table_id, "where": record_ids},
            )
        transport.close()
        session.close()


@pytest.mark.sandbox_mutation
def test_temporary_relationship_lifecycle_uses_documented_endpoints(
    sandbox_client: QuickBaseClient,
):
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to run mutation contract tests")

    suffix = uuid.uuid4().hex[:10]
    parent_name = f"qbvisor_tmp_parent_{suffix}"
    child_name = f"qbvisor_tmp_child_{suffix}"
    parent_created = child_created = False
    try:
        sandbox_client.create_table(APP_NAME, parent_name)
        parent_created = True
        sandbox_client.create_table(APP_NAME, child_name)
        child_created = True
        relationship = sandbox_client.create_relationship(
            APP_NAME,
            child_name,
            parent_name,
            foreign_key_label="Related Temporary Parent",
        )
        assert relationship["foreignKeyField"]["label"] == "Related Temporary Parent"

        deleted_id = sandbox_client.delete_relationship(
            APP_NAME, child_name, "Related Temporary Parent"
        )
        assert deleted_id == relationship["id"]
    finally:
        if child_created:
            sandbox_client.delete_table(APP_NAME, child_name)
        if parent_created:
            sandbox_client.delete_table(APP_NAME, parent_name)
