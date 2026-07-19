from __future__ import annotations

import base64
import json
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

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

from qbvisor import BackupOptions
from qbvisor.client import QuickBaseClient
from qbvisor.models import RelationshipSummary
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
                    contract.record_fields["Attachment"],
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


def test_repeated_label_resolution_reuses_live_metadata(
    sandbox_transport: QuickBaseTransport,
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    sandbox_client.meta.invalidate_tables(APP_NAME)

    with patch.object(sandbox_transport, "get", wraps=sandbox_transport.get) as get:
        for _ in range(100):
            assert (
                sandbox_client.get_field_id(
                    sandbox_contract.app_id,
                    sandbox_contract.records_table_id,
                    "Fixture Key",
                )
                == sandbox_contract.record_fields["Fixture Key"]
            )
            assert (
                sandbox_client.get_field_id(
                    sandbox_contract.app_id,
                    sandbox_contract.records_table_id,
                    "name",
                )
                == sandbox_contract.record_fields["Name"]
            )

    assert get.call_count == 3
    assert [call.args[0] for call in get.call_args_list] == [
        "tables",
        f"tables/{sandbox_contract.records_table_id}",
        "fields",
    ]


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

    complete_subset = sandbox_client.query_dataframe(
        APP_NAME,
        sandbox_contract.records_table_id,
        ["Fixture Key", "Name"],
        where="OR".join(
            f"{{{sandbox_contract.record_fields['Fixture Key']}.EX.'{key}'}}"
            for key in ("qbvisor-alpha", "qbvisor-beta", "qbvisor-gamma")
        ),
        top=2,
    )
    assert list(complete_subset.columns) == ["Fixture Key", "Name"]
    assert len(complete_subset) == 2


def test_formula_response_matches_documented_object_shape(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    response = sandbox_client.run_formula(APP_NAME, sandbox_contract.records_table_id, "1 + 2")

    assert response == {"result": "3"}


def test_app_events_and_roles_match_documented_array_shapes(
    sandbox_client: QuickBaseClient,
):
    events = sandbox_client.get_app_events(APP_NAME)
    roles = sandbox_client.get_app_roles(APP_NAME)

    assert isinstance(events, list)
    assert all(isinstance(event, dict) for event in events)
    assert roles
    assert all(
        isinstance(role.get("id"), int) and isinstance(role.get("name"), str) for role in roles
    )


def test_field_usage_supports_table_and_single_field_inspection(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    usage = sandbox_client.get_fields_usage(
        APP_NAME,
        sandbox_contract.records_table_id,
        top=100,
    )
    fixture_key_usage = sandbox_client.get_field_usage(
        APP_NAME,
        sandbox_contract.records_table_id,
        "Fixture Key",
    )

    returned_ids = {item["field"]["id"] for item in usage}
    assert set(sandbox_contract.record_fields.values()).issubset(returned_ids)
    assert len(fixture_key_usage) == 1
    assert fixture_key_usage[0]["field"]["id"] == sandbox_contract.record_fields["Fixture Key"]
    assert isinstance(fixture_key_usage[0]["usage"], dict)


def test_records_modified_since_returns_persistent_fixture_changes(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    response = sandbox_client.records_modified_since(
        APP_NAME,
        sandbox_contract.records_table_id,
        "2000-01-01T00:00:00Z",
        field_list=["Name"],
        include_details=True,
    )

    assert response["count"] >= len(sandbox_contract.record_ids)
    changes = response["changes"]
    assert isinstance(changes, list)
    changed_record_ids = {change["recordId"] for change in changes}
    assert set(sandbox_contract.record_ids.values()).issubset(changed_record_ids)
    assert all(change["changeType"] in {"CREATE", "MODIFY", "DELETE"} for change in changes)


def test_records_modified_since_accepts_a_current_fractional_timestamp(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
):
    response = sandbox_client.records_modified_since(
        APP_NAME,
        sandbox_contract.records_table_id,
        datetime.now(UTC),
    )

    assert response["count"] == 0


@pytest.mark.sandbox_mutation
def test_upsert_results_distinguish_created_updated_and_unchanged_records(
    sandbox_client: QuickBaseClient,
    sandbox_transport: QuickBaseTransport,
    sandbox_contract: SandboxContract,
):
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to run mutation contract tests")

    fixture_key = f"qbvisor-upsert-contract-{uuid.uuid4().hex[:10]}"
    key_fid = sandbox_contract.record_fields["Fixture Key"]
    status_fid = sandbox_contract.record_fields["Status"]
    try:
        created = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [{"Fixture Key": fixture_key, "Status": "Ready"}],
            merge_field_label="Fixture Key",
            fields_to_return=["Fixture Key", "Status"],
        )

        assert created["success"] is True
        assert len(created["createdRecordIds"]) == 1
        assert created["updatedRecordIds"] == []
        assert created["unchangedRecordIds"] == []
        assert created["totalProcessed"] == 1
        record_id = int(created["createdRecordIds"][0])
        assert created["data"] == [
            {
                "3": {"value": record_id},
                str(key_fid): {"value": fixture_key},
                str(status_fid): {"value": "Ready"},
            }
        ]

        unchanged = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [{"Fixture Key": fixture_key, "Status": "Ready"}],
            merge_field_label="Fixture Key",
            fields_to_return=["Fixture Key", "Status"],
        )

        assert unchanged["success"] is True
        assert unchanged["createdRecordIds"] == []
        assert unchanged["updatedRecordIds"] == []
        assert unchanged["unchangedRecordIds"] == [record_id]
        assert unchanged["data"] == created["data"]

        updated = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [{"Fixture Key": fixture_key, "Status": "Running"}],
            merge_field_label="Fixture Key",
            fields_to_return=["Fixture Key", "Status"],
        )

        assert updated["success"] is True
        assert updated["createdRecordIds"] == []
        assert updated["updatedRecordIds"] == [record_id]
        assert updated["unchangedRecordIds"] == []
        assert updated["data"][0][str(status_fid)]["value"] == "Running"
    finally:
        records = _query_keys(sandbox_transport, sandbox_contract)
        if fixture_key in records:
            sandbox_client.delete_records(
                APP_NAME,
                sandbox_contract.records_table_id,
                [int(records[fixture_key]["3"]["value"])],
            )


@pytest.mark.sandbox_mutation
def test_upsert_results_preserve_partial_line_errors_and_successful_records(
    sandbox_client: QuickBaseClient,
    sandbox_transport: QuickBaseTransport,
    sandbox_contract: SandboxContract,
):
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to run mutation contract tests")

    valid_key = f"qbvisor-upsert-valid-{uuid.uuid4().hex[:10]}"
    invalid_key = f"qbvisor-upsert-invalid-{uuid.uuid4().hex[:10]}"
    created_ids: list[int] = []
    try:
        result = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [
                {"Fixture Key": valid_key, "Status": "Complete"},
                {"Fixture Key": invalid_key, "Status": "Not a configured choice"},
            ],
            merge_field_label="Fixture Key",
            fields_to_return=["Fixture Key", "Status"],
        )

        created_ids = result["createdRecordIds"]
        assert result["success"] is False
        assert result["partial"] is True
        assert result["totalProcessed"] == 2
        assert set(result["lineErrors"]) == {"2"}
        assert result["lineErrors"]["2"]
        assert len(created_ids) == 1
        assert result["updatedRecordIds"] == []
        assert result["unchangedRecordIds"] == []
        assert all(isinstance(record, dict) for record in result["data"])
    finally:
        records = _query_keys(sandbox_transport, sandbox_contract)
        cleanup_ids = [
            int(records[key]["3"]["value"]) for key in (valid_key, invalid_key) if key in records
        ]
        if cleanup_ids:
            sandbox_client.delete_records(
                APP_NAME,
                sandbox_contract.records_table_id,
                cleanup_ids,
            )


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


def test_streaming_record_export_round_trips_persistent_rows(
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


def test_attachment_discovery_scans_all_keyset_pages(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    record_ids = [
        sandbox_contract.record_ids[key]
        for key in ("qbvisor-alpha", "qbvisor-beta", "qbvisor-gamma")
    ]
    where = "OR".join(f"{{3.EX.{record_id}}}" for record_id in record_ids)
    original_query = sandbox_client._query_records_by_ids
    queries: list[dict[str, Any]] = []

    def record_query(*args: Any, **kwargs: Any) -> dict[str, Any]:
        queries.append(kwargs)
        return original_query(*args, **kwargs)

    monkeypatch.setattr(sandbox_client, "_query_records_by_ids", record_query)

    results = sandbox_client.download_attachments_async(
        APP_NAME,
        sandbox_contract.records_table_id,
        "Attachment",
        str(tmp_path),
        where=where,
        page_size=2,
    )

    assert len(queries) == 2
    assert queries[0]["where"] == where
    assert queries[1]["where"].startswith(f"({where})AND{{3.GT.")
    assert len(results) == 1
    assert Path(results[0]["saved_path"]).read_bytes() == ATTACHMENT_CONTENT


def test_application_backup_round_trips_persistent_records_and_attachments(
    sandbox_client: QuickBaseClient,
    sandbox_contract: SandboxContract,
    tmp_path: Path,
):
    backup = sandbox_client.backup_app(
        APP_NAME,
        tmp_path,
        options=BackupOptions(attachment_versions="all", page_size=2),
    )

    verification = backup.verify()
    assert verification.artifact_count == len(backup.manifest.artifacts)
    assert backup.manifest.source_app_id == sandbox_contract.app_id
    assert backup.manifest.consistent is True

    frame = backup.table_dataframe(sandbox_contract.records_table_id)
    fixture_rows = frame[frame["Fixture Key"].str.startswith("qbvisor-")]
    assert {"qbvisor-alpha", "qbvisor-beta", "qbvisor-gamma"}.issubset(
        set(fixture_rows["Fixture Key"])
    )

    index_path = backup.path / "tables" / sandbox_contract.records_table_id / "attachments.jsonl"
    entries = [json.loads(line) for line in index_path.read_text().splitlines()]
    match = next(
        entry
        for entry in entries
        if entry["record_id"] == sandbox_contract.record_ids["qbvisor-alpha"]
        and entry["field_id"] == sandbox_contract.record_fields["Attachment"]
    )
    assert (backup.path / match["path"]).read_bytes() == ATTACHMENT_CONTENT


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
        sandbox_client.create_field(APP_NAME, parent_name, "Lookup Source", "text")
        sandbox_client.create_field(APP_NAME, child_name, "Hours", "numeric")
        relationship = sandbox_client.create_relationship(
            APP_NAME,
            child_name,
            parent_name,
            foreign_key_label="Related Temporary Parent",
        )
        assert relationship["foreignKeyField"]["label"] == "Related Temporary Parent"

        updated = sandbox_client.update_relationship(
            APP_NAME,
            child_name,
            "Related Temporary Parent",
            lookup_fields=["Lookup Source"],
            summary_fields=[RelationshipSummary("COUNT", label="Temporary Child Count")],
        )
        assert updated["id"] == relationship["id"]
        assert len(updated["lookupFields"]) == 1
        assert "Lookup Source" in updated["lookupFields"][0]["label"]
        assert {field["label"] for field in updated["summaryFields"]} == {"Temporary Child Count"}

        deleted_id = sandbox_client.delete_relationship(
            APP_NAME, child_name, "Related Temporary Parent"
        )
        assert deleted_id == relationship["id"]
    finally:
        if child_created:
            sandbox_client.delete_table(APP_NAME, child_name)
        if parent_created:
            sandbox_client.delete_table(APP_NAME, parent_name)


@pytest.mark.sandbox_mutation
def test_file_version_deletion_removes_only_the_disposable_attachment(
    sandbox_client: QuickBaseClient,
    sandbox_transport: QuickBaseTransport,
    sandbox_contract: SandboxContract,
):
    if os.getenv(MUTATION_ENV) != "1":
        pytest.skip(f"Set {MUTATION_ENV}=1 to run mutation contract tests")

    fixture_key = f"qbvisor-delete-file-{uuid.uuid4().hex[:10]}"
    record_id: int | None = None
    attachment_fid = sandbox_contract.record_fields["Attachment"]
    try:
        created = sandbox_client.upsert_records(
            APP_NAME,
            sandbox_contract.records_table_id,
            [
                {
                    "Fixture Key": fixture_key,
                    "Attachment": {
                        "fileName": "qbvisor-delete-contract.txt",
                        "data": base64.b64encode(b"Disposable attachment version.\n").decode(
                            "ascii"
                        ),
                    },
                }
            ],
            fields_to_return=["Fixture Key", "Attachment"],
        )
        assert created["success"] is True
        records = _query_keys(sandbox_transport, sandbox_contract)
        record_id = int(records[fixture_key]["3"]["value"])
        attachment = records[fixture_key][str(attachment_fid)]["value"]
        version = max(attachment["versions"], key=lambda item: item["versionNumber"])

        deleted = sandbox_client.delete_file(
            APP_NAME,
            sandbox_contract.records_table_id,
            record_id,
            "Attachment",
            int(version["versionNumber"]),
        )

        assert deleted["versionNumber"] == version["versionNumber"]
        records_after = _query_keys(sandbox_transport, sandbox_contract)
        assert (
            not records_after[fixture_key]
            .get(str(attachment_fid), {})
            .get("value", {})
            .get("versions")
        )
    finally:
        if record_id is not None:
            sandbox_transport.delete(
                "records",
                json_body={
                    "from": sandbox_contract.records_table_id,
                    "where": [record_id],
                },
            )
