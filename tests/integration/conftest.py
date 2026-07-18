from __future__ import annotations

import base64
import json
import os
from dataclasses import dataclass, field
from typing import Any, cast

import pytest
from dotenv import load_dotenv

from qbvisor.client import QuickBaseClient
from qbvisor.transport import JSONValue, QuickBaseTransport, RetryPolicy

APP_NAME = "Sandbox"
RECORDS_TABLE_NAME = "qbvisor_sdk_contract_records"
DETAILS_TABLE_NAME = "qbvisor_sdk_contract_details"
MUTATION_ENV = "QBVISOR_ALLOW_SANDBOX_MUTATIONS"
ATTACHMENT_FILE_NAME = "qbvisor-contract.txt"
ATTACHMENT_CONTENT = b"Persistent qbvisor attachment contract.\n"


@dataclass(frozen=True)
class SandboxConfig:
    realm: str
    token: str = field(repr=False)
    app_id: str


@dataclass(frozen=True)
class SandboxContract:
    app_id: str
    records_table_id: str
    details_table_id: str
    record_fields: dict[str, int]
    detail_fields: dict[str, int]
    relationship_id: int
    record_ids: dict[str, int]


def _object(payload: JSONValue, label: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        pytest.fail(f"{label} returned {type(payload).__name__}, expected an object")
    return cast(dict[str, Any], payload)


def _objects(payload: JSONValue, label: str) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        pytest.fail(f"{label} returned a non-object array response")
    return cast(list[dict[str, Any]], payload)


def _mutations_allowed() -> bool:
    return os.getenv(MUTATION_ENV) == "1"


def _require_mutations(reason: str) -> None:
    if not _mutations_allowed():
        pytest.skip(f"{reason}; set {MUTATION_ENV}=1 to allow sandbox fixture changes")


def _tables(transport: QuickBaseTransport, app_id: str) -> list[dict[str, Any]]:
    return _objects(transport.get("tables", params={"appId": app_id}), "GET /tables")


def _fields(transport: QuickBaseTransport, table_id: str) -> list[dict[str, Any]]:
    return _objects(transport.get("fields", params={"tableId": table_id}), "GET /fields")


def _ensure_table(
    client: QuickBaseClient,
    transport: QuickBaseTransport,
    app_id: str,
    *,
    name: str,
    single_record_name: str,
    plural_record_name: str,
) -> str:
    match = next((table for table in _tables(transport, app_id) if table.get("name") == name), None)
    if match is not None:
        return str(match["id"])

    _require_mutations(f"Persistent table {name!r} is missing")
    created = client.create_table(
        APP_NAME,
        name,
        description="Persistent qbvisor SDK contract fixture. Safe to reuse between test runs.",
        singular_record_name=single_record_name,
        plural_record_name=plural_record_name,
    )
    return str(created["id"])


def _ensure_field(
    client: QuickBaseClient,
    transport: QuickBaseTransport,
    table_id: str,
    *,
    label: str,
    field_type: str,
    properties: dict[str, Any] | None = None,
    unique: bool = False,
) -> int:
    match = next(
        (field for field in _fields(transport, table_id) if field.get("label") == label), None
    )
    if match is None:
        _require_mutations(f"Persistent field {label!r} is missing")
        if properties:
            payload = transport.post(
                "fields",
                params={"tableId": table_id},
                json_body={"label": label, "fieldType": field_type, "properties": properties},
            )
            match = _object(payload, "POST /fields")
        else:
            match = client.create_field(APP_NAME, table_id, label, field_type)

    actual_type = match.get("fieldType")
    if actual_type != field_type:
        pytest.fail(f"Field {label!r} has type {actual_type!r}, expected {field_type!r}")

    field_id = int(match["id"])
    if properties:
        current_properties = match.get("properties", {})
        properties_match = isinstance(current_properties, dict) and all(
            current_properties.get(name) == value for name, value in properties.items()
        )
        if not properties_match:
            _require_mutations(f"Persistent field {label!r} properties have drifted")
            _object(
                transport.post(
                    f"fields/{field_id}",
                    params={"tableId": table_id},
                    json_body={"properties": properties},
                ),
                f"POST /fields/{field_id}",
            )
    if unique and not match.get("unique"):
        _require_mutations(f"Persistent field {label!r} is not unique")
        _object(
            transport.post(
                f"fields/{field_id}",
                params={"tableId": table_id},
                json_body={"unique": True},
            ),
            f"POST /fields/{field_id}",
        )
    return field_id


def _ensure_relationship(
    client: QuickBaseClient,
    transport: QuickBaseTransport,
    *,
    parent_table_id: str,
    child_table_id: str,
) -> tuple[int, int]:
    path = f"tables/{child_table_id}/relationships"
    response = _object(transport.get(path), f"GET /{path}")
    relationships = response.get("relationships", [])
    if not isinstance(relationships, list):
        pytest.fail("Relationship response did not contain an array")
    match = next(
        (
            relationship
            for relationship in relationships
            if isinstance(relationship, dict)
            and relationship.get("parentTableId") == parent_table_id
            and relationship.get("childTableId") == child_table_id
        ),
        None,
    )
    if match is None:
        _require_mutations("Persistent fixture relationship is missing")
        match = client.create_relationship(
            APP_NAME,
            child_table_id,
            parent_table_id,
            foreign_key_label="Related Contract Record",
        )

    foreign_key = match.get("foreignKeyField")
    if not isinstance(foreign_key, dict):
        pytest.fail("Relationship response did not include foreignKeyField metadata")
    return int(match["id"]), int(foreign_key["id"])


def _query_fixture_records(
    transport: QuickBaseTransport, table_id: str, key_field_id: int
) -> dict[str, int]:
    response = _object(
        transport.post(
            "records/query",
            json_body={
                "from": table_id,
                "select": [3, key_field_id],
                "options": {"top": 100},
            },
            retry_policy=RetryPolicy.SAFE,
        ),
        "POST /records/query",
    )
    records = response.get("data", [])
    if not isinstance(records, list):
        pytest.fail("Query response did not contain a data array")
    result: dict[str, int] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        key = record.get(str(key_field_id), {})
        record_id = record.get("3", {})
        if isinstance(key, dict) and isinstance(record_id, dict):
            key_value = key.get("value")
            record_id_value = record_id.get("value")
            if isinstance(key_value, str) and isinstance(record_id_value, int):
                result[key_value] = record_id_value
    return result


def _ensure_records(
    transport: QuickBaseTransport,
    table_id: str,
    fields: dict[str, int],
) -> dict[str, int]:
    expected = {
        "qbvisor-alpha": ("Alpha", 10.5, "Ready", True, "2026-01-15"),
        "qbvisor-beta": ("Beta", 20, "Running", False, "2026-02-15"),
        "qbvisor-gamma": ("Gamma", 30.25, "Complete", True, "2026-03-15"),
    }
    existing = _query_fixture_records(transport, table_id, fields["Fixture Key"])
    if set(expected).issubset(existing) and not _mutations_allowed():
        return existing
    _require_mutations("Persistent fixture records are incomplete")

    data = []
    for key, (name, amount, status, active, event_date) in expected.items():
        data.append(
            {
                str(fields["Fixture Key"]): {"value": key},
                str(fields["Name"]): {"value": name},
                str(fields["Amount"]): {"value": amount},
                str(fields["Status"]): {"value": status},
                str(fields["Active"]): {"value": active},
                str(fields["Event Date"]): {"value": event_date},
            }
        )
    response = _object(
        transport.post(
            "records",
            json_body={
                "to": table_id,
                "data": data,
                "mergeFieldId": fields["Fixture Key"],
                "fieldsToReturn": [3, fields["Fixture Key"]],
            },
        ),
        "POST /records",
    )
    metadata = response.get("metadata", {})
    if isinstance(metadata, dict) and metadata.get("lineErrors"):
        pytest.fail(f"Fixture upsert returned line errors: {metadata['lineErrors']}")
    return _query_fixture_records(transport, table_id, fields["Fixture Key"])


def _ensure_details(
    transport: QuickBaseTransport,
    table_id: str,
    fields: dict[str, int],
    record_ids: dict[str, int],
) -> None:
    existing = _query_fixture_records(transport, table_id, fields["Detail Key"])
    expected_keys = {"qbvisor-alpha-detail", "qbvisor-beta-detail"}
    if expected_keys.issubset(existing) and not _mutations_allowed():
        return
    _require_mutations("Persistent detail records are incomplete")
    data = [
        {
            str(fields["Detail Key"]): {"value": "qbvisor-alpha-detail"},
            str(fields["Note"]): {"value": "Persistent alpha relationship fixture"},
            str(fields["Related Contract Record"]): {"value": record_ids["qbvisor-alpha"]},
        },
        {
            str(fields["Detail Key"]): {"value": "qbvisor-beta-detail"},
            str(fields["Note"]): {"value": "Persistent beta relationship fixture"},
            str(fields["Related Contract Record"]): {"value": record_ids["qbvisor-beta"]},
        },
    ]
    response = _object(
        transport.post(
            "records",
            json_body={
                "to": table_id,
                "data": data,
                "mergeFieldId": fields["Detail Key"],
            },
        ),
        "POST /records",
    )
    metadata = response.get("metadata", {})
    if isinstance(metadata, dict) and metadata.get("lineErrors"):
        pytest.fail(f"Detail fixture upsert returned line errors: {metadata['lineErrors']}")


def _attachment_version(
    transport: QuickBaseTransport,
    table_id: str,
    record_id: int,
    field_id: int,
) -> dict[str, Any] | None:
    response = _object(
        transport.post(
            "records/query",
            json_body={
                "from": table_id,
                "select": [3, field_id],
                "where": f"{{3.EX.'{record_id}'}}",
            },
            retry_policy=RetryPolicy.SAFE,
        ),
        "POST /records/query",
    )
    records = response.get("data", [])
    if not isinstance(records, list) or not records:
        pytest.fail("Persistent attachment record is missing")
    record = records[0]
    if not isinstance(record, dict):
        pytest.fail("Persistent attachment query returned a non-object record")
    cell = record.get(str(field_id), {})
    if not isinstance(cell, dict):
        return None
    value = cell.get("value", {})
    if not isinstance(value, dict):
        return None
    versions = value.get("versions", [])
    if not isinstance(versions, list) or not versions:
        return None
    version = max(
        (item for item in versions if isinstance(item, dict)),
        key=lambda item: item.get("versionNumber", 0),
        default=None,
    )
    return cast(dict[str, Any] | None, version)


def _ensure_attachment(
    transport: QuickBaseTransport,
    table_id: str,
    field_id: int,
    record_id: int,
) -> None:
    version = _attachment_version(transport, table_id, record_id, field_id)
    if version is not None:
        version_number = int(version["versionNumber"])
        payload = transport.get_file(f"files/{table_id}/{record_id}/{field_id}/{version_number}")
        if payload == ATTACHMENT_CONTENT and version.get("fileName") == ATTACHMENT_FILE_NAME:
            return

    _require_mutations("Persistent attachment fixture is missing or has drifted")
    response = _object(
        transport.post(
            "records",
            json_body={
                "to": table_id,
                "mergeFieldId": 3,
                "data": [
                    {
                        "3": {"value": record_id},
                        str(field_id): {
                            "value": {
                                "fileName": ATTACHMENT_FILE_NAME,
                                "data": base64.b64encode(ATTACHMENT_CONTENT).decode("ascii"),
                            }
                        },
                    }
                ],
            },
        ),
        "POST /records",
    )
    metadata = response.get("metadata", {})
    if isinstance(metadata, dict) and metadata.get("lineErrors"):
        pytest.fail(f"Attachment fixture upsert returned line errors: {metadata['lineErrors']}")

    version = _attachment_version(transport, table_id, record_id, field_id)
    if version is None:
        pytest.fail("Attachment fixture was not returned after upsert")
    payload = transport.get_file(
        f"files/{table_id}/{record_id}/{field_id}/{int(version['versionNumber'])}"
    )
    if payload != ATTACHMENT_CONTENT:
        pytest.fail("Attachment fixture bytes did not round-trip through Quickbase")


@pytest.fixture(scope="session")
def sandbox_config():
    if os.getenv("QBVISOR_RUN_INTEGRATION") != "1":
        pytest.skip("Set QBVISOR_RUN_INTEGRATION=1 to run live sandbox tests")
    load_dotenv()
    variable_names = (
        "QBVISOR_TEST_REALM",
        "QBVISOR_TEST_TOKEN",
        "QBVISOR_TEST_APP_ID",
    )
    config = {name: os.getenv(name) for name in variable_names}
    missing = [name for name, value in config.items() if not value]
    if missing:
        pytest.skip(f"Persistent sandbox is not configured; missing {', '.join(missing)}")

    sandbox = SandboxConfig(
        realm=cast(str, config["QBVISOR_TEST_REALM"]),
        token=cast(str, config["QBVISOR_TEST_TOKEN"]),
        app_id=cast(str, config["QBVISOR_TEST_APP_ID"]),
    )
    previous_app_ids = os.environ.get("QB_APP_IDS")
    os.environ["QB_APP_IDS"] = json.dumps({APP_NAME: sandbox.app_id})
    try:
        yield sandbox
    finally:
        if previous_app_ids is None:
            os.environ.pop("QB_APP_IDS", None)
        else:
            os.environ["QB_APP_IDS"] = previous_app_ids


@pytest.fixture(scope="session")
def sandbox_transport(sandbox_config: SandboxConfig):
    transport = QuickBaseTransport(
        realm_hostname=sandbox_config.realm,
        auth_token=sandbox_config.token,
    )
    try:
        yield transport
    finally:
        transport.close()


@pytest.fixture(scope="session")
def sandbox_client(sandbox_transport: QuickBaseTransport):
    with QuickBaseClient(transport=sandbox_transport) as client:
        yield client


@pytest.fixture(scope="session")
def sandbox_contract(
    sandbox_config: SandboxConfig,
    sandbox_transport: QuickBaseTransport,
    sandbox_client: QuickBaseClient,
):
    records_table_id = _ensure_table(
        sandbox_client,
        sandbox_transport,
        sandbox_config.app_id,
        name=RECORDS_TABLE_NAME,
        single_record_name="Contract Record",
        plural_record_name="Contract Records",
    )
    details_table_id = _ensure_table(
        sandbox_client,
        sandbox_transport,
        sandbox_config.app_id,
        name=DETAILS_TABLE_NAME,
        single_record_name="Contract Detail",
        plural_record_name="Contract Details",
    )

    record_fields = {
        "Fixture Key": _ensure_field(
            sandbox_client,
            sandbox_transport,
            records_table_id,
            label="Fixture Key",
            field_type="text",
            unique=True,
        ),
        "Name": _ensure_field(
            sandbox_client,
            sandbox_transport,
            records_table_id,
            label="Name",
            field_type="text",
        ),
        "Amount": _ensure_field(
            sandbox_client,
            sandbox_transport,
            records_table_id,
            label="Amount",
            field_type="numeric",
        ),
        "Status": _ensure_field(
            sandbox_client,
            sandbox_transport,
            records_table_id,
            label="Status",
            field_type="text-multiple-choice",
            properties={
                "choices": ["Ready", "Running", "Complete"],
                "allowNewChoices": False,
            },
        ),
        "Active": _ensure_field(
            sandbox_client,
            sandbox_transport,
            records_table_id,
            label="Active",
            field_type="checkbox",
        ),
        "Event Date": _ensure_field(
            sandbox_client,
            sandbox_transport,
            records_table_id,
            label="Event Date",
            field_type="date",
        ),
        "Attachment": _ensure_field(
            sandbox_client,
            sandbox_transport,
            records_table_id,
            label="Attachment",
            field_type="file",
        ),
    }
    detail_fields = {
        "Detail Key": _ensure_field(
            sandbox_client,
            sandbox_transport,
            details_table_id,
            label="Detail Key",
            field_type="text",
            unique=True,
        ),
        "Note": _ensure_field(
            sandbox_client,
            sandbox_transport,
            details_table_id,
            label="Note",
            field_type="text",
        ),
    }
    relationship_id, relationship_field_id = _ensure_relationship(
        sandbox_client,
        sandbox_transport,
        parent_table_id=records_table_id,
        child_table_id=details_table_id,
    )
    detail_fields["Related Contract Record"] = relationship_field_id
    record_ids = _ensure_records(sandbox_transport, records_table_id, record_fields)
    _ensure_attachment(
        sandbox_transport,
        records_table_id,
        record_fields["Attachment"],
        record_ids["qbvisor-alpha"],
    )
    _ensure_details(sandbox_transport, details_table_id, detail_fields, record_ids)

    return SandboxContract(
        app_id=sandbox_config.app_id,
        records_table_id=records_table_id,
        details_table_id=details_table_id,
        record_fields=record_fields,
        detail_fields=detail_fields,
        relationship_id=relationship_id,
        record_ids=record_ids,
    )
