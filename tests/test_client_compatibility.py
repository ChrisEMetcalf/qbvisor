import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pandas as pd
import pytest

import qbvisor.client as client_module
from qbvisor.client import QuickBaseClient
from qbvisor.exceptions import QuickbaseResponseError
from qbvisor.models import RelationshipSummary
from qbvisor.transport import RetryPolicy


class FakeMeta:
    invalidated_fields: list[tuple[str, str]]

    def __init__(self):
        self.invalidated_fields = []

    cache = {
        "Operations": {
            "tables": {
                "Projects": {
                    "id": "tbl_projects",
                    "size": 2,
                    "fields": {},
                }
            }
        }
    }

    def get_app_id(self, app: str) -> str:
        assert app in {"Operations", "app_operations"}
        return "app_operations"

    def get_table_id(self, app: str, table: str) -> str:
        assert app == "app_operations"
        assert table in {"Projects", "tbl_projects", "Customers", "tbl_customers"}
        return "tbl_customers" if table in {"Customers", "tbl_customers"} else "tbl_projects"

    def get_field_id(self, app: str, table: str, label: str) -> int:
        assert app == "app_operations"
        fields = {
            "tbl_projects": {"Record ID#": 3, "Name": 6, "Status": 7, "Hours": 8},
            "tbl_customers": {"Record ID#": 3, "Customer Name": 6},
        }
        return fields[table][label]

    def get_field_map(self, app: str, table: str) -> dict[str, dict[str, object]]:
        assert app == "app_operations"
        assert table == "tbl_projects"
        return {
            "Record ID#": {"id": 3, "type": "recordid"},
            "Name": {"id": 6, "type": "text"},
            "Status": {"id": 7, "type": "text"},
        }

    def get_table(self, app: str, table: str) -> dict[str, object]:
        assert app == "app_operations"
        assert table == "tbl_projects"
        return {"id": "tbl_projects", "size": 2, "fields": {}}

    def normalize_app(self, app: str) -> str:
        assert app == "app_operations"
        return "Operations"

    def invalidate_fields(self, app: str, table: str) -> None:
        self.invalidated_fields.append((app, table))


@pytest.fixture
def client() -> QuickBaseClient:
    instance = QuickBaseClient.__new__(QuickBaseClient)
    instance.meta = FakeMeta()
    instance.transport = SimpleNamespace(
        base_url="https://api.quickbase.com/v1",
        headers={"Authorization": "QB-USER-TOKEN test"},
    )
    instance.logger = Mock()
    instance._request = Mock()
    return instance


def test_client_accepts_caller_owned_transport(monkeypatch):
    monkeypatch.setenv("QB_APP_IDS", '{"Operations": "app_operations"}')
    injected_transport = Mock()

    instance = QuickBaseClient(transport=injected_transport)
    instance.close()

    assert instance.transport is injected_transport
    assert instance.meta.transport is injected_transport
    injected_transport.close.assert_not_called()


def test_client_context_closes_transport_it_creates(monkeypatch):
    monkeypatch.setenv("QB_APP_IDS", '{"Operations": "app_operations"}')
    owned_transport = Mock()
    monkeypatch.setattr(client_module, "QuickBaseTransport", Mock(return_value=owned_transport))

    with QuickBaseClient() as instance:
        assert instance.transport is owned_transport

    owned_transport.close.assert_called_once_with()


def test_ids_resolve_names_to_stable_ids(client):
    assert client._ids("Operations") == ("app_operations", None)
    assert client._ids("Operations", "Projects") == ("app_operations", "tbl_projects")


def test_request_rejects_an_undocumented_top_level_shape():
    instance = QuickBaseClient.__new__(QuickBaseClient)
    instance.transport = Mock()
    instance.transport.get.return_value = {"tables": []}
    instance.logger = Mock()

    with pytest.raises(QuickbaseResponseError, match="expected JSON array, got dict"):
        QuickBaseClient._request(instance, "GET", "tables", response_type=list)


def test_create_app_preserves_existing_request_shape(client):
    client._request.return_value = {"id": "new_app"}

    result = client.create_app(
        "New App",
        description="Created by qbvisor",
        assign_token=True,
        variables=[{"name": "Environment", "value": "Sandbox"}],
        security_properties={"allowClone": False},
    )

    assert result == {"id": "new_app"}
    client._request.assert_called_once_with(
        method="POST",
        path="apps",
        json_body={
            "name": "New App",
            "assignToken": True,
            "description": "Created by qbvisor",
            "variables": [{"name": "Environment", "value": "Sandbox"}],
            "securityProperties": {"allowClone": False},
        },
    )


def test_update_app_requires_at_least_one_change(client):
    with pytest.raises(ValueError, match="No update parameters provided"):
        client.update_app("Operations")


def test_app_events_and_roles_use_documented_array_responses(client):
    client._request.side_effect = [
        [{"type": "webhook", "name": "Sync"}],
        [{"id": 10, "name": "Viewer"}],
    ]

    events = client.get_app_events("Operations")
    roles = client.get_app_roles("Operations")

    assert events == [{"type": "webhook", "name": "Sync"}]
    assert roles == [{"id": 10, "name": "Viewer"}]
    assert client._request.call_args_list == [
        call(
            method="GET",
            path="apps/app_operations/events",
            response_type=list,
        ),
        call(
            method="GET",
            path="apps/app_operations/roles",
            response_type=list,
        ),
    ]


def test_create_table_preserves_existing_request_shape(client):
    client._request.return_value = {"id": "tbl_new"}

    result = client.create_table(
        "Operations",
        "Tasks",
        description="Project tasks",
        singular_record_name="Task",
        plural_record_name="Tasks",
    )

    assert result == {"id": "tbl_new"}
    client._request.assert_called_once_with(
        method="POST",
        path="tables",
        params={"appId": "app_operations"},
        json_body={
            "name": "Tasks",
            "description": "Project tasks",
            "singleRecordName": "Task",
            "pluralRecordName": "Tasks",
        },
    )


def test_list_tables_preserves_documented_top_level_array(client):
    client._request.return_value = [{"id": "tbl_projects", "name": "Projects"}]

    result = client.get_tables_for_app("Operations")

    assert result == [{"id": "tbl_projects", "name": "Projects"}]
    client._request.assert_called_once_with(
        method="GET",
        path="tables",
        params={"appId": "app_operations"},
        response_type=list,
    )


def test_field_mutations_put_table_id_in_query_parameter(client):
    client._request.return_value = {"id": 8, "label": "Owner"}

    assert client.create_field("Operations", "Projects", "Owner", "text") == {
        "id": 8,
        "label": "Owner",
    }
    client._request.assert_called_once_with(
        method="POST",
        path="fields",
        params={"tableId": "tbl_projects"},
        json_body={"label": "Owner", "fieldType": "text"},
    )

    client._request.reset_mock(return_value=True)
    client._request.return_value = {"deletedFieldIds": [7]}
    assert client.delete_fields("Operations", "Projects", ["Status"]) == {"deletedFieldIds": [7]}
    client._request.assert_called_once_with(
        method="DELETE",
        path="fields",
        params={"tableId": "tbl_projects"},
        json_body={"fieldIds": [7]},
    )


def test_field_usage_resolves_names_and_preserves_pagination(client):
    client._request.side_effect = [
        [{"field": {"id": 6, "name": "Name"}, "usage": {}}],
        [{"field": {"id": 7, "name": "Status"}, "usage": {}}],
        [{"field": {"id": 6, "name": "Name"}, "usage": {}}],
    ]

    usage = client.get_fields_usage("Operations", "Projects", skip=5, top=25)
    by_label = client.get_field_usage("Operations", "Projects", "Status")
    by_id = client.get_field_usage("Operations", "Projects", 6)

    assert usage[0]["field"]["id"] == 6
    assert by_label[0]["field"]["id"] == 7
    assert by_id[0]["field"]["id"] == 6
    assert client._request.call_args_list == [
        call(
            method="GET",
            path="fields/usage",
            params={"tableId": "tbl_projects", "skip": 5, "top": 25},
            response_type=list,
        ),
        call(
            method="GET",
            path="fields/usage/7",
            params={"tableId": "tbl_projects"},
            response_type=list,
        ),
        call(
            method="GET",
            path="fields/usage/6",
            params={"tableId": "tbl_projects"},
            response_type=list,
        ),
    ]


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [({"skip": -1}, "skip cannot be negative"), ({"top": 0}, "top must be at least 1")],
)
def test_field_usage_rejects_invalid_pagination(client, kwargs, message):
    with pytest.raises(ValueError, match=message):
        client.get_fields_usage("Operations", "Projects", **kwargs)

    client._request.assert_not_called()


def test_relationship_mutations_match_documented_paths_and_body(client):
    client._request.return_value = {"id": 9}

    assert client.create_relationship(
        "Operations",
        "Projects",
        "Customers",
        foreign_key_label="Related Customer",
    ) == {"id": 9}
    client._request.assert_called_once_with(
        method="POST",
        path="tables/tbl_projects/relationship",
        json_body={
            "parentTableId": "tbl_customers",
            "foreignKeyField": {"label": "Related Customer"},
        },
    )

    client._request.reset_mock(return_value=True)
    client._request.return_value = {"relationshipId": 7}
    assert client.delete_relationship("Operations", "Projects", "Status") == 7
    client._request.assert_called_once_with(
        method="DELETE",
        path="tables/tbl_projects/relationship/7",
    )


def test_update_relationship_resolves_typed_lookup_and_summary_fields(client):
    client._request.side_effect = [
        {
            "relationships": [
                {"id": 7, "parentTableId": "tbl_customers", "childTableId": "tbl_projects"}
            ]
        },
        {"id": 7, "parentTableId": "tbl_customers", "childTableId": "tbl_projects"},
    ]

    result = client.update_relationship(
        "Operations",
        "Projects",
        "Status",
        lookup_fields=["Customer Name", 3],
        summary_fields=[
            RelationshipSummary("SUM", "Hours", label="Total Hours"),
            RelationshipSummary("COUNT", label="Project Count", where="{7.EX.'Active'}"),
        ],
    )

    assert result["id"] == 7
    assert client._request.call_args_list == [
        call(method="GET", path="tables/tbl_projects/relationships"),
        call(
            method="POST",
            path="tables/tbl_projects/relationship/7",
            json_body={
                "lookupFieldIds": [6, 3],
                "summaryFields": [
                    {
                        "accumulationType": "SUM",
                        "summaryFid": 8,
                        "label": "Total Hours",
                    },
                    {
                        "accumulationType": "COUNT",
                        "label": "Project Count",
                        "where": "{7.EX.'Active'}",
                    },
                ],
            },
        ),
    ]
    assert client.meta.invalidated_fields == [
        ("app_operations", "tbl_projects"),
        ("app_operations", "tbl_customers"),
    ]


def test_update_relationship_accepts_ids_without_metadata_lookup(client):
    client._request.return_value = {
        "id": 7,
        "parentTableId": "tbl_customers",
        "childTableId": "tbl_projects",
    }

    client.update_relationship("Operations", "Projects", 7, lookup_fields=[6])

    client._request.assert_called_once_with(
        method="POST",
        path="tables/tbl_projects/relationship/7",
        json_body={"lookupFieldIds": [6]},
    )


def test_update_relationship_requires_a_schema_change(client):
    with pytest.raises(ValueError, match="at least one lookup field or summary field"):
        client.update_relationship("Operations", "Projects", 7)

    client._request.assert_not_called()


def test_query_records_translates_labels_to_field_ids(client):
    client._request.return_value = {"data": []}

    result = client.query_records(
        "Operations",
        "Projects",
        select_fields=["Name", "Status"],
        where="{7.EX.'Active'}",
        sort_by=[("Name", "asc")],
        group_by=["Status"],
        skip=10,
        top=50,
    )

    assert result == {"data": []}
    client._request.assert_called_once_with(
        method="POST",
        path="records/query",
        json_body={
            "from": "tbl_projects",
            "select": [6, 7],
            "where": "{7.EX.'Active'}",
            "sortBy": [{"fieldId": 6, "order": "ASC"}],
            "groupBy": [{"fieldId": 7, "grouping": "equal-values"}],
            "options": {"skip": 10, "top": 50},
        },
        retry_policy=RetryPolicy.SAFE,
    )


def test_query_dataframe_uses_quickbase_labels_as_columns(client):
    client._request.return_value = {
        "fields": [{"id": 6, "label": "Name"}, {"id": 7, "label": "Status"}],
        "data": [
            {"6": {"value": "Migration"}, "7": {"value": "Active"}},
            {"6": {"value": "Backup"}, "7": {"value": "Queued"}},
        ],
    }

    result = client.query_dataframe("Operations", "Projects", ["Name", "Status"])

    expected = pd.DataFrame(
        [
            {"Name": "Migration", "Status": "Active"},
            {"Name": "Backup", "Status": "Queued"},
        ]
    )
    pd.testing.assert_frame_equal(result, expected)
    client._request.assert_called_once_with(
        method="POST",
        path="records/query",
        json_body={
            "from": "tbl_projects",
            "select": [6, 7],
            "options": {"skip": 0, "top": 1000},
        },
        retry_policy=RetryPolicy.SAFE,
    )


def test_run_report_marks_read_like_post_as_safe(client):
    client._request.return_value = {
        "fields": [{"id": 6, "label": "Name"}],
        "data": [{"6": {"value": "Migration"}}],
    }

    result = client.run_report("Operations", "Projects", 12)

    pd.testing.assert_frame_equal(result, pd.DataFrame([{"Name": "Migration"}]))
    client._request.assert_called_once_with(
        method="POST",
        path="reports/12/run",
        params={"tableId": "tbl_projects", "skip": 0, "top": 1000},
        retry_policy=RetryPolicy.SAFE,
    )


def test_run_formula_marks_read_like_post_as_safe(client):
    client._request.return_value = {"result": "formula result"}

    result = client.run_formula("Operations", "Projects", "[Name]", record_id=101)

    assert result == {"result": "formula result"}
    client._request.assert_called_once_with(
        method="POST",
        path="formula/run",
        json_body={"from": "tbl_projects", "formula": "[Name]", "rid": 101},
        retry_policy=RetryPolicy.SAFE,
    )


def test_upsert_records_translates_labels_and_reports_success(client):
    client._request.return_value = {
        "metadata": {
            "createdRecordIds": [101],
            "totalNumberOfRecordsProcessed": 1,
        }
    }

    result = client.upsert_records(
        "Operations",
        "Projects",
        [{"Name": "Migration", "Status": "Active"}],
        merge_field_label="Name",
        fields_to_return=["Record ID#", "Status"],
    )

    assert result == {"success": True, "createdRecordIds": [101], "totalProcessed": 1}
    client._request.assert_called_once_with(
        method="POST",
        path="records",
        json_body={
            "to": "tbl_projects",
            "data": [{"6": {"value": "Migration"}, "7": {"value": "Active"}}],
            "mergeFieldId": 6,
            "fieldsToReturn": [3, 7],
        },
    )


def test_upsert_records_preserves_partial_failure_details(client):
    client._request.return_value = {
        "metadata": {
            "lineErrors": {"2": "Invalid value"},
            "createdRecordIds": [101],
            "totalNumberOfRecordsProcessed": 2,
        }
    }

    result = client.upsert_records(
        "Operations",
        "Projects",
        [{"Name": "Valid"}, {"Name": "Invalid"}],
    )

    assert result == {
        "success": False,
        "partial": True,
        "lineErrors": {"2": "Invalid value"},
        "createdRecordIds": [101],
        "totalProcessed": 2,
    }


def test_delete_records_accepts_query_or_record_ids(client):
    client._request.return_value = {"numberDeleted": 2}

    assert client.delete_records("Operations", "Projects", [101, 102]) == 2
    client._request.assert_called_once_with(
        method="DELETE",
        path="records",
        json_body={"from": "tbl_projects", "where": [101, 102]},
    )


def test_delete_records_rejects_unsupported_filter_types(client):
    with pytest.raises(ValueError, match="must be either"):
        client.delete_records("Operations", "Projects", 101)


def test_report_parser_preserves_field_labels():
    response = {
        "fields": [{"id": 6, "label": "Name"}, {"id": 7, "label": "Status"}],
        "data": [{"6": {"value": "Backup"}, "7": {"value": "Complete"}}],
    }

    assert QuickBaseClient._parse_report(response) == [{"Name": "Backup", "Status": "Complete"}]


def test_file_attachment_fields_are_resolved_from_metadata(client):
    client.meta.get_field_map = Mock(
        return_value={
            "Name": {"id": 6, "type": "text"},
            "Document": {"id": 8, "type": "file"},
        }
    )

    assert client.get_file_attachment_fields("Operations", "Projects") == ["Document"]


def test_record_export_caps_batches_without_skipping_records(client, tmp_path):
    client.meta.get_table = Mock(return_value={"id": "tbl_projects", "size": 2500})
    client._gather_chunks = AsyncMock(return_value=[[{"Name": "Migration"}]])

    output = client.download_records_to_csv(
        "Operations",
        "Projects",
        str(tmp_path),
        chunk_size=2500,
        record_limit=2500,
        max_concurrency=3,
    )

    assert output.startswith(str(tmp_path / "Projects_"))
    assert output.endswith(".csv")
    client._gather_chunks.assert_awaited_once_with(
        "tbl_projects",
        [3, 6, 7],
        "{3.GT.'0'}",
        [(0, 1000), (1000, 1000), (2000, 500)],
        3,
    )


def test_record_export_chunk_preserves_quickbase_field_labels(client):
    async_transport = SimpleNamespace(
        post_json=AsyncMock(
            return_value={
                "fields": [{"id": 6, "label": "Name"}, {"id": 7, "label": "Status"}],
                "data": [
                    {"6": {"value": "Migration"}, "7": {"value": "Active"}},
                ],
            }
        )
    )
    body = {"from": "tbl_projects", "select": [6, 7], "options": {"skip": 0, "top": 1000}}

    rows = asyncio.run(client._fetch_chunk(async_transport, body, 0))

    assert rows == [{"Name": "Migration", "Status": "Active"}]
    async_transport.post_json.assert_awaited_once_with(
        "records/query",
        json_body=body,
        headers={"Accept-Encoding": "gzip"},
        retry_policy=RetryPolicy.SAFE,
    )


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"chunk_size": 0}, "chunk_size must be at least 1"),
        ({"max_concurrency": 0}, "max_concurrency must be at least 1"),
        ({"record_limit": -1}, "record_limit cannot be negative"),
    ],
)
def test_record_export_rejects_invalid_batch_configuration(client, tmp_path, kwargs, message):
    with pytest.raises(ValueError, match=message):
        client.download_records_to_csv("Operations", "Projects", str(tmp_path), **kwargs)


def test_record_export_respects_an_explicit_zero_record_limit(client, tmp_path):
    client._gather_chunks = AsyncMock(return_value=[])

    output = client.download_records_to_csv(
        "Operations",
        "Projects",
        str(tmp_path),
        record_limit=0,
    )

    assert output == ""
    client._gather_chunks.assert_awaited_once_with(
        "tbl_projects",
        [3, 6, 7],
        "{3.GT.'0'}",
        [],
        4,
    )
