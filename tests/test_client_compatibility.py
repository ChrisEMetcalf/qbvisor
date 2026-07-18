from types import SimpleNamespace
from unittest.mock import Mock

import pandas as pd
import pytest

from qbvisor.client import QuickBaseClient


class FakeMeta:
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
        assert table in {"Projects", "tbl_projects"}
        return "tbl_projects"

    def get_field_id(self, app: str, table: str, label: str) -> int:
        assert app == "app_operations"
        assert table == "tbl_projects"
        return {"Record ID#": 3, "Name": 6, "Status": 7}[label]

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


def test_ids_resolve_names_to_stable_ids(client):
    assert client._ids("Operations") == ("app_operations", None)
    assert client._ids("Operations", "Projects") == ("app_operations", "tbl_projects")


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
            "singularRecordName": "Task",
            "pluralRecordName": "Tasks",
        },
    )


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
