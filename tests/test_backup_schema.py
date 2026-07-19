import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from qbvisor._backup import BackupWorkspace, capture_schema


class FakeSchemaClient:
    app = {"id": "app_operations", "name": "Operations", "description": "Sandbox"}
    events = [{"id": 1, "type": "webhook", "name": "Sync"}]
    roles = [{"id": 10, "name": "Viewer"}]
    table_summaries = [{"id": "tbl_projects", "name": "../ Renamed Projects"}]
    table = {"id": "tbl_projects", "name": "../ Renamed Projects", "nextRecordId": 3}
    fields = [{"id": 3, "label": "Record ID#", "fieldType": "recordid"}]
    relationships = [{"id": 4, "parentTableId": "tbl_customers"}]
    report_summaries = [{"id": "7", "name": "All Projects"}]
    report = {"id": 7, "name": "All Projects", "query": "{3.GT.0}"}

    def _ids(self, app_name: str, table_name: None = None) -> tuple[str, None]:
        assert app_name == "Operations"
        assert table_name is None
        return "app_operations", None

    def get_app(self, app_name: str) -> dict[str, Any]:
        return deepcopy(self.app)

    def get_app_events(self, app_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.events)

    def get_app_roles(self, app_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.roles)

    def get_tables_for_app(self, app_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.table_summaries)

    def get_table(self, app_name: str, table_name: str) -> dict[str, Any]:
        assert table_name == "tbl_projects"
        return deepcopy(self.table)

    def get_fields_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.fields)

    def get_all_relationships(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.relationships)

    def get_reports_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        return deepcopy(self.report_summaries)

    def get_report(self, app_name: str, table_name: str, report_id: int) -> dict[str, Any]:
        assert report_id == 7
        return deepcopy(self.report)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def test_schema_capture_preserves_raw_metadata_under_stable_ids(tmp_path):
    client = FakeSchemaClient()
    workspace = BackupWorkspace(tmp_path)

    captured = capture_schema(client, "Operations", workspace)

    assert captured.app_id == "app_operations"
    assert captured.app_name == "Operations"
    assert captured.tables[0].id == "tbl_projects"
    assert captured.tables[0].name == "../ Renamed Projects"
    table_root = tmp_path / "tables" / "tbl_projects"
    assert read_json(tmp_path / "app.json") == client.app
    assert read_json(table_root / "table.json") == {
        "summary": client.table_summaries[0],
        "details": client.table,
    }
    assert read_json(table_root / "fields.json") == client.fields
    assert read_json(table_root / "relationships.json") == client.relationships
    assert read_json(table_root / "reports.json") == {
        "summaries": client.report_summaries,
        "details": [client.report],
    }
    assert not (tmp_path / "Renamed Projects").exists()


def test_schema_artifacts_include_recomputable_integrity_metadata(tmp_path):
    workspace = BackupWorkspace(tmp_path)

    captured = capture_schema(FakeSchemaClient(), "Operations", workspace)

    assert tuple(artifact.path for artifact in workspace.artifacts) == captured.artifacts
    for artifact in workspace.artifacts:
        content = (tmp_path / artifact.path).read_bytes()
        assert artifact.bytes == len(content)
        assert artifact.sha256 == hashlib.sha256(content).hexdigest()
    assert (
        next(artifact for artifact in workspace.artifacts if artifact.kind == "fields").item_count
        == 1
    )


def test_workspace_rejects_escape_paths_without_writing_outside_root(tmp_path):
    workspace = BackupWorkspace(tmp_path / "snapshot")

    with pytest.raises(ValueError, match="within the backup workspace"):
        workspace.write_json("../credentials.json", "application", {"secret": True})

    assert not (tmp_path / "credentials.json").exists()


def test_workspace_rejects_duplicate_artifacts_without_overwriting(tmp_path):
    workspace = BackupWorkspace(tmp_path)
    workspace.write_json("app.json", "application", {"name": "Original"})

    with pytest.raises(ValueError, match="already exists"):
        workspace.write_json("app.json", "application", {"name": "Replacement"})

    assert read_json(tmp_path / "app.json") == {"name": "Original"}


def test_schema_capture_rejects_mismatched_quickbase_identity(tmp_path):
    client = FakeSchemaClient()
    client.app = {**client.app, "id": "different_app"}

    with pytest.raises(ValueError, match="returned app"):
        capture_schema(client, "Operations", BackupWorkspace(tmp_path))
