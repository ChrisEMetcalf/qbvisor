"""Capture application and table schema without reshaping Quickbase metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from .workspace import BackupWorkspace


class SchemaClient(Protocol):
    def _ids(self, app_name: str, table_name: None = None) -> tuple[str, None]: ...

    def get_app(self, app_name: str) -> dict[str, Any]: ...

    def get_app_events(self, app_name: str) -> list[dict[str, Any]]: ...

    def get_app_roles(self, app_name: str) -> list[dict[str, Any]]: ...

    def get_tables_for_app(self, app_name: str) -> list[dict[str, Any]]: ...

    def get_table(self, app_name: str, table_name: str) -> dict[str, Any]: ...

    def get_fields_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]: ...

    def get_all_relationships(self, app_name: str, table_name: str) -> list[dict[str, Any]]: ...

    def get_reports_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]: ...

    def get_report(self, app_name: str, table_name: str, report_id: int) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class CapturedTable:
    id: str
    name: str
    fields: tuple[dict[str, Any], ...]
    artifacts: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CapturedSchema:
    app_id: str
    app_name: str
    tables: tuple[CapturedTable, ...]
    artifacts: tuple[str, ...]


def _required_identifier(payload: dict[str, Any], key: str, resource: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"Quickbase {resource} is missing a valid {key}")
    return value


def _report_id(summary: dict[str, Any], table_id: str) -> int:
    value = summary.get("id")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    raise ValueError(f"Quickbase report in table {table_id} is missing a valid id")


def capture_schema(
    client: SchemaClient,
    app_name: str,
    workspace: BackupWorkspace,
) -> CapturedSchema:
    """Capture raw application metadata and every table's schema artifacts."""
    app_id, _ = client._ids(app_name)
    app = client.get_app(app_name)
    response_app_id = app.get("id")
    if response_app_id is not None and response_app_id != app_id:
        raise ValueError(f"Quickbase returned app {response_app_id!r} while backing up {app_id!r}")
    resolved_app_name = _required_identifier(app, "name", "application")
    events = client.get_app_events(app_name)
    roles = client.get_app_roles(app_name)
    table_summaries = client.get_tables_for_app(app_name)

    artifact_paths = [
        workspace.write_json("app.json", "application", app).path,
        workspace.write_json("events.json", "events", events, item_count=len(events)).path,
        workspace.write_json("roles.json", "roles", roles, item_count=len(roles)).path,
    ]
    captured_tables: list[CapturedTable] = []

    for summary in table_summaries:
        table_id = _required_identifier(summary, "id", "table summary")
        table_name = _required_identifier(summary, "name", f"table {table_id}")
        details = client.get_table(app_name, table_id)
        response_table_id = details.get("id")
        if response_table_id is not None and response_table_id != table_id:
            raise ValueError(
                f"Quickbase returned table {response_table_id!r} while backing up {table_id!r}"
            )
        fields = client.get_fields_for_table(app_name, table_id)
        relationships = client.get_all_relationships(app_name, table_id)
        report_summaries = client.get_reports_for_table(app_name, table_id)
        reports = [
            client.get_report(app_name, table_id, _report_id(report, table_id))
            for report in report_summaries
        ]

        prefix = f"tables/{table_id}"
        table_artifacts = (
            workspace.write_json(
                f"{prefix}/table.json",
                "table",
                {"summary": summary, "details": details},
            ).path,
            workspace.write_json(
                f"{prefix}/fields.json", "fields", fields, item_count=len(fields)
            ).path,
            workspace.write_json(
                f"{prefix}/relationships.json",
                "relationships",
                relationships,
                item_count=len(relationships),
            ).path,
            workspace.write_json(
                f"{prefix}/reports.json",
                "reports",
                {"summaries": report_summaries, "details": reports},
                item_count=len(reports),
            ).path,
        )
        artifact_paths.extend(table_artifacts)
        captured_tables.append(
            CapturedTable(
                id=table_id,
                name=table_name,
                fields=tuple(fields),
                artifacts=table_artifacts,
            )
        )

    return CapturedSchema(
        app_id=app_id,
        app_name=resolved_app_name,
        tables=tuple(captured_tables),
        artifacts=tuple(artifact_paths),
    )
