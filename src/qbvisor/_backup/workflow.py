"""Atomic orchestration for complete application backups."""

from __future__ import annotations

import json
import os
import shutil
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from ..backup import ApplicationBackup, BackupManifest, BackupOptions, BackupTable
from ..exceptions import BackupConsistencyError, QuickbaseResponseError
from ..transport import QuickBaseTransport
from .attachments import AttachmentClient, capture_attachments
from .records import RecordClient, capture_records
from .schema import SchemaClient, capture_schema
from .workspace import BackupWorkspace


class BackupClient(SchemaClient, RecordClient, AttachmentClient, Protocol):
    transport: QuickBaseTransport

    def records_modified_since(
        self,
        app_name: str,
        table_name: str,
        after: datetime | str,
        *,
        field_list: Any = None,
        include_details: bool = False,
    ) -> dict[str, Any]: ...


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _package_version() -> str:
    try:
        return version("qbvisor")
    except PackageNotFoundError:
        return "0+unknown"


def _changed_tables(
    client: BackupClient,
    app_name: str,
    table_ids: tuple[str, ...],
    started_at: str,
) -> tuple[str, ...]:
    changed: list[str] = []
    for table_id in table_ids:
        response = client.records_modified_since(app_name, table_id, started_at)
        count = response.get("count")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise QuickbaseResponseError(
                "POST",
                "records/modifiedSince",
                expected="non-negative integer count",
                actual=type(count).__name__,
            )
        if count:
            changed.append(table_id)
    return tuple(changed)


def _write_manifest(root: Path, manifest: BackupManifest) -> None:
    destination = root / "manifest.json"
    content = (
        json.dumps(
            manifest.to_dict(),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_bytes(content)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def create_application_backup(
    client: BackupClient,
    app_name: str,
    output_dir: str | Path,
    *,
    options: BackupOptions,
) -> ApplicationBackup:
    """Capture, inventory, and atomically publish one complete application backup."""
    if not isinstance(options, BackupOptions):
        raise ValueError("options must be BackupOptions")
    app_id, _ = client._ids(app_name)
    if not app_id.isalnum():
        raise ValueError(f"Quickbase returned an unsafe application ID: {app_id!r}")

    started = datetime.now(UTC)
    started_at = _timestamp(started)
    snapshot_id = str(uuid4())
    output_root = Path(output_dir).expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    staging = output_root / f".qbvisor-{snapshot_id}.tmp"
    final = output_root / (f"{app_id}-{started.strftime('%Y%m%dT%H%M%SZ')}-{snapshot_id}")
    staging.mkdir(exist_ok=False)
    workspace = BackupWorkspace(staging)

    try:
        schema = capture_schema(client, app_name, workspace)
        records = capture_records(
            client,
            schema,
            workspace,
            page_size=options.page_size,
        )
        attachments = capture_attachments(
            client,
            schema,
            workspace,
            mode=options.attachment_versions,
            max_concurrency=options.max_attachment_concurrency,
        )
        table_ids = tuple(table.id for table in schema.tables)
        changed_tables = _changed_tables(client, app_name, table_ids, started_at)
        if changed_tables and options.fail_on_changes:
            raise BackupConsistencyError(changed_tables)

        records_by_table = {table.id: table for table in records.tables}
        attachments_by_table = {table.id: table for table in attachments.tables}
        artifacts = tuple(sorted(workspace.artifacts, key=lambda artifact: artifact.path))
        backup_tables: list[BackupTable] = []
        for table in schema.tables:
            table_records = records_by_table[table.id]
            table_attachments = attachments_by_table[table.id]
            prefix = f"tables/{table.id}/"
            table_artifacts = tuple(
                artifact.path for artifact in artifacts if artifact.path.startswith(prefix)
            )
            backup_tables.append(
                BackupTable(
                    id=table.id,
                    name=table.name,
                    record_count=table_records.record_count,
                    attachment_count=table_attachments.attachment_count,
                    artifacts=table_artifacts,
                )
            )

        realm = client.transport.realm_hostname
        if not isinstance(realm, str) or not realm:
            raise ValueError("Quickbase transport does not have a realm hostname")
        manifest = BackupManifest(
            snapshot_id=snapshot_id,
            source_realm=realm,
            source_app_id=schema.app_id,
            source_app_name=schema.app_name,
            qbvisor_version=_package_version(),
            started_at=started_at,
            completed_at=_timestamp(datetime.now(UTC)),
            options=options,
            consistent=not changed_tables,
            changed_tables=changed_tables,
            tables=tuple(backup_tables),
            artifacts=artifacts,
        )
        _write_manifest(staging, manifest)
        if final.exists():
            raise FileExistsError(f"Backup destination already exists: {final}")
        os.replace(staging, final)
        return ApplicationBackup(path=final, manifest=manifest)
    except Exception as error:
        try:
            shutil.rmtree(staging)
        except FileNotFoundError:
            pass
        except OSError as cleanup_error:
            error.add_note(f"Could not remove incomplete backup {staging}: {cleanup_error}")
        raise
