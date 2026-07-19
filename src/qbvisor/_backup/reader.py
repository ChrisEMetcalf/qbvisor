"""Open, verify, and analyze completed application backups."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from ..backup import (
    ApplicationBackup,
    BackupArtifact,
    BackupManifest,
    BackupTable,
    BackupVerification,
)
from ..exceptions import BackupIntegrityError


def open_backup(path: str | Path) -> ApplicationBackup:
    root = Path(path).expanduser().resolve()
    manifest_path = root / "manifest.json"
    try:
        payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"Could not read backup manifest at {manifest_path}") from error
    if not isinstance(payload, dict):
        raise ValueError("backup manifest must contain a JSON object")
    return ApplicationBackup(path=root, manifest=BackupManifest.from_dict(payload))


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _json_line_count(path: Path, issues: list[str]) -> int:
    count = 0
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                issues.append(f"{path.name} contains invalid JSON at line {line_number}")
                continue
            if not isinstance(payload, dict):
                issues.append(f"{path.name} contains a non-object at line {line_number}")
            count += 1
    return count


def _json_item_count(path: Path, artifact: BackupArtifact, issues: list[str]) -> int | None:
    if artifact.kind in {"records", "attachment-index"}:
        return _json_line_count(path, issues)
    if artifact.item_count is None:
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        issues.append(f"{artifact.path} is not valid JSON")
        return None
    if artifact.kind == "reports" and isinstance(payload, dict):
        payload = payload.get("details")
    if not isinstance(payload, list):
        issues.append(f"{artifact.path} does not contain the expected array")
        return None
    return len(payload)


def _verify_table_counts(backup: ApplicationBackup, issues: list[str]) -> None:
    artifacts = {artifact.path: artifact for artifact in backup.manifest.artifacts}
    for table in backup.manifest.tables:
        table_artifacts = [artifacts[path] for path in table.artifacts]
        record_artifacts = [artifact for artifact in table_artifacts if artifact.kind == "records"]
        index_artifacts = [
            artifact for artifact in table_artifacts if artifact.kind == "attachment-index"
        ]
        if len(record_artifacts) != 1 or record_artifacts[0].item_count != table.record_count:
            issues.append(f"table {table.id} record count does not match its records artifact")
        if len(index_artifacts) != 1 or index_artifacts[0].item_count != table.attachment_count:
            issues.append(f"table {table.id} attachment count does not match its index")


def _verify_attachment_indexes(backup: ApplicationBackup, issues: list[str]) -> None:
    attachments = {
        artifact.path: artifact
        for artifact in backup.manifest.artifacts
        if artifact.kind == "attachment"
    }
    indexed_paths: set[str] = set()
    for artifact in backup.manifest.artifacts:
        if artifact.kind != "attachment-index":
            continue
        path = backup.path / artifact.path
        if not path.is_file():
            continue
        table_prefix = artifact.path.removesuffix("attachments.jsonl") + "attachments/"
        try:
            with path.open(encoding="utf-8") as stream:
                for line_number, line in enumerate(stream, start=1):
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not isinstance(entry, dict):
                        continue
                    referenced_path = entry.get("path")
                    if not isinstance(referenced_path, str):
                        issues.append(
                            f"{artifact.path} line {line_number} has an invalid attachment path"
                        )
                        continue
                    if not referenced_path.startswith(table_prefix):
                        issues.append(
                            f"{artifact.path} line {line_number} references another table"
                        )
                        continue
                    referenced = attachments.get(referenced_path)
                    if referenced is None:
                        issues.append(
                            f"{artifact.path} line {line_number} references a missing attachment"
                        )
                        continue
                    if referenced_path in indexed_paths:
                        issues.append(f"attachment is indexed more than once: {referenced_path}")
                    indexed_paths.add(referenced_path)
                    if (
                        entry.get("sha256") != referenced.sha256
                        or entry.get("bytes") != referenced.bytes
                    ):
                        issues.append(
                            f"{artifact.path} line {line_number} has incorrect attachment integrity"
                        )
        except (OSError, UnicodeDecodeError):
            # The general artifact pass already reports unreadable index content.
            continue
    missing = set(attachments) - indexed_paths
    if missing:
        issues.append(f"attachment artifacts are not indexed: {', '.join(sorted(missing))}")


def verify_backup(backup: ApplicationBackup) -> BackupVerification:
    root = backup.path
    issues: list[str] = []
    if not root.is_dir() or root.is_symlink():
        raise BackupIntegrityError((f"backup root is not a regular directory: {root}",))
    try:
        disk_manifest = open_backup(root).manifest
    except ValueError as error:
        issues.append(str(error))
    else:
        if disk_manifest != backup.manifest:
            issues.append("manifest.json changed after this backup was opened")

    expected_paths = {artifact.path for artifact in backup.manifest.artifacts} | {"manifest.json"}
    actual_paths: set[str] = set()
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        if path.is_symlink():
            issues.append(f"backup contains a symbolic link: {relative}")
        elif path.is_file():
            actual_paths.add(relative)
    missing = expected_paths - actual_paths
    unexpected = actual_paths - expected_paths
    if missing:
        issues.append(f"backup files are missing: {', '.join(sorted(missing))}")
    if unexpected:
        issues.append(f"backup contains untracked files: {', '.join(sorted(unexpected))}")

    total_bytes = 0
    for artifact in backup.manifest.artifacts:
        path = root / artifact.path
        if not path.is_file() or path.is_symlink():
            continue
        try:
            digest, size = _hash_file(path)
        except OSError as error:
            issues.append(f"artifact could not be read: {artifact.path} ({error})")
            continue
        total_bytes += size
        if digest != artifact.sha256:
            issues.append(f"artifact hash does not match: {artifact.path}")
        if size != artifact.bytes:
            issues.append(f"artifact byte count does not match: {artifact.path}")
        try:
            item_count = _json_item_count(path, artifact, issues)
        except (OSError, UnicodeDecodeError) as error:
            issues.append(f"artifact content could not be read: {artifact.path} ({error})")
            continue
        if item_count is not None and item_count != artifact.item_count:
            issues.append(f"artifact item count does not match: {artifact.path}")

    _verify_table_counts(backup, issues)
    _verify_attachment_indexes(backup, issues)
    if issues:
        raise BackupIntegrityError(tuple(issues))
    return BackupVerification(
        artifact_count=len(backup.manifest.artifacts),
        total_bytes=total_bytes,
    )


def _resolve_table(backup: ApplicationBackup, value: str) -> BackupTable:
    matches = [
        table
        for table in backup.manifest.tables
        if table.id == value or table.name.casefold() == value.casefold()
    ]
    if not matches:
        available = ", ".join(table.name for table in backup.manifest.tables)
        raise KeyError(f"Table {value!r} is not in this backup. Available: {available}")
    if len(matches) > 1:
        raise KeyError(f"Table name {value!r} is ambiguous; use a table ID")
    return matches[0]


def table_dataframe(backup: ApplicationBackup, table: str) -> pd.DataFrame:
    selected = _resolve_table(backup, table)
    fields_path = backup.path / f"tables/{selected.id}/fields.json"
    records_path = backup.path / f"tables/{selected.id}/records.jsonl"
    fields = json.loads(fields_path.read_text(encoding="utf-8"))
    if not isinstance(fields, list) or not all(isinstance(field, dict) for field in fields):
        raise ValueError(f"Captured fields for table {selected.id} are invalid")
    columns: list[tuple[str, str]] = []
    for field in fields:
        field_id = field.get("id")
        label = field.get("label")
        if (
            not isinstance(field_id, int)
            or isinstance(field_id, bool)
            or not isinstance(label, str)
        ):
            raise ValueError(f"Captured field metadata for table {selected.id} is invalid")
        columns.append((str(field_id), label))
    labels = [label for _, label in columns]
    if len(set(labels)) != len(labels):
        raise ValueError(f"Captured field labels for table {selected.id} are not unique")

    rows: list[dict[str, Any]] = []
    known_ids = {field_id for field_id, _ in columns}
    with records_path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            record = json.loads(line)
            if not isinstance(record, dict):
                raise ValueError(f"Captured record at line {line_number} is not an object")
            unknown = set(record) - known_ids
            if unknown:
                raise ValueError(
                    f"Captured record at line {line_number} contains unknown fields: {unknown}"
                )
            row: dict[str, Any] = {}
            for field_id, label in columns:
                cell = record.get(field_id)
                row[label] = cell.get("value") if isinstance(cell, dict) else None
            rows.append(row)
    return pd.DataFrame.from_records(rows, columns=labels)
