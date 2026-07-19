"""Versioned models for portable qbvisor application backups."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any, Literal, cast
from uuid import UUID

if TYPE_CHECKING:
    import pandas as pd

BACKUP_FORMAT = "qbvisor-application-backup"
BACKUP_FORMAT_VERSION = 1

AttachmentVersionMode = Literal["all", "latest", "none"]
BackupArtifactKind = Literal[
    "application",
    "events",
    "roles",
    "table",
    "fields",
    "relationships",
    "reports",
    "records",
    "attachment-index",
    "attachment",
]

_ATTACHMENT_VERSION_MODES = frozenset({"all", "latest", "none"})
_ARTIFACT_KINDS = frozenset(
    {
        "application",
        "events",
        "roles",
        "table",
        "fields",
        "relationships",
        "reports",
        "records",
        "attachment-index",
        "attachment",
    }
)


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{key} must be an integer")
    return value


def _required_bool(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise ValueError(f"{key} must be a boolean")
    return value


def _utc_datetime(value: str, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field_name} must be an ISO-8601 UTC timestamp ending in Z")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as error:
        raise ValueError(f"{field_name} must be a valid ISO-8601 UTC timestamp") from error
    if parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError(f"{field_name} must be in UTC")
    return parsed


def _validate_relative_path(value: str, field_name: str = "path") -> None:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ValueError(f"{field_name} must be a non-empty POSIX relative path")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts or path.as_posix() != value:
        raise ValueError(f"{field_name} must stay within the backup directory")


@dataclass(frozen=True, slots=True, kw_only=True)
class BackupOptions:
    """Control application backup completeness and resource usage."""

    attachment_versions: AttachmentVersionMode = "all"
    page_size: int = 1000
    max_attachment_concurrency: int = 4
    fail_on_changes: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.attachment_versions, str):
            raise ValueError("attachment_versions must be a string")
        if self.attachment_versions not in _ATTACHMENT_VERSION_MODES:
            raise ValueError(f"Unsupported attachment version mode: {self.attachment_versions}")
        if not isinstance(self.page_size, int) or isinstance(self.page_size, bool):
            raise ValueError("page_size must be an integer")
        if not 1 <= self.page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")
        if not isinstance(self.max_attachment_concurrency, int) or isinstance(
            self.max_attachment_concurrency, bool
        ):
            raise ValueError("max_attachment_concurrency must be an integer")
        if self.max_attachment_concurrency < 1:
            raise ValueError("max_attachment_concurrency must be at least 1")
        if not isinstance(self.fail_on_changes, bool):
            raise ValueError("fail_on_changes must be a boolean")

    def to_dict(self) -> dict[str, Any]:
        return {
            "attachment_versions": self.attachment_versions,
            "page_size": self.page_size,
            "max_attachment_concurrency": self.max_attachment_concurrency,
            "fail_on_changes": self.fail_on_changes,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BackupOptions:
        if not isinstance(payload, dict):
            raise ValueError("options must be an object")
        return cls(
            attachment_versions=cast(
                AttachmentVersionMode, _required_str(payload, "attachment_versions")
            ),
            page_size=_required_int(payload, "page_size"),
            max_attachment_concurrency=_required_int(payload, "max_attachment_concurrency"),
            fail_on_changes=_required_bool(payload, "fail_on_changes"),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class BackupArtifact:
    """Integrity metadata for one file stored in a backup."""

    path: str
    kind: BackupArtifactKind
    sha256: str
    bytes: int
    item_count: int | None = None

    def __post_init__(self) -> None:
        _validate_relative_path(self.path)
        if not isinstance(self.kind, str):
            raise ValueError("artifact kind must be a string")
        if self.kind not in _ARTIFACT_KINDS:
            raise ValueError(f"Unsupported backup artifact kind: {self.kind}")
        if not isinstance(self.sha256, str):
            raise ValueError("sha256 must be a string")
        if len(self.sha256) != 64 or any(
            character not in "0123456789abcdef" for character in self.sha256
        ):
            raise ValueError("sha256 must be a lowercase hexadecimal SHA-256 digest")
        if not isinstance(self.bytes, int) or isinstance(self.bytes, bool):
            raise ValueError("artifact bytes must be an integer")
        if self.bytes < 0:
            raise ValueError("artifact bytes cannot be negative")
        if self.item_count is not None:
            if not isinstance(self.item_count, int) or isinstance(self.item_count, bool):
                raise ValueError("artifact item_count must be an integer or null")
            if self.item_count < 0:
                raise ValueError("artifact item_count cannot be negative")

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "path": self.path,
            "kind": self.kind,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }
        if self.item_count is not None:
            payload["item_count"] = self.item_count
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BackupArtifact:
        if not isinstance(payload, dict):
            raise ValueError("artifact entries must be objects")
        item_count = payload.get("item_count")
        if item_count is not None and (
            not isinstance(item_count, int) or isinstance(item_count, bool)
        ):
            raise ValueError("item_count must be an integer or null")
        return cls(
            path=_required_str(payload, "path"),
            kind=cast(BackupArtifactKind, _required_str(payload, "kind")),
            sha256=_required_str(payload, "sha256"),
            bytes=_required_int(payload, "bytes"),
            item_count=item_count,
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class BackupTable:
    """Inventory counts and artifacts for one backed-up table."""

    id: str
    name: str
    record_count: int
    attachment_count: int
    artifacts: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.id, str)
            or not isinstance(self.name, str)
            or not self.id
            or not self.name
        ):
            raise ValueError("backup table id and name are required")
        if not isinstance(self.record_count, int) or isinstance(self.record_count, bool):
            raise ValueError("record_count must be an integer")
        if not isinstance(self.attachment_count, int) or isinstance(self.attachment_count, bool):
            raise ValueError("attachment_count must be an integer")
        if self.record_count < 0 or self.attachment_count < 0:
            raise ValueError("backup table counts cannot be negative")
        if not isinstance(self.artifacts, tuple):
            raise ValueError("backup table artifacts must be a tuple of paths")
        if len(set(self.artifacts)) != len(self.artifacts):
            raise ValueError(f"backup table {self.id} contains duplicate artifact paths")
        for artifact_path in self.artifacts:
            _validate_relative_path(artifact_path, "table artifact path")

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "record_count": self.record_count,
            "attachment_count": self.attachment_count,
            "artifacts": list(self.artifacts),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BackupTable:
        if not isinstance(payload, dict):
            raise ValueError("table entries must be objects")
        artifacts = payload.get("artifacts")
        if not isinstance(artifacts, list) or not all(isinstance(item, str) for item in artifacts):
            raise ValueError("table artifacts must be an array of paths")
        return cls(
            id=_required_str(payload, "id"),
            name=_required_str(payload, "name"),
            record_count=_required_int(payload, "record_count"),
            attachment_count=_required_int(payload, "attachment_count"),
            artifacts=tuple(artifacts),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class BackupManifest:
    """Versioned root manifest for a complete application backup."""

    snapshot_id: str
    source_realm: str
    source_app_id: str
    source_app_name: str
    qbvisor_version: str
    started_at: str
    completed_at: str
    options: BackupOptions
    consistent: bool
    changed_tables: tuple[str, ...]
    tables: tuple[BackupTable, ...]
    artifacts: tuple[BackupArtifact, ...]
    format: str = BACKUP_FORMAT
    format_version: int = BACKUP_FORMAT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.format, str):
            raise ValueError("backup format must be a string")
        if self.format != BACKUP_FORMAT:
            raise ValueError(f"Unsupported backup format: {self.format}")
        if not isinstance(self.format_version, int) or isinstance(self.format_version, bool):
            raise ValueError("backup format version must be an integer")
        if self.format_version != BACKUP_FORMAT_VERSION:
            raise ValueError(f"Unsupported backup format version: {self.format_version}")
        try:
            UUID(self.snapshot_id)
        except (TypeError, ValueError, AttributeError) as error:
            raise ValueError("snapshot_id must be a UUID") from error
        for name, value in (
            ("source_realm", self.source_realm),
            ("source_app_id", self.source_app_id),
            ("source_app_name", self.source_app_name),
            ("qbvisor_version", self.qbvisor_version),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} is required")
        started = _utc_datetime(self.started_at, "started_at")
        completed = _utc_datetime(self.completed_at, "completed_at")
        if completed < started:
            raise ValueError("completed_at cannot be earlier than started_at")
        if not isinstance(self.options, BackupOptions):
            raise ValueError("options must be BackupOptions")
        if not isinstance(self.consistent, bool):
            raise ValueError("consistent must be a boolean")
        if not isinstance(self.changed_tables, tuple) or not all(
            isinstance(table_id, str) for table_id in self.changed_tables
        ):
            raise ValueError("changed_tables must be a tuple of table IDs")
        if not isinstance(self.tables, tuple) or not all(
            isinstance(table, BackupTable) for table in self.tables
        ):
            raise ValueError("tables must be a tuple of BackupTable values")
        if not isinstance(self.artifacts, tuple) or not all(
            isinstance(artifact, BackupArtifact) for artifact in self.artifacts
        ):
            raise ValueError("artifacts must be a tuple of BackupArtifact values")

        table_ids = [table.id for table in self.tables]
        if len(set(table_ids)) != len(table_ids):
            raise ValueError("backup manifest contains duplicate table IDs")
        artifact_paths = [artifact.path for artifact in self.artifacts]
        if len(set(artifact_paths)) != len(artifact_paths):
            raise ValueError("backup manifest contains duplicate artifact paths")
        artifact_path_set = set(artifact_paths)
        for table in self.tables:
            missing = set(table.artifacts) - artifact_path_set
            if missing:
                raise ValueError(f"backup table {table.id} references missing artifacts: {missing}")
        if not set(self.changed_tables).issubset(table_ids):
            raise ValueError("changed_tables must reference tables in the manifest")
        if len(set(self.changed_tables)) != len(self.changed_tables):
            raise ValueError("changed_tables cannot contain duplicate table IDs")
        if self.consistent and self.changed_tables:
            raise ValueError("a consistent backup cannot contain changed_tables")

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "format_version": self.format_version,
            "snapshot_id": self.snapshot_id,
            "source": {
                "realm": self.source_realm,
                "app_id": self.source_app_id,
                "app_name": self.source_app_name,
            },
            "qbvisor_version": self.qbvisor_version,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "options": self.options.to_dict(),
            "consistency": {
                "consistent": self.consistent,
                "changed_tables": list(self.changed_tables),
            },
            "tables": [table.to_dict() for table in self.tables],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BackupManifest:
        if not isinstance(payload, dict):
            raise ValueError("backup manifest must be an object")
        source = payload.get("source")
        consistency = payload.get("consistency")
        tables = payload.get("tables")
        artifacts = payload.get("artifacts")
        options = payload.get("options")
        if not isinstance(source, dict):
            raise ValueError("backup manifest source must be an object")
        if not isinstance(consistency, dict):
            raise ValueError("backup manifest consistency must be an object")
        if not isinstance(tables, list):
            raise ValueError("backup manifest tables must be an array")
        if not isinstance(artifacts, list):
            raise ValueError("backup manifest artifacts must be an array")
        if not isinstance(options, dict):
            raise ValueError("backup manifest options must be an object")
        changed_tables = consistency.get("changed_tables")
        if not isinstance(changed_tables, list) or not all(
            isinstance(item, str) for item in changed_tables
        ):
            raise ValueError("changed_tables must be an array of table IDs")
        return cls(
            format=_required_str(payload, "format"),
            format_version=_required_int(payload, "format_version"),
            snapshot_id=_required_str(payload, "snapshot_id"),
            source_realm=_required_str(source, "realm"),
            source_app_id=_required_str(source, "app_id"),
            source_app_name=_required_str(source, "app_name"),
            qbvisor_version=_required_str(payload, "qbvisor_version"),
            started_at=_required_str(payload, "started_at"),
            completed_at=_required_str(payload, "completed_at"),
            options=BackupOptions.from_dict(options),
            consistent=_required_bool(consistency, "consistent"),
            changed_tables=tuple(changed_tables),
            tables=tuple(BackupTable.from_dict(table) for table in tables),
            artifacts=tuple(BackupArtifact.from_dict(artifact) for artifact in artifacts),
        )


@dataclass(frozen=True, slots=True)
class BackupVerification:
    """Summary of a successful backup integrity verification."""

    artifact_count: int
    total_bytes: int


@dataclass(frozen=True, slots=True)
class ApplicationBackup:
    """A completed application backup and its validated manifest."""

    path: Path
    manifest: BackupManifest

    def __post_init__(self) -> None:
        if not isinstance(self.path, Path):
            raise ValueError("backup path must be a pathlib.Path")
        if not isinstance(self.manifest, BackupManifest):
            raise ValueError("backup manifest must be a BackupManifest")

    @classmethod
    def open(cls, path: str | Path) -> ApplicationBackup:
        """Open and validate a completed backup manifest without hashing its files."""
        from ._backup.reader import open_backup

        return open_backup(path)

    def verify(self) -> BackupVerification:
        """Recompute artifact integrity and validate archive cross-references."""
        from ._backup.reader import verify_backup

        return verify_backup(self)

    def table_dataframe(self, table: str) -> pd.DataFrame:
        """Load one backed-up table into a pandas DataFrame using captured labels."""
        from ._backup.reader import table_dataframe

        return table_dataframe(self, table)
