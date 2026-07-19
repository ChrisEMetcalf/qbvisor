"""Archive file attachment versions referenced by captured table records."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Protocol

from ..async_transport import AsyncQuickBaseTransport
from ..backup import AttachmentVersionMode, BackupArtifact
from ..exceptions import QuickbaseResponseError
from ..helpers import sanitize_filenames
from ..transport import QuickBaseTransport
from .schema import CapturedSchema, CapturedTable
from .workspace import BackupWorkspace, JsonLinesArtifactWriter


class AttachmentClient(Protocol):
    transport: QuickBaseTransport


class AsyncFileTransport(Protocol):
    async def __aenter__(self) -> AsyncFileTransport: ...

    async def __aexit__(self, *_: object) -> None: ...

    async def get_file(self, path: str) -> bytes: ...


@dataclass(frozen=True, slots=True)
class AttachmentJob:
    table_id: str
    record_id: int
    field_id: int
    version_number: int
    file_name: str
    path: str
    metadata: dict[str, Any]


@dataclass(frozen=True, slots=True)
class CapturedAttachmentTable:
    id: str
    attachment_count: int
    index_artifact: str


@dataclass(frozen=True, slots=True)
class CapturedAttachments:
    tables: tuple[CapturedAttachmentTable, ...]


def _safe_filename(value: str, field_id: int, version_number: int) -> str:
    clean = sanitize_filenames(value)
    clean = "".join(character for character in clean if " " <= character != "\x7f")
    clean = clean.strip().strip(".")
    if not clean:
        clean = f"fid{field_id}_v{version_number}.bin"
    while len(clean.encode("utf-8")) > 200:
        clean = clean[:-1]
    return clean


def _file_field_ids(table: CapturedTable) -> tuple[int, ...]:
    field_ids: list[int] = []
    for field in table.fields:
        if field.get("fieldType") != "file":
            continue
        field_id = field.get("id")
        if not isinstance(field_id, int) or isinstance(field_id, bool):
            raise ValueError(f"Quickbase file field in table {table.id} is missing a valid id")
        field_ids.append(field_id)
    return tuple(field_ids)


def _record_id(record: dict[str, Any], table_id: str, line_number: int) -> int:
    cell = record.get("3")
    value = cell.get("value") if isinstance(cell, dict) else None
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(
            f"records.jsonl for table {table_id} has an invalid Record ID# at line {line_number}"
        )
    return value


def _selected_versions(
    versions: Any,
    mode: AttachmentVersionMode,
    *,
    table_id: str,
    record_id: int,
    field_id: int,
) -> tuple[dict[str, Any], ...]:
    if not isinstance(versions, list) or not all(isinstance(version, dict) for version in versions):
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected="file attachment versions array",
            actual=type(versions).__name__,
        )
    numbered: list[tuple[int, dict[str, Any]]] = []
    for version in versions:
        number = version.get("versionNumber")
        if not isinstance(number, int) or isinstance(number, bool) or number < 1:
            raise ValueError(
                f"Attachment version for {table_id}/{record_id}/{field_id} has an invalid number"
            )
        numbered.append((number, version))
    numbered.sort(key=lambda item: item[0])
    if len({number for number, _ in numbered}) != len(numbered):
        raise ValueError(
            f"Attachment versions for {table_id}/{record_id}/{field_id} contain duplicates"
        )
    if mode == "latest" and numbered:
        numbered = [numbered[-1]]
    return tuple(version for _, version in numbered)


def _attachment_jobs(
    table: CapturedTable,
    workspace: BackupWorkspace,
    mode: AttachmentVersionMode,
) -> Iterator[AttachmentJob]:
    if mode == "none":
        return
    field_ids = _file_field_ids(table)
    if not field_ids:
        return
    records_path = workspace.root / "tables" / table.id / "records.jsonl"
    with records_path.open(encoding="utf-8") as records:
        for line_number, line in enumerate(records, start=1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(
                    f"records.jsonl for table {table.id} is invalid at line {line_number}"
                ) from error
            if not isinstance(record, dict):
                raise ValueError(f"records.jsonl for table {table.id} must contain objects")
            record_id = _record_id(record, table.id, line_number)
            for field_id in field_ids:
                cell = record.get(str(field_id))
                value = cell.get("value") if isinstance(cell, dict) else None
                if value in (None, ""):
                    continue
                if not isinstance(value, dict):
                    raise QuickbaseResponseError(
                        "POST",
                        "records/query",
                        expected="file attachment value object",
                        actual=type(value).__name__,
                    )
                versions = _selected_versions(
                    value.get("versions", []),
                    mode,
                    table_id=table.id,
                    record_id=record_id,
                    field_id=field_id,
                )
                for version in versions:
                    version_number = int(version["versionNumber"])
                    raw_name = version.get("fileName") or value.get("fileName")
                    file_name = (
                        raw_name
                        if isinstance(raw_name, str) and raw_name
                        else f"fid{field_id}_v{version_number}.bin"
                    )
                    safe_name = _safe_filename(file_name, field_id, version_number)
                    path = (
                        f"tables/{table.id}/attachments/{record_id}/"
                        f"{field_id}/{version_number}/{safe_name}"
                    )
                    yield AttachmentJob(
                        table_id=table.id,
                        record_id=record_id,
                        field_id=field_id,
                        version_number=version_number,
                        file_name=file_name,
                        path=path,
                        metadata=version,
                    )


def _index_entry(job: AttachmentJob, artifact: BackupArtifact) -> dict[str, Any]:
    return {
        "record_id": job.record_id,
        "field_id": job.field_id,
        "version_number": job.version_number,
        "file_name": job.file_name,
        "path": artifact.path,
        "sha256": artifact.sha256,
        "bytes": artifact.bytes,
        "metadata": job.metadata,
    }


async def _download_job(
    transport: AsyncFileTransport,
    workspace: BackupWorkspace,
    job: AttachmentJob,
) -> tuple[AttachmentJob, BackupArtifact]:
    endpoint = f"files/{job.table_id}/{job.record_id}/{job.field_id}/{job.version_number}"
    content = await transport.get_file(endpoint)
    return job, workspace.write_bytes(job.path, "attachment", content)


async def _capture_table_attachments(
    client: AttachmentClient,
    table: CapturedTable,
    workspace: BackupWorkspace,
    mode: AttachmentVersionMode,
    max_concurrency: int,
    index_writer: JsonLinesArtifactWriter,
    transport_factory: Callable[[QuickBaseTransport], AsyncFileTransport],
) -> int:
    jobs = iter(_attachment_jobs(table, workspace, mode))
    count = 0
    async with transport_factory(client.transport) as transport:
        while True:
            batch: list[AttachmentJob] = []
            for _ in range(max_concurrency):
                try:
                    batch.append(next(jobs))
                except StopIteration:
                    break
            if not batch:
                return count
            results = await asyncio.gather(
                *(_download_job(transport, workspace, job) for job in batch)
            )
            for job, artifact in results:
                index_writer.write(_index_entry(job, artifact))
                count += 1


def capture_attachments(
    client: AttachmentClient,
    schema: CapturedSchema,
    workspace: BackupWorkspace,
    *,
    mode: AttachmentVersionMode,
    max_concurrency: int,
    transport_factory: Callable[[QuickBaseTransport], AsyncFileTransport] = AsyncQuickBaseTransport,
) -> CapturedAttachments:
    """Archive selected attachment versions with bounded concurrent downloads."""
    if mode not in {"all", "latest", "none"}:
        raise ValueError(f"Unsupported attachment version mode: {mode}")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be at least 1")
    captured: list[CapturedAttachmentTable] = []
    for table in schema.tables:
        index_path = f"tables/{table.id}/attachments.jsonl"
        with workspace.json_lines_writer(index_path, "attachment-index") as writer:
            count = asyncio.run(
                _capture_table_attachments(
                    client,
                    table,
                    workspace,
                    mode,
                    max_concurrency,
                    writer,
                    transport_factory,
                )
            )
        captured.append(
            CapturedAttachmentTable(
                id=table.id,
                attachment_count=count,
                index_artifact=writer.artifact.path,
            )
        )
    return CapturedAttachments(tables=tuple(captured))
