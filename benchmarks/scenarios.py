"""Deterministic, credential-free benchmark scenarios for qbvisor.

The scenarios deliberately exercise production behavior at the narrowest stable
boundary.  A few repository benchmarks use private implementation functions where
there is no public streaming or planning API yet; those scenarios say so in their
descriptions.  They are benchmark infrastructure, not supported qbvisor APIs.
"""

from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import logging
import re
import shutil
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, SimpleNamespace
from typing import Any, cast

import pandas as pd

from benchmarks.core import BenchmarkScenario, PreparedScenario
from benchmarks.profiles import BenchmarkProfile
from qbvisor import (
    ApplicationBackup,
    AppSpec,
    FieldSpec,
    QuickBaseClient,
    TableSpec,
)
from qbvisor._backup.workspace import BackupWorkspace
from qbvisor._records.pagination import iter_record_pages_by_id
from qbvisor._records.upsert import (
    MAX_UPSERT_PAYLOAD_BYTES,
    UpsertBatch,
    _json_payload_size,
    plan_upsert_batches,
)
from qbvisor.backup import BackupManifest, BackupOptions, BackupTable
from qbvisor.metadata import QuickBaseMetaCache

_APP_ID = "appbenchmark"
_APP_NAME = "Benchmark"
_TABLE_ID = "tblrecords"
_TABLE_NAME = "Records"
_RECORD_ID_FIELD = ("Record ID#", 3)
_ATTACHMENT_CONCURRENCY = 4
_CURSOR_PATTERN = re.compile(r"\{3\.GT\.(\d+)\}")


def _remove_tree(path: Path) -> None:
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def _shape_int(shape: Mapping[str, str | int | float | bool | None], key: str) -> int:
    value = shape[key]
    if not isinstance(value, int) or isinstance(value, bool):
        raise AssertionError(f"Benchmark shape {key!r} is not an integer")
    return value


def _temporary_work_root(prefix: str) -> Path:
    """Create a private work root, falling back when a sandbox narrows system temp writes."""
    roots = (
        lambda: Path(tempfile.mkdtemp(prefix=prefix, dir=Path.cwd())),
        lambda: Path(tempfile.mkdtemp(prefix=prefix)),
    )
    last_error: OSError | None = None
    for create_root in roots:
        try:
            root = create_root()
        except OSError as error:
            last_error = error
            continue
        probe = root / ".qbvisor-write-probe" / "nested"
        try:
            probe.mkdir(parents=True)
            (probe / "probe.bin").write_bytes(b"qbvisor")
        except OSError as error:
            last_error = error
            try:
                _remove_tree(root)
            except OSError as cleanup_error:
                error.add_note(f"Could not remove unusable benchmark root {root}: {cleanup_error}")
            continue
        _remove_tree(probe.parent)
        return root
    assert last_error is not None
    raise last_error


def _business_fields(count: int) -> tuple[tuple[str, int], ...]:
    return tuple((f"Field {index}", index + 5) for index in range(1, count + 1))


def _field_definitions(count: int) -> tuple[tuple[str, int], ...]:
    return (_RECORD_ID_FIELD, *_business_fields(count))


def _record(record_id: int, fields: Sequence[tuple[str, int]]) -> dict[str, Any]:
    values: dict[str, Any] = {"3": {"value": record_id}}
    for _label, field_id in fields:
        if field_id != 3:
            values[str(field_id)] = {"value": f"record-{record_id:08d}-field-{field_id}"}
    return values


class _DatasetMetadata:
    def __init__(self, fields: Sequence[tuple[str, int]]):
        self._ids_by_label = {label: field_id for label, field_id in fields}
        self._field_map = {
            label: {"id": field_id, "type": "recordid" if field_id == 3 else "text"}
            for label, field_id in fields
        }
        self.app_ids = {_APP_NAME: _APP_ID}
        self.cache = {
            _APP_NAME: {
                "tables": {
                    _TABLE_NAME: {"id": _TABLE_ID, "name": _TABLE_NAME},
                }
            }
        }

    def get_app_id(self, app: str) -> str:
        assert app in {_APP_NAME, _APP_ID}
        return _APP_ID

    def get_table_id(self, app: str, table: str) -> str:
        assert app in {_APP_NAME, _APP_ID}
        assert table in {_TABLE_NAME, _TABLE_ID}
        return _TABLE_ID

    def get_field_id(self, app_id: str, table_id: str, label: str) -> int:
        assert app_id == _APP_ID
        assert table_id == _TABLE_ID
        return self._ids_by_label[label]

    def get_field_map(self, app_id: str, table_id: str) -> dict[str, dict[str, Any]]:
        assert app_id == _APP_ID
        assert table_id == _TABLE_ID
        return self._field_map

    def get_table(self, app_id: str, table_id: str) -> dict[str, Any]:
        assert app_id == _APP_ID
        assert table_id == _TABLE_ID
        return {"id": _TABLE_ID, "name": _TABLE_NAME}

    def normalize_app(self, app_id: str) -> str:
        assert app_id == _APP_ID
        return _APP_NAME


class _RecordDatasetClient:
    """Stateful in-process Quickbase response boundary for record workloads."""

    def __init__(
        self,
        records: Sequence[dict[str, Any]],
        fields: Sequence[tuple[str, int]],
        page_size: int,
    ):
        self.records = tuple(records)
        self.fields = tuple(fields)
        self.page_size = page_size
        self.meta: _DatasetMetadata | QuickBaseMetaCache = _DatasetMetadata(fields)
        self.logger = logging.getLogger("qbvisor.benchmarks")
        self.calls: list[dict[str, Any]] = []
        self.active_queries = 0
        self.max_active_queries = 0
        self.responses = self._prepare_responses()

    def _prepare_responses(self) -> dict[tuple[int, tuple[int, ...]], dict[str, Any]]:
        """Prebuild fake HTTP response objects so timing measures qbvisor, not a fake server."""
        field_by_id = {field_id: label for label, field_id in self.fields}
        field_shapes = {
            tuple(field_by_id),
            (*tuple(field_id for field_id in field_by_id if field_id != 3), 3),
        }
        responses: dict[tuple[int, tuple[int, ...]], dict[str, Any]] = {}
        for selected in field_shapes:
            for start in range(0, len(self.records), self.page_size):
                page = self.records[start : start + self.page_size]
                responses[(start, selected)] = {
                    "data": [
                        {str(field_id): record[str(field_id)] for field_id in selected}
                        for record in page
                    ],
                    "fields": [
                        {"id": field_id, "label": field_by_id[field_id]} for field_id in selected
                    ],
                    "metadata": {
                        "numFields": len(selected),
                        "numRecords": len(page),
                        "totalRecords": len(self.records) - start,
                        "skip": 0,
                    },
                }
        return responses

    def _ids(self, app_name: str, table_name: str | None = None) -> tuple[str, str | None]:
        return QuickBaseClient._ids(cast(QuickBaseClient, self), app_name, table_name)

    def _query_records_by_ids(
        self,
        table_id: str,
        *,
        select_fields: Sequence[int] | None = None,
        where: str | None = None,
        sort_by: Sequence[tuple[int, str]] | None = None,
        group_by: Sequence[int] | None = None,
        skip: int = 0,
        top: int | None = 1000,
    ) -> dict[str, Any]:
        self.active_queries += 1
        self.max_active_queries = max(self.max_active_queries, self.active_queries)
        try:
            selected = tuple(select_fields or (field_id for _label, field_id in self.fields))
            self.calls.append(
                {
                    "table_id": table_id,
                    "select_fields": selected,
                    "where": where,
                    "sort_by": tuple(sort_by) if sort_by is not None else None,
                    "group_by": tuple(group_by) if group_by is not None else None,
                    "skip": skip,
                    "top": top,
                }
            )

            cursor_matches = _CURSOR_PATTERN.findall(where or "")
            cursor = int(cursor_matches[-1]) if cursor_matches else 0
            if skip != 0:
                raise AssertionError("keyset benchmark fake does not support offset reads")
            if top not in {None, self.page_size}:
                raise AssertionError("keyset benchmark fake received an unexpected page size")
            return self.responses[(cursor, selected)]
        finally:
            self.active_queries -= 1


def _dataset_client(profile: BenchmarkProfile) -> _RecordDatasetClient:
    fields = _field_definitions(profile.field_count)
    records = tuple(_record(record_id, fields) for record_id in range(1, profile.record_count + 1))
    return _RecordDatasetClient(records, fields, profile.page_size)


class _MetadataTransport:
    """Deterministic transport boundary used by the real metadata cache."""

    def __init__(self, fields: Sequence[tuple[str, int]], record_count: int):
        self.fields = tuple(fields)
        self.record_count = record_count
        self.calls: list[tuple[str, dict[str, Any] | None]] = []

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        self.calls.append((path, params))
        if path == "tables":
            return [{"id": _TABLE_ID, "name": _TABLE_NAME}]
        if path == f"tables/{_TABLE_ID}":
            return {"id": _TABLE_ID, "name": _TABLE_NAME, "nextRecordId": self.record_count + 1}
        if path == "fields":
            return [
                {
                    "id": field_id,
                    "label": label,
                    "fieldType": "recordid" if field_id == 3 else "text",
                }
                for label, field_id in self.fields
            ]
        raise AssertionError(f"unexpected metadata request: {path}")


def _metadata_cache(transport: _MetadataTransport) -> QuickBaseMetaCache:
    # QuickBaseMetaCache's public constructor reads QB_APP_IDS. Repository benchmarks
    # install the same deterministic initial state directly so fixture creation neither
    # requires nor mutates the process environment.
    cache = QuickBaseMetaCache.__new__(QuickBaseMetaCache)
    cache.app_ids = {_APP_NAME: _APP_ID}
    cache.name_map = {_APP_NAME.lower(): _APP_NAME}
    cache.transport = transport  # type: ignore[assignment]
    cache.cache = {}
    cache._table_catalogs = {}
    cache._tables_by_id = {}
    cache._tables_by_name = {}
    cache._loaded_field_maps = set()
    cache._field_labels = {}
    return cache


def _dataframe_client(
    profile: BenchmarkProfile,
    *,
    warm_metadata: bool,
) -> tuple[_RecordDatasetClient, _MetadataTransport]:
    client = _dataset_client(profile)
    transport = _MetadataTransport(client.fields, profile.record_count)
    client.meta = _metadata_cache(transport)
    if warm_metadata:
        client.meta.get_field_map(_APP_ID, _TABLE_ID)
    return client, transport


def _validate_keyset_calls(client: _RecordDatasetClient, record_count: int) -> None:
    assert client.max_active_queries == 1
    assert all(call["skip"] == 0 for call in client.calls)
    assert all(call["sort_by"] == ((3, "ASC"),) for call in client.calls)
    cursors = [
        int(matches[-1]) if (matches := _CURSOR_PATTERN.findall(call["where"] or "")) else 0
        for call in client.calls
    ]
    assert cursors[0] == 0
    assert cursors == sorted(set(cursors))
    assert cursors[-1] < record_count


@dataclass(frozen=True, slots=True)
class _PaginationResult:
    record_count: int
    page_count: int
    record_id_sum: int


@dataclass(slots=True)
class _PreparedKeysetPagination:
    client: _RecordDatasetClient
    shape: Mapping[str, str | int | float | bool | None]
    unit_count: int
    unit_label: str = "records"

    def run(self) -> _PaginationResult:
        record_count = 0
        page_count = 0
        record_id_sum = 0
        for page in iter_record_pages_by_id(
            self.client,
            _TABLE_ID,
            select_fields=tuple(field_id for _label, field_id in self.client.fields),
            page_size=self.client.page_size,
        ):
            page_count += 1
            record_count += len(page)
            record_id_sum += sum(record["3"]["value"] for record in page)
        return _PaginationResult(record_count, page_count, record_id_sum)

    def validate(self, result: object) -> None:
        assert isinstance(result, _PaginationResult)
        assert result.record_count == self.unit_count
        assert (
            result.page_count
            == (self.unit_count + self.client.page_size - 1) // self.client.page_size
        )
        assert result.record_id_sum == self.unit_count * (self.unit_count + 1) // 2
        _validate_keyset_calls(self.client, self.unit_count)

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        assert isinstance(result, _PaginationResult)
        return {
            "records": result.record_count,
            "pages": result.page_count,
        }

    def cleanup(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class KeysetPaginationScenario:
    name: str = "record-keyset-pagination"
    description: str = (
        "Consume stable Record ID# pages through the private pagination boundary; "
        "the private import is intentional repository benchmark infrastructure."
    )
    track_peak_memory: bool = True

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        return _PreparedKeysetPagination(
            client=_dataset_client(profile),
            shape={
                "record_count": profile.record_count,
                "page_size": profile.page_size,
                "selected_field_count": profile.field_count + 1,
                "stable_record_id_order": True,
            },
            unit_count=profile.record_count,
        )


@dataclass(slots=True)
class _PreparedDataFrame:
    client: _RecordDatasetClient
    metadata_transport: _MetadataTransport
    metadata_calls_before: int
    expected_timed_metadata_calls: int
    selected_labels: list[str]
    shape: Mapping[str, str | int | float | bool | None]
    unit_count: int
    unit_label: str = "records"

    def run(self) -> pd.DataFrame:
        return QuickBaseClient.query_dataframe(
            cast(QuickBaseClient, self.client),
            _APP_NAME,
            _TABLE_NAME,
            self.selected_labels,
        )

    def validate(self, result: object) -> None:
        assert isinstance(result, pd.DataFrame)
        assert result.shape == (self.unit_count, len(self.selected_labels))
        assert list(result.columns) == self.selected_labels
        assert result.iloc[0, 0] == "record-00000001-field-6"
        assert result.iloc[-1, -1] == (
            f"record-{self.unit_count:08d}-field-{self.client.fields[-1][1]}"
        )
        assert all(3 in call["select_fields"] for call in self.client.calls)
        assert len(self.metadata_transport.calls) - self.metadata_calls_before == (
            self.expected_timed_metadata_calls
        )
        _validate_keyset_calls(self.client, self.unit_count)

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        assert isinstance(result, pd.DataFrame)
        return {
            "records": len(result.index),
            "cells": int(result.size),
            "pages": len(self.client.calls),
            "metadata_requests": (len(self.metadata_transport.calls) - self.metadata_calls_before),
        }

    def cleanup(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class DataFrameMaterializationScenario:
    metadata_cache: str
    name: str
    description: str = (
        "Materialize a complete unsorted query through public query_dataframe(); complete "
        "in-memory materialization is an intentional analysis-oriented contract."
    )
    track_peak_memory: bool = True

    def __post_init__(self) -> None:
        if self.metadata_cache not in {"cold", "warm"}:
            raise ValueError("metadata_cache must be cold or warm")

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        client, metadata_transport = _dataframe_client(
            profile,
            warm_metadata=self.metadata_cache == "warm",
        )
        selected_labels = [label for label, _field_id in _business_fields(profile.field_count)]
        return _PreparedDataFrame(
            client=client,
            metadata_transport=metadata_transport,
            metadata_calls_before=len(metadata_transport.calls),
            expected_timed_metadata_calls=3 if self.metadata_cache == "cold" else 0,
            selected_labels=selected_labels,
            shape={
                "record_count": profile.record_count,
                "column_count": profile.field_count,
                "page_size": profile.page_size,
                "record_id_cursor_injected": True,
                "complete_materialization": True,
                "metadata_cache": self.metadata_cache,
                "timed_metadata_requests": 3 if self.metadata_cache == "cold" else 0,
            },
            unit_count=profile.record_count,
        )


@dataclass(slots=True)
class _PreparedCsvExport:
    client: _RecordDatasetClient
    output_root: Path
    shape: Mapping[str, str | int | float | bool | None]
    unit_count: int
    unit_label: str = "records"

    def run(self) -> str:
        return QuickBaseClient.download_records_to_csv(
            cast(QuickBaseClient, self.client),
            _APP_NAME,
            _TABLE_NAME,
            str(self.output_root),
            chunk_size=self.client.page_size,
        )

    def validate(self, result: object) -> None:
        assert isinstance(result, str) and result
        path = Path(result)
        assert path.is_file()
        with path.open(newline="", encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            assert reader.fieldnames == [label for label, _field_id in self.client.fields]
            count = 0
            first: dict[str, str] | None = None
            last: dict[str, str] | None = None
            for row in reader:
                first = row if first is None else first
                last = row
                count += 1
        assert count == self.unit_count
        assert first is not None and first["Record ID#"] == "1"
        assert last is not None and last["Record ID#"] == str(self.unit_count)
        assert not list(self.output_root.glob(".*.tmp"))
        _validate_keyset_calls(self.client, self.unit_count)

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        assert isinstance(result, str) and result
        return {
            "records": self.unit_count,
            "pages": len(self.client.calls),
            "published_bytes": Path(result).stat().st_size,
        }

    def cleanup(self) -> None:
        _remove_tree(self.output_root.parent)


@dataclass(frozen=True, slots=True)
class SequentialCsvExportScenario:
    name: str = "csv-sequential-export"
    description: str = (
        "Write a public CSV export through stable sequential Record ID# pages and atomic "
        "publication; max_concurrency is intentionally not a scaling control."
    )
    track_peak_memory: bool = True

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        work_root = _temporary_work_root("qbvisor-benchmark-csv-")
        try:
            output_root = work_root / "exports"
            output_root.mkdir()
            return _PreparedCsvExport(
                client=_dataset_client(profile),
                output_root=output_root,
                shape={
                    "record_count": profile.record_count,
                    "column_count": profile.field_count + 1,
                    "chunk_size": profile.page_size,
                    "sequential_page_reads": True,
                    "atomic_publication": True,
                },
                unit_count=profile.record_count,
            )
        except BaseException as error:
            try:
                _remove_tree(work_root)
            except OSError as cleanup_error:
                error.add_note(
                    f"Could not remove incomplete CSV benchmark fixture: {cleanup_error}"
                )
            raise


@dataclass(slots=True)
class _PreparedUpsertPlanning:
    records: list[dict[str, Any]]
    request_template: dict[str, Any]
    shape: Mapping[str, str | int | float | bool | None]
    unit_count: int
    unit_label: str = "records"

    def run(self) -> tuple[UpsertBatch, ...]:
        return plan_upsert_batches(
            self.records,
            request_template=self.request_template,
            max_payload_bytes=MAX_UPSERT_PAYLOAD_BYTES,
        )

    def validate(self, result: object) -> None:
        assert isinstance(result, tuple) and result
        assert all(isinstance(batch, UpsertBatch) for batch in result)
        batches = result
        assert sum(len(batch.records) for batch in batches) == self.unit_count
        assert [(batch.start_line, batch.end_line) for batch in batches] == [
            (
                1 + sum(len(previous.records) for previous in batches[:index]),
                sum(len(previous.records) for previous in batches[: index + 1]),
            )
            for index in range(len(batches))
        ]
        assert all(batch.payload_bytes <= MAX_UPSERT_PAYLOAD_BYTES for batch in batches)
        assert all(
            batch.payload_bytes == _json_payload_size(batch.json_body(self.request_template))
            for batch in batches
        )
        for left, right in zip(batches, batches[1:], strict=False):
            combined = [*left.records, *right.records]
            assert (
                _json_payload_size({**self.request_template, "data": combined})
                > MAX_UPSERT_PAYLOAD_BYTES
            )
        total_payload = _shape_int(self.shape, "unbatched_payload_bytes")
        assert (len(batches) > 1) == (total_payload > MAX_UPSERT_PAYLOAD_BYTES)

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        assert isinstance(result, tuple) and result
        batches = cast(tuple[UpsertBatch, ...], result)
        return {
            "records": sum(len(batch.records) for batch in batches),
            "batches": len(batches),
            "serialized_bytes": sum(batch.payload_bytes for batch in batches),
        }

    def cleanup(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class UpsertBatchPlanningScenario:
    name: str = "upsert-batch-planning"
    description: str = (
        "Plan the fewest contiguous upsert requests through the private payload planner at "
        "Quickbase's real 40,000,000-byte ceiling; no mutation is sent."
    )
    track_peak_memory: bool = True

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        width = profile.upsert_value_bytes
        records = [
            {
                "6": {"value": f"{index:08d}" + "x" * max(0, width - 8)},
                "7": {"value": index},
            }
            for index in range(1, profile.upsert_record_count + 1)
        ]
        request_template = {
            "to": _TABLE_ID,
            "mergeFieldId": 7,
            "fieldsToReturn": [3, 7],
        }
        total_payload = _json_payload_size({**request_template, "data": records})
        return _PreparedUpsertPlanning(
            records=records,
            request_template=request_template,
            shape={
                "record_count": profile.upsert_record_count,
                "value_bytes_per_record": profile.upsert_value_bytes,
                "payload_limit_bytes": MAX_UPSERT_PAYLOAD_BYTES,
                "unbatched_payload_bytes": total_payload,
                "planning_only": True,
            },
            unit_count=profile.upsert_record_count,
        )


class _NoOpLogger:
    def info(self, _message: str) -> None:
        return None

    def warning(self, _message: str) -> None:
        return None

    def error(self, _message: str) -> None:
        return None


class _DeterministicAsyncFileTransport:
    """Yield until one full permitted wave is active, then return local bytes."""

    def __init__(
        self,
        payload_seeds: Mapping[str, int],
        attachment_bytes: int,
        concurrency: int,
    ):
        self.payload_seeds = payload_seeds
        self.attachment_bytes = attachment_bytes
        self.release_size = min(len(payload_seeds), concurrency)
        self.release = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def get_file(self, path: str) -> bytes:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        if self.active == self.release_size:
            self.release.set()
        await self.release.wait()
        await asyncio.sleep(0)
        try:
            # Allocate the response body inside the timed action. Retaining prepared bytes
            # here would make peak-memory results exclude the transfer's primary allocation.
            return bytes((self.payload_seeds[path],)) * self.attachment_bytes
        finally:
            self.active -= 1


@dataclass(frozen=True, slots=True)
class _AttachmentResult:
    outcomes: tuple[dict[str, Any], ...]
    max_active: int


@dataclass(slots=True)
class _PreparedAttachmentTransfer:
    client: Any
    transport: _DeterministicAsyncFileTransport
    destinations: tuple[tuple[str, Path], ...]
    output_root: Path
    concurrency: int
    shape: Mapping[str, str | int | float | bool | None]
    unit_count: int
    unit_label: str = "attachments"

    async def _download_all(self) -> _AttachmentResult:
        semaphore = asyncio.Semaphore(self.concurrency)
        tasks = [
            QuickBaseClient._async_download_attachment(
                self.client,
                cast(Any, self.transport),
                semaphore,
                url,
                destination,
                record_id=index,
                file_name=destination.name,
            )
            for index, (url, destination) in enumerate(self.destinations, start=1)
        ]
        outcomes = await asyncio.gather(*tasks)
        errors = [error for _result, error in outcomes if error is not None]
        assert not errors
        return _AttachmentResult(
            outcomes=tuple(result for result, _error in outcomes),
            max_active=self.transport.max_active,
        )

    def run(self) -> _AttachmentResult:
        return asyncio.run(self._download_all())

    def validate(self, result: object) -> None:
        assert isinstance(result, _AttachmentResult)
        assert len(result.outcomes) == self.unit_count
        assert all(outcome["status"] == "downloaded" for outcome in result.outcomes)
        assert all(
            outcome["bytes_written"] == _shape_int(self.shape, "bytes_per_attachment")
            for outcome in result.outcomes
        )
        assert result.max_active == min(self.concurrency, self.unit_count)
        assert result.max_active <= self.concurrency
        for url, destination in self.destinations:
            expected = bytes((self.transport.payload_seeds[url],)) * self.transport.attachment_bytes
            assert destination.read_bytes() == expected
        assert not list(self.output_root.glob(".*.part"))

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        assert isinstance(result, _AttachmentResult)
        return {
            "attachments": len(result.outcomes),
            "bytes": sum(int(outcome["bytes_written"]) for outcome in result.outcomes),
        }

    def cleanup(self) -> None:
        _remove_tree(self.output_root.parent)


@dataclass(frozen=True, slots=True)
class ConcurrentAttachmentTransferScenario:
    name: str = "attachment-concurrent-transfer"
    description: str = (
        "Transfer attachments through the private atomic-download boundary with a deterministic "
        "in-process transport, real files, and a four-transfer semaphore bound."
    )
    track_peak_memory: bool = True

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        work_root = _temporary_work_root("qbvisor-benchmark-attachments-")
        try:
            output_root = work_root / "attachments"
            output_root.mkdir()
            payload_seeds: dict[str, int] = {}
            destinations: list[tuple[str, Path]] = []
            for index in range(1, profile.attachment_count + 1):
                url = f"files/{_TABLE_ID}/{index}/8/1"
                payload_seeds[url] = index % 251
                destinations.append((url, output_root / f"{index}_attachment-{index:04d}.bin"))
            concurrency = min(_ATTACHMENT_CONCURRENCY, profile.attachment_count)
            return _PreparedAttachmentTransfer(
                client=SimpleNamespace(logger=_NoOpLogger()),
                transport=_DeterministicAsyncFileTransport(
                    payload_seeds,
                    profile.attachment_bytes,
                    concurrency,
                ),
                destinations=tuple(destinations),
                output_root=output_root,
                concurrency=concurrency,
                shape={
                    "attachment_count": profile.attachment_count,
                    "bytes_per_attachment": profile.attachment_bytes,
                    "max_concurrency": concurrency,
                    "atomic_file_publication": True,
                },
                unit_count=profile.attachment_count,
            )
        except BaseException as error:
            try:
                _remove_tree(work_root)
            except OSError as cleanup_error:
                error.add_note(
                    f"Could not remove incomplete attachment benchmark fixture: {cleanup_error}"
                )
            raise


def _populate_local_backup(
    backup_root: Path,
    profile: BenchmarkProfile,
) -> tuple[Path, dict[str, str]]:
    backup_root.mkdir()
    workspace = BackupWorkspace(backup_root)
    fields = [
        {"id": 3, "label": "Record ID#", "fieldType": "recordid"},
        *[
            {"id": field_id, "label": label, "fieldType": "text"}
            for label, field_id in _business_fields(profile.field_count)
        ],
    ]
    fields_artifact = workspace.write_json(
        f"tables/{_TABLE_ID}/fields.json",
        "fields",
        fields,
        item_count=len(fields),
    )
    with workspace.json_lines_writer(
        f"tables/{_TABLE_ID}/records.jsonl", "records"
    ) as records_writer:
        field_definitions = _field_definitions(profile.field_count)
        for record_id in range(1, profile.record_count + 1):
            records_writer.write(_record(record_id, field_definitions))
    records_artifact = records_writer.artifact
    attachments_artifact = workspace.write_json_lines(
        f"tables/{_TABLE_ID}/attachments.jsonl",
        "attachment-index",
        (),
    )
    artifacts = tuple(sorted(workspace.artifacts, key=lambda item: item.path))
    manifest = BackupManifest(
        snapshot_id="00000000-0000-0000-0000-000000000024",
        source_realm="benchmark.quickbase.invalid",
        source_app_id=_APP_ID,
        source_app_name=_APP_NAME,
        qbvisor_version="0.4.0-benchmark",
        started_at="2026-01-01T00:00:00.000000Z",
        completed_at="2026-01-01T00:00:01.000000Z",
        options=BackupOptions(attachment_versions="none", page_size=profile.page_size),
        consistent=True,
        changed_tables=(),
        tables=(
            BackupTable(
                id=_TABLE_ID,
                name=_TABLE_NAME,
                record_count=profile.record_count,
                attachment_count=0,
                artifacts=tuple(
                    artifact.path
                    for artifact in (fields_artifact, records_artifact, attachments_artifact)
                ),
            ),
        ),
        artifacts=artifacts,
    )
    manifest_path = backup_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    hashes = {
        path.relative_to(backup_root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in backup_root.rglob("*")
        if path.is_file()
    }
    return backup_root, hashes


def _create_local_backup(profile: BenchmarkProfile) -> tuple[Path, dict[str, str]]:
    work_root = _temporary_work_root("qbvisor-benchmark-backup-")
    try:
        return _populate_local_backup(work_root / "backup", profile)
    except BaseException as error:
        try:
            _remove_tree(work_root)
        except OSError as cleanup_error:
            error.add_note(f"Could not remove incomplete backup benchmark fixture: {cleanup_error}")
        raise


@dataclass(frozen=True, slots=True)
class _BackupReadResult:
    backup: ApplicationBackup
    artifact_count: int
    total_bytes: int
    frame: pd.DataFrame


@dataclass(slots=True)
class _PreparedBackupRead:
    backup_root: Path
    fixture_hashes: Mapping[str, str]
    shape: Mapping[str, str | int | float | bool | None]
    unit_count: int
    unit_label: str = "records"

    def run(self) -> _BackupReadResult:
        backup = ApplicationBackup.open(self.backup_root)
        verification = backup.verify()
        frame = backup.table_dataframe(_TABLE_NAME)
        return _BackupReadResult(
            backup=backup,
            artifact_count=verification.artifact_count,
            total_bytes=verification.total_bytes,
            frame=frame,
        )

    def validate(self, result: object) -> None:
        assert isinstance(result, _BackupReadResult)
        assert result.backup.path == self.backup_root
        assert result.artifact_count == _shape_int(self.shape, "artifact_count")
        assert result.total_bytes > 0
        assert result.frame.shape == (
            self.unit_count,
            _shape_int(self.shape, "column_count"),
        )
        assert list(result.frame["Record ID#"]) == list(range(1, self.unit_count + 1))
        current_hashes = {
            path.relative_to(self.backup_root).as_posix(): hashlib.sha256(
                path.read_bytes()
            ).hexdigest()
            for path in self.backup_root.rglob("*")
            if path.is_file()
        }
        assert current_hashes == dict(self.fixture_hashes)
        assert not any(path.is_symlink() for path in self.backup_root.rglob("*"))

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        assert isinstance(result, _BackupReadResult)
        return {
            "records": len(result.frame.index),
            "artifacts": result.artifact_count,
            "artifact_bytes": result.total_bytes,
        }

    def cleanup(self) -> None:
        _remove_tree(self.backup_root.parent)


@dataclass(frozen=True, slots=True)
class BackupReadScenario:
    name: str = "backup-read-verify-dataframe"
    description: str = (
        "Open, cryptographically verify, and materialize one table from a valid deterministic "
        "local backup through the public ApplicationBackup reader."
    )
    track_peak_memory: bool = True

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        backup_root, fixture_hashes = _create_local_backup(profile)
        return _PreparedBackupRead(
            backup_root=backup_root,
            fixture_hashes=fixture_hashes,
            shape={
                "record_count": profile.record_count,
                "column_count": profile.field_count + 1,
                "artifact_count": 3,
                "integrity_verification": True,
                "complete_materialization": True,
            },
            unit_count=profile.record_count,
        )


class _SchemaMetadataClient:
    def __init__(
        self,
        app: dict[str, Any],
        tables: Sequence[dict[str, Any]],
        fields_by_table: Mapping[str, Sequence[dict[str, Any]]],
    ):
        self.meta = SimpleNamespace(app_ids={_APP_NAME: _APP_ID})
        self.app = app
        self.tables = list(tables)
        self.fields_by_table = {
            table_id: list(fields) for table_id, fields in fields_by_table.items()
        }
        self.calls: list[dict[str, Any]] = []

    def _request(
        self,
        *,
        method: str,
        path: str,
        params: Mapping[str, Any] | None = None,
        response_type: type[Any] | None = None,
        **_kwargs: Any,
    ) -> Any:
        request_params = dict(params or {})
        call: dict[str, Any] = {
            "method": method,
            "path": path,
            "params": request_params,
            "response_type": response_type,
        }
        self.calls.append(call)
        if method != "GET":
            raise AssertionError(f"schema planning attempted mutation method {method}")
        if path.startswith("records") or path.startswith("files"):
            raise AssertionError(f"schema planning attempted data read {path}")
        if path == f"apps/{_APP_ID}":
            return self.app
        if path == "tables":
            return self.tables
        if path == "fields":
            return self.fields_by_table[str(request_params["tableId"])]
        raise AssertionError(f"unexpected schema metadata request: {path}")


def _schema_fixture(
    profile: BenchmarkProfile,
) -> tuple[AppSpec, _SchemaMetadataClient]:
    table_specs: list[TableSpec] = []
    remote_tables: list[dict[str, Any]] = []
    fields_by_table: dict[str, list[dict[str, Any]]] = {}
    for table_index in range(1, profile.schema_table_count + 1):
        table_id = f"tbl{table_index:04d}"
        table_name = f"Table {table_index}"
        field_specs: list[FieldSpec] = []
        remote_fields: list[dict[str, Any]] = []
        for field_index in range(1, profile.schema_fields_per_table + 1):
            label = f"Field {field_index}"
            field_specs.append(
                FieldSpec(
                    key=f"field_{field_index}",
                    label=label,
                    field_type="text",
                )
            )
            remote_fields.append(
                {
                    "id": field_index + 5,
                    "label": label,
                    "fieldType": "text",
                    "properties": {},
                }
            )
        table_specs.append(
            TableSpec(
                key=f"table_{table_index}",
                name=table_name,
                fields=field_specs,
            )
        )
        remote_tables.append({"id": table_id, "name": table_name})
        fields_by_table[table_id] = remote_fields
    spec = AppSpec(key="benchmark", name=_APP_NAME, tables=table_specs)
    client = _SchemaMetadataClient(
        {"id": _APP_ID, "name": _APP_NAME},
        remote_tables,
        fields_by_table,
    )
    return spec, client


@dataclass(slots=True)
class _PreparedSchemaPlan:
    spec: AppSpec
    client: _SchemaMetadataClient
    state_path: Path
    work_root: Path
    shape: Mapping[str, str | int | float | bool | None]
    unit_count: int
    unit_label: str = "schema resources"

    def run(self) -> object:
        return QuickBaseClient.plan_app(
            cast(QuickBaseClient, self.client),
            self.spec,
            state_path=self.state_path,
        )

    def validate(self, result: object) -> None:
        from qbvisor import SchemaPlan

        assert isinstance(result, SchemaPlan)
        assert len(result.changes) == self.unit_count
        assert result.action_counts == {
            "create": 0,
            "update": 0,
            "unchanged": self.unit_count,
            "conflict": 0,
        }
        assert result.can_apply
        assert result.quickbase_change_count == 0
        assert not self.state_path.exists()
        assert len(self.client.calls) == _shape_int(self.shape, "metadata_request_count")
        assert all(call["method"] == "GET" for call in self.client.calls)
        assert all(not call["path"].startswith(("records", "files")) for call in self.client.calls)

    def workload_observations(self, result: object) -> Mapping[str, int | float]:
        from qbvisor import SchemaPlan

        assert isinstance(result, SchemaPlan)
        return {
            "resources": len(result.changes),
            "metadata_requests": len(self.client.calls),
        }

    def cleanup(self) -> None:
        _remove_tree(self.work_root)


@dataclass(frozen=True, slots=True)
class ReadOnlySchemaPlanningScenario:
    name: str = "schema-readonly-planning"
    description: str = (
        "Build a public declarative schema plan from deterministic matching metadata; planning "
        "is intentionally read-only and never reads table records."
    )
    track_peak_memory: bool = True

    def prepare(self, profile: BenchmarkProfile) -> PreparedScenario:
        spec, client = _schema_fixture(profile)
        work_root = _temporary_work_root("qbvisor-benchmark-schema-")
        resource_count = 1 + profile.schema_table_count * (1 + profile.schema_fields_per_table)
        return _PreparedSchemaPlan(
            spec=spec,
            client=client,
            state_path=work_root / "state.json",
            work_root=work_root,
            shape={
                "table_count": profile.schema_table_count,
                "fields_per_table": profile.schema_fields_per_table,
                "resource_count": resource_count,
                "metadata_request_count": 2 + profile.schema_table_count,
                "record_read_count": 0,
                "read_only": True,
            },
            unit_count=resource_count,
        )


_SCENARIO_VALUES: tuple[BenchmarkScenario, ...] = (
    ConcurrentAttachmentTransferScenario(),
    BackupReadScenario(),
    SequentialCsvExportScenario(),
    DataFrameMaterializationScenario(
        metadata_cache="cold",
        name="dataframe-materialization-cold-metadata",
    ),
    DataFrameMaterializationScenario(
        metadata_cache="warm",
        name="dataframe-materialization-warm-metadata",
    ),
    KeysetPaginationScenario(),
    ReadOnlySchemaPlanningScenario(),
    UpsertBatchPlanningScenario(),
)

SCENARIOS: Mapping[str, BenchmarkScenario] = MappingProxyType(
    {scenario.name: scenario for scenario in sorted(_SCENARIO_VALUES, key=lambda item: item.name)}
)
