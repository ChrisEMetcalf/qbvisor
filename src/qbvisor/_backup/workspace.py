"""Safe, deterministic artifact writes inside a backup staging directory."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO
from uuid import uuid4

from ..backup import BackupArtifact, BackupArtifactKind


class JsonLinesArtifactWriter:
    """Incrementally build one atomic JSON Lines artifact."""

    def __init__(
        self,
        workspace: BackupWorkspace,
        relative_path: str,
        kind: BackupArtifactKind,
    ):
        self._workspace = workspace
        self.relative_path = relative_path
        self.kind = kind
        self._destination = workspace._available_destination(relative_path)
        self._temporary = self._destination.with_name(
            f".{self._destination.name}.{uuid4().hex}.tmp"
        )
        self._stream: BinaryIO | None = None
        self._digest = hashlib.sha256()
        self._byte_count = 0
        self._item_count = 0
        self._artifact: BackupArtifact | None = None

    def __enter__(self) -> JsonLinesArtifactWriter:
        self._destination.parent.mkdir(parents=True, exist_ok=True)
        self._stream = self._temporary.open("xb")
        return self

    def __exit__(self, error_type: object, error: object, traceback: object) -> None:
        if self._stream is not None:
            self._stream.close()
        if error_type is not None:
            self._temporary.unlink(missing_ok=True)
            return
        try:
            os.replace(self._temporary, self._destination)
        finally:
            self._temporary.unlink(missing_ok=True)
        self._artifact = BackupArtifact(
            path=self.relative_path,
            kind=self.kind,
            sha256=self._digest.hexdigest(),
            bytes=self._byte_count,
            item_count=self._item_count,
        )
        self._workspace._register(self._artifact)

    @property
    def artifact(self) -> BackupArtifact:
        if self._artifact is None:
            raise RuntimeError("JSON Lines artifact is not complete")
        return self._artifact

    def write(self, item: Any) -> None:
        if self._stream is None:
            raise RuntimeError("JSON Lines writer is not open")
        line = (
            json.dumps(
                item,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
        self._stream.write(line)
        self._digest.update(line)
        self._byte_count += len(line)
        self._item_count += 1


class BackupWorkspace:
    """Write and inventory files under one explicitly scoped staging directory."""

    def __init__(self, root: Path):
        self.root = root
        self._artifacts: list[BackupArtifact] = []

    @property
    def artifacts(self) -> tuple[BackupArtifact, ...]:
        return tuple(self._artifacts)

    def write_json(
        self,
        relative_path: str,
        kind: BackupArtifactKind,
        payload: Any,
        *,
        item_count: int | None = None,
    ) -> BackupArtifact:
        """Atomically write deterministic JSON and return its integrity metadata."""
        destination = self._available_destination(relative_path)
        content = (
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False)
            + "\n"
        ).encode("utf-8")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
        try:
            temporary.write_bytes(content)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

        artifact = BackupArtifact(
            path=relative_path,
            kind=kind,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
            item_count=item_count,
        )
        self._register(artifact)
        return artifact

    def write_json_lines(
        self,
        relative_path: str,
        kind: BackupArtifactKind,
        items: Iterable[Any],
    ) -> BackupArtifact:
        """Atomically stream deterministic JSON Lines without retaining all items."""
        with self.json_lines_writer(relative_path, kind) as writer:
            for item in items:
                writer.write(item)
        return writer.artifact

    def json_lines_writer(
        self,
        relative_path: str,
        kind: BackupArtifactKind,
    ) -> JsonLinesArtifactWriter:
        return JsonLinesArtifactWriter(self, relative_path, kind)

    def write_bytes(
        self,
        relative_path: str,
        kind: BackupArtifactKind,
        content: bytes,
    ) -> BackupArtifact:
        """Atomically write a binary artifact and inventory its exact bytes."""
        if not isinstance(content, bytes):
            raise ValueError("binary artifact content must be bytes")
        destination = self._available_destination(relative_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        temporary = destination.with_name(f".{destination.name}.{uuid4().hex}.tmp")
        try:
            temporary.write_bytes(content)
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)
        artifact = BackupArtifact(
            path=relative_path,
            kind=kind,
            sha256=hashlib.sha256(content).hexdigest(),
            bytes=len(content),
        )
        self._register(artifact)
        return artifact

    def _available_destination(self, relative_path: str) -> Path:
        destination = self._destination(relative_path)
        if any(existing.path == relative_path for existing in self._artifacts):
            raise ValueError(f"backup artifact already exists: {relative_path}")
        return destination

    def _destination(self, relative_path: str) -> Path:
        if not isinstance(relative_path, str) or not relative_path or "\\" in relative_path:
            raise ValueError("artifact path must be a non-empty POSIX relative path")
        path = PurePosixPath(relative_path)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != relative_path:
            raise ValueError("artifact path must stay within the backup workspace")
        return self.root.joinpath(*path.parts)

    def _register(self, artifact: BackupArtifact) -> None:
        self._artifacts.append(artifact)
