"""Safe, deterministic artifact writes inside a backup staging directory."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from ..backup import BackupArtifact, BackupArtifactKind


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
        destination = self._destination(relative_path)
        if any(existing.path == relative_path for existing in self._artifacts):
            raise ValueError(f"backup artifact already exists: {relative_path}")
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

    def _destination(self, relative_path: str) -> Path:
        if not isinstance(relative_path, str) or not relative_path or "\\" in relative_path:
            raise ValueError("artifact path must be a non-empty POSIX relative path")
        path = PurePosixPath(relative_path)
        if path.is_absolute() or ".." in path.parts or path.as_posix() != relative_path:
            raise ValueError("artifact path must stay within the backup workspace")
        return self.root.joinpath(*path.parts)

    def _register(self, artifact: BackupArtifact) -> None:
        self._artifacts.append(artifact)
