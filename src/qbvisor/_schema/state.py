"""Read persistent declarative schema state without changing local files."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, cast

from ..exceptions import QuickbaseSchemaLockError, QuickbaseSchemaStateError
from ..schema import SchemaState

DEFAULT_SCHEMA_STATE_PATH = Path(".qbvisor/state.json")

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows only
    fcntl = None  # type: ignore[assignment]

try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX only
    msvcrt = None  # type: ignore[assignment]


def load_schema_state(path: str | Path) -> SchemaState | None:
    """Load a state file, returning ``None`` when it has not been created yet."""
    state_path = Path(path)
    if not state_path.exists():
        return None
    if not state_path.is_file():
        raise QuickbaseSchemaStateError(state_path, "path is not a regular file")
    try:
        with state_path.open(encoding="utf-8") as source:
            payload = json.load(source)
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise QuickbaseSchemaStateError(state_path, str(error)) from error
    if not isinstance(payload, dict):
        raise QuickbaseSchemaStateError(state_path, "top-level JSON value must be an object")
    try:
        return SchemaState.from_dict(cast(dict[str, Any], payload))
    except ValueError as error:
        raise QuickbaseSchemaStateError(state_path, str(error)) from error


class SchemaStateLock:
    """Non-blocking cross-process advisory lock for one schema state path."""

    def __init__(self, state_path: str | Path):
        self.state_path = Path(state_path)
        self.lock_path = self.state_path.with_name(f"{self.state_path.name}.lock")
        self._handle: Any | None = None

    def __enter__(self) -> SchemaStateLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+b")
        try:
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            elif msvcrt is not None:  # pragma: no cover - Windows only
                if self.lock_path.stat().st_size == 0:
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:  # pragma: no cover - unsupported Python platform
                raise OSError("platform does not provide advisory file locking")
        except OSError as error:
            handle.close()
            raise QuickbaseSchemaLockError(
                f"Could not acquire schema state lock: {self.state_path}"
            ) from error
        self._handle = handle
        return self

    def __exit__(self, *_: object) -> None:
        if self._handle is None:
            return
        try:
            if fcntl is not None:
                fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            elif msvcrt is not None:  # pragma: no cover - Windows only
                self._handle.seek(0)
                msvcrt.locking(self._handle.fileno(), msvcrt.LK_UNLCK, 1)
        finally:
            self._handle.close()
            self._handle = None


def write_schema_state_candidate(path: str | Path, state: SchemaState) -> Path:
    """Durably write state beside its destination without publishing it."""
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{state_path.name}.",
        suffix=".tmp",
        dir=state_path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as target:
            json.dump(state.to_dict(), target, indent=2, sort_keys=True)
            target.write("\n")
            target.flush()
            os.fsync(target.fileno())
        return temporary_path
    except BaseException:
        temporary_path.unlink(missing_ok=True)
        raise


def publish_schema_state(candidate_path: str | Path, state_path: str | Path) -> None:
    """Atomically replace the published state with a verified candidate."""
    candidate = Path(candidate_path)
    destination = Path(state_path)
    os.replace(candidate, destination)
    try:
        directory = os.open(destination.parent, os.O_RDONLY)
    except OSError:  # pragma: no cover - platform-specific durability fallback
        return
    try:
        try:
            os.fsync(directory)
        except OSError:  # pragma: no cover - platform-specific durability fallback
            pass
    finally:
        os.close(directory)
