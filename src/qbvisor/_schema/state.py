"""Read persistent declarative schema state without changing local files."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from ..exceptions import QuickbaseSchemaStateError
from ..schema import SchemaState

DEFAULT_SCHEMA_STATE_PATH = Path(".qbvisor/state.json")


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
