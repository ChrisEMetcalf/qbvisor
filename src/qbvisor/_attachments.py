"""Validation helpers for attachment metadata returned by record queries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .exceptions import QuickbaseResponseError


@dataclass(frozen=True, slots=True)
class LatestAttachment:
    """The latest downloadable version of a file attachment."""

    version_number: int
    file_name: str


def latest_attachment(
    value: Any,
    *,
    table_id: str,
    record_id: int,
    field_id: int,
) -> LatestAttachment | None:
    """Return validated latest-version metadata, or ``None`` for an empty file cell."""
    location = f"{table_id}/{record_id}/{field_id}"
    if value in (None, ""):
        return None
    if not isinstance(value, dict):
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected="file attachment value object",
            actual=f"{type(value).__name__} at {location}",
        )

    versions = value.get("versions")
    if versions is None:
        return None
    if not isinstance(versions, list) or not all(isinstance(version, dict) for version in versions):
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected="file attachment versions array",
            actual=f"{type(versions).__name__} at {location}",
        )
    if not versions:
        return None

    numbered: list[tuple[int, dict[str, Any]]] = []
    for version in versions:
        number = version.get("versionNumber")
        if not isinstance(number, int) or isinstance(number, bool) or number < 1:
            raise QuickbaseResponseError(
                "POST",
                "records/query",
                expected="positive integer attachment versionNumber",
                actual=f"{number!r} at {location}",
            )
        numbered.append((number, version))

    if len({number for number, _ in numbered}) != len(numbered):
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected="unique attachment versionNumber values",
            actual=f"duplicates at {location}",
        )

    version_number, latest = max(numbered, key=lambda item: item[0])
    file_name = latest.get("fileName") or value.get("fileName")
    if file_name is None or file_name == "":
        file_name = f"fid{field_id}_v{version_number}.bin"
    elif not isinstance(file_name, str):
        raise QuickbaseResponseError(
            "POST",
            "records/query",
            expected="string attachment fileName",
            actual=f"{type(file_name).__name__} at {location}",
        )

    return LatestAttachment(version_number=version_number, file_name=file_name)
