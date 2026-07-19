"""Typed request models for higher-level Quickbase operations."""

from dataclasses import dataclass
from typing import Literal

RelationshipAccumulation = Literal[
    "AVG",
    "SUM",
    "MAX",
    "MIN",
    "STD-DEV",
    "COUNT",
    "COMBINED-TEXT",
    "COMBINED-USER",
    "DISTINCT-COUNT",
]

_RELATIONSHIP_ACCUMULATIONS = frozenset(
    {
        "AVG",
        "SUM",
        "MAX",
        "MIN",
        "STD-DEV",
        "COUNT",
        "COMBINED-TEXT",
        "COMBINED-USER",
        "DISTINCT-COUNT",
    }
)


@dataclass(frozen=True, slots=True)
class RelationshipSummary:
    """Describe a summary field to add while updating a relationship."""

    accumulation_type: RelationshipAccumulation
    field: str | int | None = None
    label: str | None = None
    where: str | None = None

    def __post_init__(self) -> None:
        if self.accumulation_type not in _RELATIONSHIP_ACCUMULATIONS:
            raise ValueError(f"Unsupported accumulation type: {self.accumulation_type}")
        if self.accumulation_type == "COUNT":
            if self.field not in (None, 0):
                raise ValueError("COUNT summaries must omit field or use field ID 0")
        elif self.field is None:
            raise ValueError(f"{self.accumulation_type} summaries require a field")
