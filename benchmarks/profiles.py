"""Deterministic data-volume profiles shared by local benchmark scenarios."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any, Final


@dataclass(frozen=True, slots=True)
class BenchmarkProfile:
    """One repeatable workload shape, independent of machine performance."""

    name: str
    record_count: int
    page_size: int
    field_count: int
    attachment_count: int
    attachment_bytes: int
    schema_table_count: int
    schema_fields_per_table: int
    upsert_record_count: int
    upsert_value_bytes: int

    def __post_init__(self) -> None:
        if not self.name or self.name.strip() != self.name:
            raise ValueError(
                "Benchmark profile name must be non-empty and have no outer whitespace"
            )
        for field_name, value in asdict(self).items():
            if field_name != "name" and (
                not isinstance(value, int) or isinstance(value, bool) or value <= 0
            ):
                raise ValueError(f"Benchmark profile {field_name} must be a positive integer")
        if self.page_size > 1_000:
            raise ValueError("Benchmark profile page_size cannot exceed Quickbase's 1,000 limit")
        if self.page_size > self.record_count:
            raise ValueError("Benchmark profile page_size cannot exceed record_count")

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible description in declaration order."""

        return asdict(self)


_PROFILE_VALUES: Final = {
    "small": BenchmarkProfile(
        name="small",
        record_count=100,
        page_size=25,
        field_count=6,
        attachment_count=8,
        attachment_bytes=4 * 1024,
        schema_table_count=2,
        schema_fields_per_table=8,
        upsert_record_count=100,
        upsert_value_bytes=128,
    ),
    "medium": BenchmarkProfile(
        name="medium",
        record_count=1_000,
        page_size=100,
        field_count=12,
        attachment_count=32,
        attachment_bytes=16 * 1024,
        schema_table_count=6,
        schema_fields_per_table=20,
        upsert_record_count=1_000,
        upsert_value_bytes=1_024,
    ),
    "large": BenchmarkProfile(
        name="large",
        record_count=10_000,
        page_size=250,
        field_count=24,
        attachment_count=128,
        attachment_bytes=64 * 1024,
        schema_table_count=12,
        schema_fields_per_table=40,
        # This deterministic value volume crosses Quickbase's 40 MB request boundary.
        upsert_record_count=48,
        upsert_value_bytes=1024 * 1024,
    ),
}

PROFILES: Final[Mapping[str, BenchmarkProfile]] = MappingProxyType(_PROFILE_VALUES)


def get_profile(name: str) -> BenchmarkProfile:
    """Resolve a profile name case-insensitively and explain valid choices."""

    normalized = name.strip().casefold()
    try:
        return PROFILES[normalized]
    except KeyError as exc:
        choices = ", ".join(PROFILES)
        raise ValueError(f"Unknown benchmark profile {name!r}; choose one of: {choices}") from exc
