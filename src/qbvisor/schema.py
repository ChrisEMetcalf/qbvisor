"""Declarative desired-state and persistent-state contracts for Quickbase schemas."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from math import isfinite
from types import MappingProxyType
from typing import Any, Literal, cast
from uuid import UUID, uuid4

from .models import RelationshipAccumulation

SCHEMA_STATE_FORMAT = "qbvisor-schema-state"
SCHEMA_STATE_FORMAT_VERSION = 1

SchemaResourceKind = Literal["app", "table", "field", "relationship"]

_RESOURCE_KEY = re.compile(r"^[a-z][a-z0-9_]*$")
_ACCUMULATION_TYPES = frozenset(
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
_ADDRESS_PATTERNS: dict[SchemaResourceKind, re.Pattern[str]] = {
    "app": re.compile(r"^apps\.[a-z][a-z0-9_]*$"),
    "table": re.compile(r"^apps\.[a-z][a-z0-9_]*\.tables\.[a-z][a-z0-9_]*$"),
    "field": re.compile(
        r"^apps\.[a-z][a-z0-9_]*\.tables\.[a-z][a-z0-9_]*\.fields\.[a-z][a-z0-9_]*$"
    ),
    "relationship": re.compile(r"^apps\.[a-z][a-z0-9_]*\.relationships\.[a-z][a-z0-9_]*$"),
}


def _validate_key(value: str, field_name: str = "key") -> None:
    if not isinstance(value, str) or _RESOURCE_KEY.fullmatch(value) is None:
        raise ValueError(
            f"{field_name} must start with a lowercase letter and contain only "
            "lowercase letters, numbers, and underscores"
        )


def _validate_name(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _required_string(payload: dict[str, Any], key: str, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{context} {key} must be a non-empty string")
    return value


def _required_integer(payload: dict[str, Any], key: str, context: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{context} {key} must be an integer")
    return value


def _typed_tuple[T](value: Sequence[T], item_type: type[T], field_name: str) -> tuple[T, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be a sequence of {item_type.__name__} values")
    items = tuple(value)
    if not all(isinstance(item, item_type) for item in items):
        raise ValueError(f"{field_name} must contain only {item_type.__name__} values")
    return items


def _string_tuple(value: Sequence[str], field_name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise ValueError(f"{field_name} must be a sequence of resource keys")
    items = tuple(value)
    for item in items:
        _validate_key(item, field_name)
    if len(set(items)) != len(items):
        raise ValueError(f"{field_name} cannot contain duplicate resource keys")
    return items


def _optional_mapping(
    value: Mapping[str, Any] | None,
    field_name: str,
) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field_name} must be a mapping with string keys")
    return cast(Mapping[str, Any], _freeze_json(dict(value), field_name))


def _freeze_json(value: Any, field_name: str) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not isfinite(value):
            raise ValueError(f"{field_name} cannot contain non-finite numbers")
        return value
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise ValueError(f"{field_name} object keys must be strings")
        return MappingProxyType(
            {key: _freeze_json(item, field_name) for key, item in value.items()}
        )
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(_freeze_json(item, field_name) for item in value)
    raise ValueError(f"{field_name} must contain only JSON-compatible values")


def _unique_specs(specs: Sequence[Any], resource_name: str, name_attribute: str) -> None:
    keys = [spec.key for spec in specs]
    if len(set(keys)) != len(keys):
        raise ValueError(f"{resource_name} resource keys must be unique")
    names = [getattr(spec, name_attribute).casefold() for spec in specs]
    if len(set(names)) != len(names):
        raise ValueError(f"{resource_name} names must be unique ignoring case")


@dataclass(frozen=True, slots=True, kw_only=True)
class FieldSpec:
    """Desired state for one Quickbase field."""

    key: str
    label: str
    field_type: str
    help_text: str | None = None
    required: bool | None = None
    unique: bool | None = None
    appears_by_default: bool | None = None
    find_enabled: bool | None = None
    audited: bool | None = None
    add_to_forms: bool | None = None
    bold: bool | None = None
    properties: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        _validate_key(self.key, "field key")
        _validate_name(self.label, "field label")
        _validate_name(self.field_type, "field_type")
        if self.help_text is not None and not isinstance(self.help_text, str):
            raise ValueError("help_text must be a string or None")
        for field_name in (
            "required",
            "unique",
            "appears_by_default",
            "find_enabled",
            "audited",
            "add_to_forms",
            "bold",
        ):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, bool):
                raise ValueError(f"{field_name} must be a boolean or None")
        object.__setattr__(self, "properties", _optional_mapping(self.properties, "properties"))

    def address(self, app_key: str, table_key: str) -> str:
        _validate_key(app_key, "app key")
        _validate_key(table_key, "table key")
        return f"apps.{app_key}.tables.{table_key}.fields.{self.key}"


@dataclass(frozen=True, slots=True, kw_only=True)
class TableSpec:
    """Desired state for one Quickbase table and its directly managed fields."""

    key: str
    name: str
    description: str | None = None
    singular_record_name: str | None = None
    plural_record_name: str | None = None
    fields: Sequence[FieldSpec] = ()

    def __post_init__(self) -> None:
        _validate_key(self.key, "table key")
        _validate_name(self.name, "table name")
        for field_name in ("description", "singular_record_name", "plural_record_name"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise ValueError(f"{field_name} must be a string or None")
        fields = _typed_tuple(self.fields, FieldSpec, "fields")
        _unique_specs(fields, "field", "label")
        object.__setattr__(self, "fields", fields)

    def address(self, app_key: str) -> str:
        _validate_key(app_key, "app key")
        return f"apps.{app_key}.tables.{self.key}"


@dataclass(frozen=True, slots=True, kw_only=True)
class SummaryFieldSpec:
    """Desired summary field attached to a declarative relationship."""

    accumulation_type: RelationshipAccumulation
    field: str | None = None
    label: str | None = None
    where: str | None = None

    def __post_init__(self) -> None:
        if self.accumulation_type not in _ACCUMULATION_TYPES:
            raise ValueError(f"Unsupported accumulation type: {self.accumulation_type}")
        if self.accumulation_type == "COUNT":
            if self.field is not None:
                raise ValueError("COUNT summary fields must omit field")
        elif self.field is None:
            raise ValueError(f"{self.accumulation_type} summary fields require a child field key")
        if self.field is not None:
            _validate_key(self.field, "summary field key")
        if self.label is not None:
            _validate_name(self.label, "summary field label")
        if self.where is not None and not isinstance(self.where, str):
            raise ValueError("summary field where must be a string or None")


@dataclass(frozen=True, slots=True, kw_only=True)
class RelationshipSpec:
    """Desired relationship between two tables referenced by stable keys."""

    key: str
    parent_table: str
    child_table: str
    foreign_key_label: str | None = None
    lookup_fields: Sequence[str] = ()
    summary_fields: Sequence[SummaryFieldSpec] = ()

    def __post_init__(self) -> None:
        _validate_key(self.key, "relationship key")
        _validate_key(self.parent_table, "parent table key")
        _validate_key(self.child_table, "child table key")
        if self.foreign_key_label is not None:
            _validate_name(self.foreign_key_label, "foreign key label")
        object.__setattr__(
            self,
            "lookup_fields",
            _string_tuple(self.lookup_fields, "lookup_fields"),
        )
        object.__setattr__(
            self,
            "summary_fields",
            _typed_tuple(self.summary_fields, SummaryFieldSpec, "summary_fields"),
        )

    def address(self, app_key: str) -> str:
        _validate_key(app_key, "app key")
        return f"apps.{app_key}.relationships.{self.key}"


@dataclass(frozen=True, slots=True, kw_only=True)
class AppSpec:
    """Desired application schema keyed independently from Quickbase display names."""

    key: str
    name: str
    description: str | None = None
    variables: Mapping[str, str] | None = None
    security_properties: Mapping[str, bool] | None = None
    tables: Sequence[TableSpec] = ()
    relationships: Sequence[RelationshipSpec] = ()

    def __post_init__(self) -> None:
        _validate_key(self.key, "app key")
        _validate_name(self.name, "app name")
        if self.description is not None and not isinstance(self.description, str):
            raise ValueError("description must be a string or None")
        variables = _optional_mapping(self.variables, "variables")
        if variables is not None and not all(
            isinstance(value, str) for value in variables.values()
        ):
            raise ValueError("variables values must be strings")
        security = _optional_mapping(self.security_properties, "security_properties")
        if security is not None and not all(isinstance(value, bool) for value in security.values()):
            raise ValueError("security_properties values must be booleans")
        tables = _typed_tuple(self.tables, TableSpec, "tables")
        relationships = _typed_tuple(self.relationships, RelationshipSpec, "relationships")
        _unique_specs(tables, "table", "name")
        relationship_keys = [relationship.key for relationship in relationships]
        if len(set(relationship_keys)) != len(relationship_keys):
            raise ValueError("relationship resource keys must be unique")
        table_by_key = {table.key: table for table in tables}
        relationship_labels_by_child: dict[str, set[str]] = {}
        for relationship in relationships:
            if relationship.parent_table not in table_by_key:
                raise ValueError(
                    f"relationship {relationship.key} references unknown parent table "
                    f"{relationship.parent_table}"
                )
            if relationship.child_table not in table_by_key:
                raise ValueError(
                    f"relationship {relationship.key} references unknown child table "
                    f"{relationship.child_table}"
                )
            parent_field_by_key = {
                field.key: field for field in table_by_key[relationship.parent_table].fields
            }
            missing_lookups = set(relationship.lookup_fields) - parent_field_by_key.keys()
            if missing_lookups:
                raise ValueError(
                    f"relationship {relationship.key} references unknown parent lookup fields: "
                    f"{sorted(missing_lookups)}"
                )
            child_fields = {field.key for field in table_by_key[relationship.child_table].fields}
            missing_summaries = {
                summary.field
                for summary in relationship.summary_fields
                if summary.field is not None and summary.field not in child_fields
            }
            if missing_summaries:
                raise ValueError(
                    f"relationship {relationship.key} references unknown child summary fields: "
                    f"{sorted(missing_summaries)}"
                )
            child_labels = {
                field.label.casefold() for field in table_by_key[relationship.child_table].fields
            }
            generated_labels: list[str] = []
            if relationship.foreign_key_label is not None:
                generated_labels.append(relationship.foreign_key_label)
            generated_labels.extend(
                summary.label
                for summary in relationship.summary_fields
                if summary.label is not None
            )
            folded_generated = [label.casefold() for label in generated_labels]
            if len(set(folded_generated)) != len(folded_generated):
                raise ValueError(
                    f"relationship {relationship.key} generates duplicate child field labels"
                )
            occupied_labels = child_labels | relationship_labels_by_child.setdefault(
                relationship.child_table, set()
            )
            collisions = set(folded_generated) & occupied_labels
            if collisions:
                raise ValueError(
                    f"relationship {relationship.key} generated field labels collide with "
                    f"other managed child fields: {sorted(collisions)}"
                )
            relationship_labels_by_child[relationship.child_table].update(folded_generated)
        object.__setattr__(self, "variables", variables)
        object.__setattr__(self, "security_properties", security)
        object.__setattr__(self, "tables", tables)
        object.__setattr__(self, "relationships", relationships)

    @property
    def address(self) -> str:
        return f"apps.{self.key}"


@dataclass(frozen=True, slots=True, kw_only=True)
class StateResource:
    """One stable resource address bound to a Quickbase identifier."""

    address: str
    kind: SchemaResourceKind
    remote_id: str | int
    name: str

    def __post_init__(self) -> None:
        if not isinstance(self.kind, str) or self.kind not in _ADDRESS_PATTERNS:
            raise ValueError(f"Unsupported schema resource kind: {self.kind}")
        if (
            not isinstance(self.address, str)
            or _ADDRESS_PATTERNS[self.kind].fullmatch(self.address) is None
        ):
            raise ValueError(f"Resource address does not match kind {self.kind}: {self.address}")
        _validate_name(self.name, "state resource name")
        if self.kind in {"app", "table"}:
            if not isinstance(self.remote_id, str) or not self.remote_id:
                raise ValueError(f"{self.kind} remote_id must be a non-empty string")
        elif (
            not isinstance(self.remote_id, int)
            or isinstance(self.remote_id, bool)
            or self.remote_id < 1
        ):
            raise ValueError(f"{self.kind} remote_id must be a positive integer")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "remote_id": self.remote_id,
            "name": self.name,
        }

    @classmethod
    def from_dict(cls, address: str, payload: dict[str, Any]) -> StateResource:
        if not isinstance(payload, dict):
            raise ValueError(f"State resource {address} must be an object")
        kind = payload.get("kind")
        if not isinstance(kind, str):
            raise ValueError(f"State resource {address} kind must be a string")
        remote_id = payload.get("remote_id")
        if not isinstance(remote_id, (str, int)) or isinstance(remote_id, bool):
            raise ValueError(f"State resource {address} remote_id must be a string or integer")
        return cls(
            address=address,
            kind=cast(SchemaResourceKind, kind),
            remote_id=remote_id,
            name=_required_string(payload, "name", f"State resource {address}"),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class SchemaState:
    """Versioned local bindings between declarative addresses and Quickbase IDs."""

    lineage: str = field(default_factory=lambda: str(uuid4()))
    serial: int = 0
    resources: Sequence[StateResource] = ()
    format: str = SCHEMA_STATE_FORMAT
    format_version: int = SCHEMA_STATE_FORMAT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.format, str):
            raise ValueError("schema state format must be a string")
        if self.format != SCHEMA_STATE_FORMAT:
            raise ValueError(f"Unsupported schema state format: {self.format}")
        if not isinstance(self.format_version, int) or isinstance(self.format_version, bool):
            raise ValueError("schema state format version must be an integer")
        if self.format_version != SCHEMA_STATE_FORMAT_VERSION:
            raise ValueError(f"Unsupported schema state version: {self.format_version}")
        try:
            UUID(self.lineage)
        except (TypeError, ValueError, AttributeError) as error:
            raise ValueError("schema state lineage must be a UUID") from error
        if not isinstance(self.serial, int) or isinstance(self.serial, bool) or self.serial < 0:
            raise ValueError("schema state serial must be a non-negative integer")
        resources = _typed_tuple(self.resources, StateResource, "resources")
        addresses = [resource.address for resource in resources]
        if len(set(addresses)) != len(addresses):
            raise ValueError("schema state cannot contain duplicate resource addresses")
        object.__setattr__(
            self, "resources", tuple(sorted(resources, key=lambda item: item.address))
        )

    def resource(self, address: str) -> StateResource | None:
        return next((resource for resource in self.resources if resource.address == address), None)

    def to_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "format_version": self.format_version,
            "lineage": self.lineage,
            "serial": self.serial,
            "resources": {resource.address: resource.to_dict() for resource in self.resources},
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> SchemaState:
        if not isinstance(payload, dict):
            raise ValueError("schema state must be an object")
        resources = payload.get("resources")
        if not isinstance(resources, dict) or not all(
            isinstance(address, str) for address in resources
        ):
            raise ValueError("schema state resources must be an address-keyed object")
        return cls(
            format=_required_string(payload, "format", "Schema state"),
            format_version=_required_integer(payload, "format_version", "Schema state"),
            lineage=_required_string(payload, "lineage", "Schema state"),
            serial=_required_integer(payload, "serial", "Schema state"),
            resources=tuple(
                StateResource.from_dict(address, resource)
                for address, resource in resources.items()
            ),
        )
