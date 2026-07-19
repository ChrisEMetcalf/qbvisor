import inspect

import qbvisor
from qbvisor import QuickBaseClient

EXPECTED_RESOURCE_SIGNATURES = {
    "create_app": (
        "(self, name: str, description: str | None = None, assign_token: bool = False, "
        "variables: list[dict[str, str]] | None = None, "
        "security_properties: dict[str, bool] | None = None) -> dict[str, Any]"
    ),
    "get_app": "(self, app_name: str) -> dict[str, Any]",
    "get_app_events": "(self, app_name: str) -> list[dict[str, Any]]",
    "get_app_roles": "(self, app_name: str) -> list[dict[str, Any]]",
    "update_app": (
        "(self, app_name: str, new_name: str | None = None, description: str | None = None, "
        "variables: list[dict[str, str]] | None = None, "
        "security_properties: dict[str, bool] | None = None) -> dict[str, Any]"
    ),
    "delete_app": "(self, app_name: str) -> dict[str, Any]",
    "copy_app": (
        "(self, app_name: str, new_app_name: str, description: str | None = None, "
        "properties: dict[str, Any] | None = None) -> dict[str, Any]"
    ),
    "create_table": (
        "(self, app_name: str, table_name: str, description: str | None = None, "
        "singular_record_name: str | None = None, plural_record_name: str | None = None) "
        "-> dict[str, Any]"
    ),
    "get_tables_for_app": "(self, app_name: str) -> list[dict[str, Any]]",
    "get_table": "(self, app_name: str, table_name: str) -> dict[str, Any]",
    "update_table": (
        "(self, app_name: str, table_name: str, new_table_name: str | None = None, "
        "singular_record_name: str | None = None, plural_record_name: str | None = None) "
        "-> dict[str, Any]"
    ),
    "delete_table": "(self, app_name: str, table_name: str) -> dict[str, Any]",
    "get_all_relationships": ("(self, app_name: str, table_name: str) -> list[dict[str, Any]]"),
    "create_relationship": (
        "(self, app_name: str, table_name: str, parent_table_name: str, "
        "foreign_key_label: str | None = None, lookup_field_ids: list[int] | None = None, "
        "summary_fields: list[dict[str, Any]] | None = None) -> dict[str, Any]"
    ),
    "update_relationship": (
        "(self, app_name: str, table_name: str, relationship: str | int, *, "
        "lookup_fields: Sequence[str | int] | None = None, "
        "summary_fields: Sequence[RelationshipSummary] | None = None) -> dict[str, Any]"
    ),
    "delete_relationship": (
        "(self, app_name: str, table_name: str, related_field: str) -> Any | None"
    ),
    "create_field": (
        "(self, app_name: str, table_name: str, label: str, field_type: str) -> dict[str, Any]"
    ),
    "delete_fields": (
        "(self, app_name: str, table_name: str, field_labels: list[str]) -> dict[str, Any]"
    ),
    "get_fields_usage": (
        "(self, app_name: str, table_name: str, *, skip: int | None = None, "
        "top: int | None = None) -> list[dict[str, Any]]"
    ),
    "get_field_usage": (
        "(self, app_name: str, table_name: str, field: str | int) -> list[dict[str, Any]]"
    ),
    "get_field_id": "(self, app_id: str, table_id: str, field_label: str) -> int",
    "get_table_id": "(self, app_id: str, table_id: str) -> str",
    "get_field": "(self, app_id, table_id, field_id)",
}


def _normalized_signature(method_name: str) -> str:
    signature = str(inspect.signature(getattr(QuickBaseClient, method_name)))
    for module_prefix in ("typing.", "collections.abc.", "qbvisor.models."):
        signature = signature.replace(module_prefix, "")
    return signature


def test_extracted_resource_methods_preserve_public_signatures():
    actual = {name: _normalized_signature(name) for name in EXPECTED_RESOURCE_SIGNATURES}

    assert actual == EXPECTED_RESOURCE_SIGNATURES


def test_private_resources_do_not_expand_the_supported_package_api():
    resource_names = {"AppResource", "FieldResource", "RelationshipResource", "TableResource"}

    assert resource_names.isdisjoint(qbvisor.__all__)
    assert all(not hasattr(qbvisor, name) for name in resource_names)
