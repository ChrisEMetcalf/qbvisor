import json
import os
from typing import Any, cast

from .exceptions import QuickbaseResponseError
from .log_runner import get_logger
from .transport import JSONValue, QuickBaseTransport

logger = get_logger(__name__)


def _expect_object(payload: JSONValue, path: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise QuickbaseResponseError(
            "GET", path, expected="JSON object", actual=type(payload).__name__
        )
    return cast(dict[str, Any], payload)


def _expect_object_array(payload: JSONValue, path: str) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise QuickbaseResponseError(
            "GET", path, expected="JSON array of objects", actual=type(payload).__name__
        )
    return cast(list[dict[str, Any]], payload)


class QuickBaseInputError(Exception):
    """
    Raised when the user provides an invalid app, table, or field name.
    """

    pass


class QuickBaseMetaCache:
    """
    Caches and provides access to Quickbase app, table, and field metadata.
    """

    def __init__(self, transport: QuickBaseTransport):
        # Load app IDs mapping from environment variable
        raw = os.getenv("QB_APP_IDS")
        if not raw:
            raise OSError("Environment variable 'QB_APP_IDS' is required.")
        parsed = json.loads(raw)
        self.app_ids = parsed  # friendly_name -> app_id
        self.name_map = {name.lower(): name for name in parsed.keys()}
        self.transport = transport
        self.cache: dict[str, dict[str, Any]] = {}
        self._table_catalogs: dict[str, list[dict[str, Any]]] = {}
        self._tables_by_id: dict[str, dict[str, dict[str, Any]]] = {}
        self._tables_by_name: dict[str, dict[str, dict[str, Any]]] = {}
        self._loaded_field_maps: set[tuple[str, str]] = set()
        self._field_labels: dict[tuple[str, str], dict[str, str]] = {}

    def normalize_app(self, app: str) -> str:
        # Accept either friendly name or app ID
        if app in self.app_ids.values():
            for name, aid in self.app_ids.items():
                if aid == app:
                    return name
        key = app.lower()
        if key not in self.name_map:
            raise QuickBaseInputError(
                f"App '{app}' not found. Available: {list(self.app_ids.keys())}"
            )
        return self.name_map[key]

    def get_app_id(self, app: str) -> str:
        name = self.normalize_app(app)
        return self.app_ids[name]

    def get_tables(self, app: str) -> list[dict[str, Any]]:
        """
        List tables: GET /v1/tables?appId={appId}
        """
        name = self.normalize_app(app)
        app_id = self.app_ids[name]
        payload = self.transport.get("tables", params={"appId": app_id})
        tables = _expect_object_array(payload, "tables")
        self._table_catalogs[name] = tables
        self._tables_by_id[name] = {
            table["id"]: table for table in tables if isinstance(table.get("id"), str)
        }
        self._tables_by_name[name] = {
            table["name"].lower(): table for table in tables if isinstance(table.get("name"), str)
        }
        return tables

    def get_table(self, app: str, table: str) -> dict[str, Any]:
        """
        Get table metadata: GET /v1/tables/{tableId}?appId={appId}
        Caches id and size.
        """
        name = self.normalize_app(app)
        if name not in self.cache:
            self.cache[name] = {"tables": {}}

        # Fetch the app's table catalog once, then resolve later requests locally.
        if name not in self._table_catalogs:
            self.get_tables(name)
        match = self._tables_by_id[name].get(table)
        if match is None:
            match = self._tables_by_name[name].get(table.lower())
        if not match:
            available = [
                item["name"]
                for item in self._table_catalogs[name]
                if isinstance(item.get("name"), str)
            ]
            raise QuickBaseInputError(
                f"Table '{table}' not found in app '{app}'. Available: {available}"
            )
        tbl_name = match["name"]
        tbl_id = match["id"]

        # Cache if missing
        if tbl_name not in self.cache[name]["tables"]:
            payload = self.transport.get(f"tables/{tbl_id}", params={"appId": self.app_ids[name]})
            resp = _expect_object(payload, f"tables/{tbl_id}")
            size = resp.get("nextRecordId", 1) - 1
            self.cache[name]["tables"][tbl_name] = {"id": tbl_id, "size": size, "fields": {}}
        return self.cache[name]["tables"][tbl_name]

    def get_table_id(self, app: str, table: str) -> str:
        table_info = self.get_table(app, table)
        return table_info["id"]

    def get_fields(self, app: str, table: str) -> dict[str, dict[str, Any]]:
        """
        List fields: GET /v1/fields?tableId={tableId}&includeFieldPerms=true
        Caches labels, IDs, and types.
        """
        name = self.normalize_app(app)
        # Ensure table is cached
        table_info = self.get_table(name, table)
        tbl_id = table_info["id"]

        payload = self.transport.get(
            "fields", params={"tableId": tbl_id, "includeFieldPerms": "true"}
        )
        fields = _expect_object_array(payload, "fields")

        fmap = {f["label"]: {"id": f["id"], "type": f.get("fieldType")} for f in fields}

        # ** Mutate ** the cached table-info dict in-place
        table_info["fields"] = fmap
        field_key = (name, tbl_id)
        self._loaded_field_maps.add(field_key)
        self._field_labels[field_key] = {label.lower(): label for label in fmap}
        return fmap

    def get_field_map(self, app: str, table: str) -> dict[str, dict[str, Any]]:
        # Ensure and return field mapping
        name = self.normalize_app(app)
        table_info = self.get_table(app, table)
        field_key = (name, table_info["id"])
        # If we haven't cached the fields yet, do so now
        if field_key not in self._loaded_field_maps:
            self.get_fields(app, table)
        return table_info["fields"]

    def get_field_id(self, app: str, table: str, field_label: str) -> int:
        name = self.normalize_app(app)
        table_info = self.get_table(app, table)
        field_key = (name, table_info["id"])
        if field_key not in self._loaded_field_maps:
            self.get_fields(app, table)
        fmap = table_info["fields"]
        lookup = self._field_labels[field_key]
        if field_label.lower() not in lookup:
            raise QuickBaseInputError(
                f"Field '{field_label}' not found. Options: {list(fmap.keys())}"
            )
        key = lookup[field_label.lower()]
        return fmap[key]["id"]

    def invalidate_fields(self, app: str, table: str) -> None:
        """Discard cached fields for one table after a schema mutation."""
        name = self.normalize_app(app)
        tables = self.cache.get(name, {}).get("tables", {})
        for table_name, table_info in tables.items():
            if table == table_info.get("id") or table.lower() == table_name.lower():
                table_info["fields"] = {}
                field_key = (name, table_info["id"])
                self._loaded_field_maps.discard(field_key)
                self._field_labels.pop(field_key, None)
                return

    def invalidate_app_fields(self, app: str) -> None:
        """Discard all cached field metadata after an app-wide schema mutation."""
        name = self.normalize_app(app)
        for table_info in self.cache.get(name, {}).get("tables", {}).values():
            table_info["fields"] = {}
        self._loaded_field_maps = {
            field_key for field_key in self._loaded_field_maps if field_key[0] != name
        }
        self._field_labels = {
            field_key: labels
            for field_key, labels in self._field_labels.items()
            if field_key[0] != name
        }

    def invalidate_tables(self, app: str) -> None:
        """Discard table and field metadata after a table collection mutation."""
        name = self.normalize_app(app)
        self.cache.pop(name, None)
        self._table_catalogs.pop(name, None)
        self._tables_by_id.pop(name, None)
        self._tables_by_name.pop(name, None)
        self._loaded_field_maps = {
            field_key for field_key in self._loaded_field_maps if field_key[0] != name
        }
        self._field_labels = {
            field_key: labels
            for field_key, labels in self._field_labels.items()
            if field_key[0] != name
        }

    def get_relationships(self, app: str, table: str) -> list[dict[str, Any]]:
        """
        List relationships: GET /v1/tables/{tableId}/relationships
        """
        tbl_id = self.get_table_id(app, table)
        path = f"tables/{tbl_id}/relationships"
        resp = _expect_object(self.transport.get(path), path)
        relationships = resp.get("relationships", [])
        if not isinstance(relationships, list) or not all(
            isinstance(item, dict) for item in relationships
        ):
            raise QuickbaseResponseError(
                "GET",
                path,
                expected="relationships array of objects",
                actual=type(relationships).__name__,
            )
        return cast(list[dict[str, Any]], relationships)
