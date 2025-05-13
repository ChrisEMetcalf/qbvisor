import os
import json
from typing import Dict, Any, List

from .log_runner import get_logger
from .transport import QuickBaseTransport

logger = get_logger(__name__)

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
        raw = os.getenv('QB_APP_IDS')
        if not raw:
            raise EnvironmentError("Environment variable 'QB_APP_IDS' is required.")
        parsed = json.loads(raw)
        self.app_ids   = parsed                            # friendly_name -> app_id
        self.name_map  = {name.lower(): name for name in parsed.keys()}
        self.transport = transport
        self.cache     = {}  # structure: { app_name: { 'tables': { table_name: {id,size,fields} } } }

    def normalize_app(self, app: str) -> str:
        # Accept either friendly name or app ID
        if app in self.app_ids.values():
            for name, aid in self.app_ids.items():
                if aid == app:
                    return name
        key = app.lower()
        if key not in self.name_map:
            raise QuickBaseInputError(f"App '{app}' not found. Available: {list(self.app_ids.keys())}")
        return self.name_map[key]

    def get_app_id(self, app: str) -> str:
        name = self.normalize_app(app)
        return self.app_ids[name]

    def get_tables(self, app: str) -> List[Dict[str, Any]]:
        """
        List tables: GET /v1/tables?appId={appId}
        """
        name   = self.normalize_app(app)
        app_id = self.app_ids[name]
        resp   = self.transport.get('tables', params={'appId': app_id})
        if isinstance(resp, dict):
            return resp.get('tables', [])
        if isinstance(resp, list):
            return resp
        raise QuickBaseInputError(f"Unexpected response type for tables: {type(resp)}")

    def get_table(self, app: str, table: str) -> Dict[str, Any]:
        """
        Get table metadata: GET /v1/tables/{tableId}?appId={appId}
        Caches id and size.
        """
        name = self.normalize_app(app)
        if name not in self.cache:
            self.cache[name] = {'tables': {}}

        # Find the table by friendly name (case-insensitive)
        tables = self.get_tables(name)
        if table in [t["id"] for t in tables]:
            match = next((t for t in tables if t["id"] == table), None)
        else:
            match = next((t for t in tables if t["name"].lower() == table.lower()), None)
        if not match:
            available = [t['name'] for t in tables]
            raise QuickBaseInputError(f"Table '{table}' not found in app '{app}'. Available: {available}")
        tbl_name = match['name']
        tbl_id   = match['id']

        # Cache if missing
        if tbl_name not in self.cache[name]['tables']:
            resp = self.transport.get(f'tables/{tbl_id}', params={'appId': self.app_ids[name]})
            size = resp.get('nextRecordId', 1) - 1
            self.cache[name]['tables'][tbl_name] = {
                'id': tbl_id,
                'size': size,
                'fields': {}
            }
        return self.cache[name]['tables'][tbl_name]

    def get_table_id(self, app: str, table: str) -> str:
        table_info = self.get_table(app, table)
        return table_info['id']

    def get_fields(self, app: str, table: str) -> Dict[str, Dict[str, Any]]:
        """
        List fields: GET /v1/fields?tableId={tableId}&includeFieldPerms=true
        Caches labels, IDs, and types.
        """
        name       = self.normalize_app(app)
        # Ensure table is cached
        table_info = self.get_table(name, table)
        tbl_id     = table_info['id']

        resp = self.transport.get(
            'fields',
            params={'tableId': tbl_id, 'includeFieldPerms': 'true'}
        )
        # Extract list of fields
        if isinstance(resp, dict):
            fields = resp.get('fields', [])
        elif isinstance(resp, list):
            fields = resp
        else:
            raise QuickBaseInputError(f"Unexpected fields response: {type(resp)}")

        fmap = { f['label']: {'id': f['id'], 'type': f.get('fieldType')} for f in fields }
        
        # ** Mutate ** the cached table-info dict in-place
        table_info['fields'] = fmap
        return fmap

    def get_field_map(self, app: str, table: str) -> Dict[str, Dict[str, Any]]:
        # Ensure and return field mapping
        table_info = self.get_table(app, table)
        # If we haven't cached the fields yet, do so now
        if not table_info.get('fields'):
            self.get_fields(app, table)
        return table_info['fields']

    def get_field_id(self, app: str, table: str, field_label: str) -> int:
        fmap = self.get_field_map(app, table)
        lookup = {lbl.lower(): lbl for lbl in fmap}
        if field_label.lower() not in lookup:
            raise QuickBaseInputError(f"Field '{field_label}' not found. Options: {list(fmap.keys())}")
        key = lookup[field_label.lower()]
        return fmap[key]['id']

    def get_relationships(self, app: str, table: str) -> List[Dict[str, Any]]:
        """
        List relationships: GET /v1/tables/{tableId}/relationships
        """
        tbl_id = self.get_table_id(app, table)
        resp   = self.transport.get(f'tables/{tbl_id}/relationships')
        if isinstance(resp, dict):
            return resp.get('relationships', [])
        if isinstance(resp, list):
            return resp
        raise QuickBaseInputError(f"Unexpected relationships response: {type(resp)}")

