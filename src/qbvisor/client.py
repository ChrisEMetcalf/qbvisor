import asyncio
import json
import random
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import aiohttp
import aiofiles
import pandas as pd
from dotenv import load_dotenv

from .log_runner import get_logger
from .transport import QuickBaseTransport
from .metadata import QuickBaseMetaCache

logger = get_logger(__name__)

class QuickBaseClient:
    """
    High-level QuickBase client that composes transport, metadata, and file utilities.
    """
    def __init__(self):
        load_dotenv()
        self.transport = QuickBaseTransport()
        self.meta      = QuickBaseMetaCache(self.transport)
        self.logger    = get_logger(__name__)

    # ----------------
    # Private: Map friendly names to IDs
    # ----------------
    def _ids(
            self,
            app_name: str,
            table_name: str | None = None,
    ) -> Tuple[str, str | None]:
        """
        Map friendly names to IDs.
        """
        app_id = self.meta.get_app_id(app_name)
        if table_name is None:
            return app_id, None
        table_id = self.meta.get_table_id(app_id, table_name)
        return app_id, table_id

    def _request(
            self,
            method: str,
            path: str,
            params: Optional[Dict[str, Any]] = None,
            json_body: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Centralized request handling with logging.
        """
        url = f'{self.transport.base_url}/{path}'
        try:
            func = getattr(self.transport, method.lower())
            return func(path, params=params, json_body=json_body)
        except Exception as e:
            self.logger.error(f"Error in {method} request to {path}: {e}")
            raise

    # ----------------
    # App Methods
    # ----------------
    def create_app(
        self,
        name: str,
        description: Optional[str] = None,
        assign_token: bool = False,
        variables: Optional[List[Dict[str, str]]] = None,
        security_properties: Optional[Dict[str, bool]] = None
    ) -> Dict[str, Any]:
        """
        Create a new Quickbase application: POST /v1/apps
        
        Args:
            name (str): The name of the new application. (required)
            description (str, optional): A description for the new application.
            assign_token (bool, optional): Whether to assign a token to the new app.
            variables (list, optional): A list of application variables to set.
            security_properties (dict, optional): Security settings for the new app.
            

        Returns:
            dict: The created app's metadata.
        """
        body = {"name": name, "assignToken": assign_token}
        if description:
            body["description"] = description
        if variables:
            body["variables"] = variables
        if security_properties:
            body["securityProperties"] = security_properties

        return self._request(method='POST', path="apps", json_body=body)

    def get_app(self, app_name: str) -> Dict[str, Any]:
        """
        Get app metadata: GET /v1/apps/{appId}
        """
        app_id, _ = self._ids(app_name)

        return self._request(
            method='GET',
            path=f"apps/{app_id}",
            params={'appId': app_id}
        )

    def update_app(
            self,
            app_name: str,
            new_name: Optional[str] = None,
            description: Optional[str] = None,
            variables: Optional[List[Dict[str, str]]] = None,
            security_properties: Optional[Dict[str, bool]] = None
    ) -> Dict[str, Any]:
        """
        Update an existing Quickbase application: POST /v1/apps/{appId}

        Args:
            app_name (str): The name of the application to update. (required)
            new_name (str, optional): The new name for the application.
            description (str, optional): A new description for the application.
            variables (list, optional): A list of application variables to set.
            security_properties (dict, optional): Security settings for the application.

        Returns:
            dict: The updated app's metadata.
        """
        app_id, _ = self._ids(app_name)

        body: Dict[str, Any] = {}
        if new_name:
            body["name"] = new_name
        if description:
            body["description"] = description
        if variables:
            body["variables"] = variables
        if security_properties:
            body["securityProperties"] = security_properties

        if not body:
            raise ValueError("No update parameters provided.")
        return self._request(
            method='POST',
            path=f"apps/{app_id}",
            json_body=body
        )

    def delete_app(
            self,
            app_name: str
    ) -> Dict[str, Any]:
        """
        Delete an existing Quickbase application: DELETE /v1/apps/{appId}
        
        Args:
            app_name (str): The name of the application to delete. (required)
            
        Returns:
            dict: The deleted app's App ID.
        """
        app_id, _ = self._ids(app_name)

        return self._request(
            method='DELETE',
            path=f"apps/{app_id}",
            params={'appId': app_id}
        )

    def copy_app(
        self, 
        app_name: str, 
        new_app_name: str,
        description: Optional[str] = None, 
        properties: Optional[Dict[str, Any]] = None,
        ) -> Dict[str, Any]:
        """
        Copy an existing Quickbase application: POST /v1/apps/{appId}/copy
        
        Args:
            app_name (str): The name of the application to copy. (required)
            new_app_name (str): The name for the new application. (required)
            description (str, optional): A description for the new application.
            properties (dict, optional): Additional properties for the new application.
        """
        app_id, _ = self._ids(app_name)

        body = {
            "name": new_app_name,
            "description": description,
            "properties": properties
        }
        return self._request(
            method='POST',
            path=f"apps/{app_id}/copy",
            json_body=body
        )

    # ----------------
    # Table Methods
    # ----------------
    def create_table(
            self,
            app_name: str,
            table_name: str,
            description: Optional[str] = None,
            singular_record_name: Optional[str] = None,
            plural_record_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new Quickbase table: POST /v1/tables?appId={appId}
        
        Args:
            app_id (str): The name of the application to create the table in. (required)
            table_name (str): The name of the new table. (required)
            description (str, optional): A description for the new table.
            singular_record_name (str, optional): Singular name for records in this table.
            plural_record_name (str, optional): Plural name for records in this table.

        Returns:
            dict: The created table's metadata.
        """
        app_id, _ = self._ids(app_name)

        body = {
            "name": table_name,
            "description": description,
            "singularRecordName": singular_record_name,
            "pluralRecordName": plural_record_name
        }
        return self._request(
            method='POST',
            path='tables',
            params={'appId': app_id},
            json_body=body
        )

    def get_tables_for_app(
            self, 
            app_name: str
    ) -> List[Dict[str, Any]]:
        """
        List tables for a given app: GET /v1/tables?appId={appId}

        Args:
            app_name (str): The name of the application to list tables for. (required)

        Returns:
            list: A list of dictionaries containing metadata for each table in the app.
        """
        app_id, _ = self._ids(app_name)

        return self._request(
            method='GET',
            path='tables',
            params={'appId': app_id}
        )

    def get_table(
            self,
            app_name: str,
            table_name: str,
    ) -> Dict[str, Any]:
        """
        Get table metadata: GET /v1/tables/{tableId}?appId={appId}

        Args:
            app_name (str): The name of the application to get the table from. (required)
            table_name (str): The name of the table to get. (required)

        Returns:
            dict: The table's metadata.
        """
        app_id, table_id = self._ids(app_name, table_name)

        return self._request(
            method='GET',
            path=f"tables/{table_id}",
            params={'appId': app_id}
        )

    def update_table(
            self,
            app_name: str,
            table_name: str,
            new_table_name: Optional[str] = None,
            singular_record_name: Optional[str] = None,
            plural_record_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update table metadata: POST /v1/tables/{tableId}?appId={appId}

        Args:
            app_name (str): The name of the application to update the table in. (required)
            table_name (str): The name of the table to update. (required)
            new_table_name (str, optional): The new name for the table.
            singular_record_name (str, optional): Singular name for records in this table.
            plural_record_name (str, optional): Plural name for records in this table.

        Returns:
            dict: The updated table's metadata.
        """
        app_id, table_id = self._ids(app_name, table_name)

        body: Dict[str, Any] = {}
        if new_table_name:
            body["name"] = new_table_name
        if singular_record_name:
            body["singularRecordName"] = singular_record_name
        if plural_record_name:
            body["pluralRecordName"] = plural_record_name
        
        if not body:
            raise ValueError("Must specify at least one field to update (new_table_name, singular_record_name, or plural_record_name).")

        return self._request(
            method='POST',
            path=f"tables/{table_id}",
            params={'appId': app_id},
            json_body=body
        )

    def delete_table(
            self, 
            app_name: str, 
            table_name: str
    ) -> Dict[str, Any]:
        """
        Delete a table: DELETE /v1/tables/{tableId}?appId={appId}

        Args:
            app_name (str): The name of the application to delete the table from. (required)
            table_name (str): The name of the table to delete. (required)

        Returns:
            dict: The deleted table's Table ID.
        """
        app_id, table_id = self._ids(app_name, table_name)

        return self._request(
            method='DELETE',
            path=f"tables/{table_id}",
            params={'appId': app_id}
        )

    def get_all_relationships(
            self, 
            app_name: str, 
            table_name: str
    ) -> List[Dict[str, Any]]:
        """
        List relationships: GET /v1/tables/{tableId}/relationships

        Args:
            app_name (str): The name of the application to list relationships for. (required)
            table_name (str): The name of the table to list relationships for. (required)

        Returns:
            list: A list of dictionaries containing metadata for each relationship in the table.
        """
        app_id, table_id = self._ids(app_name, table_name)

        resp = self._request(
            method='GET',
            path=f"tables/{table_id}/relationships",
            params={'appId': app_id}
        )
        return resp.get('relationships', [])

    def create_relationship(
        self,
        app_name: str,
        table_name: str,
        parent_table_name: str,
        foreign_key_label: Optional[str] = None,
        lookup_field_ids: Optional[List[int]] = None,
        summary_fields: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Create a relationship between two tables: POST /v1/tables/{tableId}/relationship

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the child table.
            parent_table_name (str): The name of the parent table.
            foreign_key_label (Optional[str]): Label for the reference field created in child.
            lookup_field_ids (Optional[List[int]]): List of parent field IDs to create lookup fields for.
            summary_fields (Optional[List[Dict[str, Any]]]): List of summary field definitions.

        Returns:
            dict: The created relationship metadata.
        """
        app_id, table_id = self._ids(app_name, table_name)

        parent_id = self.meta.get_table_id(app_id, parent_table_name)
        body: Dict[str, Any] = {
            'parentTableId': parent_id,
            'childTableId': table_id
        }
        if foreign_key_label:
            body['foreignKeyField'] = {'label': foreign_key_label}
        if lookup_field_ids:
            body['lookupFieldIds'] = lookup_field_ids
        if summary_fields:
            body['summaryFields'] = summary_fields
        return self._request(
            method='POST',
            path=f"tables/{table_id}/relationship",
            params={'appId': app_id},
            json_body=body
        )

    def delete_relationship(
        self,
        app_name: str,
        table_name: str,
        related_field: str,
    ) -> dict:
        """
        Delete a relationship between two tables.

        Args:
            app_name (str): The name of the application.
            table_name (str): Child table (where the reference field lives).
            related_field (str): The label of the field in the child table that is a reference to the parent table.

        Returns:
            dict: The relationshipId that was deleted.
        """
        app_id, table_id = self._ids(app_name, table_name)

        rel_id = self.meta.get_field_id(app_id, table_id, related_field)
        resp = self._request(
            method='DELETE',
            path=f"relationship/{rel_id}",
            params={'appId': app_id}
        )

        return resp.get('relationshipId', None)

    # ----------------
    # Report Methods
    # ----------------
    def get_reports_for_table(
        self, 
        app_name: str, 
        table_name: str
    ) -> List[Dict[str, Any]]:
        """
        List reports for a table: GET /v1/reports?tableId={tableId}

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.

        Returns:
            list: A list of dictionaries containing metadata for each report in the table.
        """
        _, table_id = self._ids(app_name, table_name)

        resp =  self._request(
            method='GET',
            path='reports',
            params={'tableId': table_id}
        )
        return resp.get('reports', [])

    def get_report(
        self,
        app_name: str,
        table_name: str,
        report_id: int
    ) -> Dict[str, Any]:
        """
        Get report metadata for an individual report: GET /v1/reports/{reportId}

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            report_id (int): Report ID.
        """
        _, table_id = self._ids(app_name, table_name)

        return self._request(
            method='GET',
            path=f"reports/{report_id}",
            params={'tableId': table_id}
        )

    def run_report(
        self, 
        app_name: str, 
        table_name: str, 
        report_id: int,
        skip: int = 0, 
        top: int = 1000
    ) -> pd.DataFrame:
        """
        Run a report: POST /v1/reports/{reportId}/run
        
        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            report_id (int): Report ID.
            skip (int): Number of records to skip. Default is 0.
            top (int): Number of records to return. Default is 1000.
            
        Returns:
            pd.DataFrame: DataFrame containing the report data.
        """
        _, table_id = self._ids(app_name, table_name)

        params = {
            'tableId': table_id,
            'skip': skip,
            'top': top
        }
        resp = self._request(
            method='POST',
            path=f"reports/{report_id}/run",
            params=params
        )
        return pd.DataFrame(self._parse_report(resp))

    @staticmethod
    def _parse_report(
        self, 
        resp: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parse the report response to extract field labels and values.
        
        Args:
            resp (dict): The response from the report run.
            
        Returns:
            list: A list of dictionaries containing field labels and their corresponding values.
        """
        fields = resp.get("fields", [])
        data   = resp.get("data", [])
        return [
            {f['label']: rec.get(str(f['id']), {}).get('value') for f in fields}
            for rec in data
        ]

    # ----------------
    # Field Methods
    # ----------------
    def create_field(
        self,
        app_name: str,
        table_name: str,
        label: str,
        field_type: str
    ) -> Dict[str, Any]:
        """
        Create a new field in a table: POST /v1/fields

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            label (str): Field label (name).
            field_type (str): Quickbase field type (e.g., 'text', 'numeric', etc.).

        Returns:
            dict: Created field metadata.
        """
        _, table_id = self._ids(app_name, table_name)

        body = {
            "tableId": table_id,
            "label": label,
            "fieldType": field_type
        }
        return self._request(
            method='POST',
            path='fields',
            json_body=body
        )

    def delete_fields(
        self,
        app_name: str,
        table_name: str,
        field_labels: List[str]
    ) -> Dict[str, Any]:
        """
        Delete one or more fields from a table: DELETE /v1/fields

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            field_labels (List[str]): List of field labels to delete.

        Returns:
            dict: Response from the API.
        """
        app_id, table_id = self._ids(app_name, table_name)

        fmap = self.meta.get_field_map(app_id, table_id)

        # Lookup field IDs based on the provided labels
        field_ids = [fmap[label]['id'] for label in field_labels]
        body = {
            "tableId": table_id,
            "fieldIds": field_ids
        }
        return self._request(
            method='DELETE',
            path='fields',
            json_body=body
        )

    # ----------------
    # Formula Methods
    # ----------------
    def run_formula(
        self,
        app_name: str,
        table_name: str,
        formula: str,
        record_id: Optional[int] = None
    ) -> Any:
        """
        Run a formula: POST /v1/formula/run

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            formula (str): The formula string to evaluate.
            record_id (Optional[int]): Record ID to run the formula against (if required).

        Returns:
            Any: The result of the formula as a string.
        """
        _, table_id = self._ids(app_name, table_name)

        body = {
            "from": table_id,
            "formula": formula
        }
        if record_id is not None:
            body["rid"] = record_id

        return self._request(
            method='POST',
            path='formula/run',
            json_body=body
        )

    # ----------------
    # Record Methods
    # ----------------
    def upsert_records(
        self,
        app_name: str,
        table_name: str,
        records: List[Dict[str, Any]],
        merge_field_label: Optional[str] = None,
        fields_to_return: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Upsert (insert or update) records: POST /v1/records

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            records (List[Dict[str, Any]]): List of record dicts.
            merge_field_label (Optional[str]): Field label to match for updates (optional).
            fields_to_return (Optional[List[str]]): Field labels to return in response (optional).

        Returns:
            dict: Raw API response.
        """
        app_id, table_id = self._ids(app_name, table_name)
        fmap = self.meta.get_field_map(app_id, table_id)

        # Build the records array
        api_records = []
        for rec in records:
            new_rec: Dict[str, Dict[str, Any]] = {}
            for label, val in rec.items():
                if label not in fmap:
                    raise ValueError(f"Field '{label}' not found in table '{table_name}'.")
                fid = fmap[label]['id']
                new_rec[str(fid)] = {"value": val}
            api_records.append(new_rec)

        body = {
            "to": table_id,
            "data": api_records
        }

        if merge_field_label:
            field_id = self.meta.get_field_id(app_id, table_id, merge_field_label)
            body["mergeFieldId"] = field_id

        if fields_to_return:
            fmap = self.meta.get_field_map(app_id, table_id)
            body["fieldsToReturn"] = [fmap[label]["id"] for label in fields_to_return]

        resp = self._request(
            method='POST',
            path='records',
            json_body=body
        )

        metadata = resp.get("metadata", {})
        status = metadata.get("statusCode", 200)

        # Handle 207 partial success
        if status == 207:
            return {
                "success": False,
                "partial": True,
                "lineErrors": metadata.get("lineErrors", {}),
                "createdRecordIds": metadata.get("createdRecordIds", []),
                "totalProcessed": metadata.get("totalNumberOfRecordsProcessed", 0),
            }
        
        # Handle 200 success
        elif status == 200:
            return {
                "success": True,
                "createdRecordIds": metadata.get("createdRecordIds", []),
                "totalProcessed": metadata.get("totalNumberOfRecordsProcessed", 0),
            }
        
        # Handle other status codes
        else:
            return {
                "success": False,
                "error": resp.get("errors", resp)
            }

    def delete_records(
        self,
        app_name: str,
        table_name: str,
        where: Union[str, List[int]]
    ) -> int:
        """
        Delete records from a table: DELETE /v1/records

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            where (str or list): Either a Quickbase formula query string or a list of record IDs.

        Returns:
            int: Number of records successfully deleted.
        """
        _, table_id = self._ids(app_name, table_name)

        if not isinstance(where, (str, list)):
            raise ValueError("'where' must be either a Quickbase query string or a list of record IDs.")

        body = {
            "from": table_id,
            "where": where
        }

        resp = self._request(
            method='DELETE',
            path='records',
            json_body=body
        )

        return resp.get('numberDeleted', 0)

    def query_records(
        self, 
        app_name: str, 
        table_name: str,
        select_fields: List[str],
        where: Optional[str] = None,
        skip: int = 0, top: int = 1000
    ) -> Dict[str, Any]:
        """
        Query records: POST /v1/records/query

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            select_fields (List[str]): List of field labels to select.
            where (Optional[str]): Quickbase formula query string (optional).
            skip (int): Number of records to skip. Default is 0.
            top (int): Number of records to return. Default is 1000.

        Returns:
            dict: Raw API response.
        """
        app_id, table_id = self._ids(app_name, table_name)

        if not select_fields:
            raise ValueError("Must specify at least one field to select.")
        
        fmap     = self.meta.get_field_map(app_id, table_id)
        fids     = [fmap[label]['id'] for label in select_fields]
        body     = {
            'from':   table_id,
            'select': fids,
            'where':  where,
            'options': {'skip': skip, 'top': top}
        }
        return self._request(
            method='POST',
            path='records/query',
            json_body=body
        )

    def query_dataframe(
        self, 
        app_name: str, 
        table_name: str,
        select_fields: List[str],
        where: Optional[str] = None,
        skip: int = 0, 
        top: int = 1000
    ) -> pd.DataFrame:
        """
        Query records and return as a DataFrame: POST /v1/records/query
        
        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            select_fields (List[str]): List of field labels to select.
            where (Optional[str]): Quickbase formula query string (optional).
            skip (int): Number of records to skip. Default is 0.
            top (int): Number of records to return. Default is 1000.
            
        Returns:
            pd.DataFrame: DataFrame containing the queried records.
        """
        app_id, table_id = self._ids(app_name, table_name)

        body = {
            'from':   table_id,
            'select': [self.meta.get_field_map(app_id, table_id)[label]['id'] for label in select_fields],
            'where':  where,
            'options': {'skip': skip, 'top': top}
        }
        resp = self._request(
            method='POST',
            path='records/query',
            json_body=body
        )
        data   = resp.get('data', [])
        fields = resp.get('fields', [])
        cols   = [f['label'] for f in fields]
        rows   = [
            {f['label']: rec.get(str(f['id']), {}).get('value') for f in fields}
            for rec in data
        ]
        return pd.DataFrame(rows, columns=cols)

    # ----------------
    # Async CSV Download
    # ----------------
    async def _fetch_chunk(
        self, 
        session, 
        url: str, 
        headers: Dict[str, Any], 
        body: Dict[str, Any],
        offset: int,
        max_retries: int = 5,
        base_delay: float = 0.5,
        max_delay: float = 10.0
    ) -> List[Dict[str, Any]]:
        attempt = 0
        while True:
            async with session.post(url, headers=headers, json=body) as resp:
                if resp.status in (429, 502, 503) and attempt < max_retries:
                    retry = resp.headers.get('Retry-After')
                    wait = float(retry) if retry else min(max_delay, base_delay * 2 ** attempt) * random.uniform(0.8, 1.2)
                    self.logger.warning(f"429 at offset {offset}; retry #{attempt+1} in {wait:.1f}s")
                    await asyncio.sleep(wait)
                    attempt += 1
                    continue
                resp.raise_for_status()
                payload = await resp.json()
            rows = [
                {f['label']: r.get(str(f['id']), {}).get('value') for f in payload.get('fields', [])}
                for r in payload.get('data', [])
            ]
            self.logger.info(f"Chunk at offset {offset}: {len(rows)} rows")
            return rows

    async def _gather_chunks(
        self, url: str, headers: dict, table_id: str,
        field_ids: List[int], where: str,
        batch_params: List[tuple], max_concurrency: int
    ) -> List[List[Dict[str, Any]]]:
        sem = asyncio.Semaphore(max_concurrency)
        conn = aiohttp.TCPConnector(limit=0)
        async with aiohttp.ClientSession(connector=conn) as session:
            tasks = []
            for offset, top in batch_params:
                body = {
                    'from': table_id,
                    'select': field_ids,
                    'where': where,
                    'options': {'skip': offset, 'top': top}
                }
                async def task(off, b=body):
                    async with sem:
                        return await self._fetch_chunk(session, url, headers, b, off)
                tasks.append(task(offset))
            self.logger.info(f"Dispatching {len(tasks)} chunk tasks")
            return await asyncio.gather(*tasks)

    def download_records_to_csv(
        self, 
        app_name: str, 
        table_name: str, 
        where: str,
        output_dir: str,
        chunk_size: int = 1000,
        record_limit: Optional[int] = None,
        max_concurrency: int = 8
    ) -> str:
        """
        Download all records matching a query into a CSV by fetching in parallel:
        POST /v1/records/query in chunks.
        
        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            where (str): Quickbase formula query string.
            output_dir (str): Directory to save the CSV file.
            chunk_size (int): Number of records to fetch in each chunk. Default is 1000.
            record_limit (Optional[int]): Maximum number of records to download. Default is None (all records).
            max_concurrency (int): Max number of concurrent requests. Default is 8.
            
        Returns:
            str: Path to the saved CSV file, or an empty string if no records found.
        """
        app_id, table_id = self._ids(app_name, table_name)

        # Build field list and figure out total rows
        fmap     = self.meta.get_field_map(app_id, table_id)
        fids     = [info['id'] for info in fmap.values()]
        size     = self.meta.get_table(app_id, table_id)['size']
        tbl_info = self.meta.get_table(app_id, table_id)
        app_key = self.meta.normalize_app(app_id)
        friendly_name = next(
            (tn for tn, inf in self.meta.cache[app_key]['tables'].items() if inf['id'] == tbl_info['id']),
            table_id
        )
        total    = min(size, record_limit) if record_limit else size

        # Prepare batches
        batches = [(o, min(chunk_size, 1000)) for o in range(0, total, chunk_size)]
        headers = {**self.transport.headers, 'Accept-Encoding': 'gzip'}
        url     = f"{self.transport.base_url}/records/query"

        # Run the async fetch
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        all_chunks = asyncio.run(
            self._gather_chunks(url, headers, table_id, fids, where, batches, max_concurrency)
        )

        # Flatten the list of chunks and write to CSV
        records = [r for chunk in all_chunks for r in chunk]
        if not records:
            self.logger.warning("No records for filter.")
            return ''

        df      = pd.DataFrame(records)
        ts      = datetime.now().strftime("%Y-%m-%d")
        out_csv = Path(output_dir) / f"{friendly_name}_{ts}.csv"
        df.to_csv(out_csv, index=False)
        self.logger.info(f"Wrote {len(df)} records to {out_csv}")
        return str(out_csv)

    # ----------------
    # File-attachment Methods
    # ----------------
    def get_file_attachment_fields(
        self, 
        app_name: str, 
        table_name: str
    ) -> List[str]:
        """
        Get all file attachment field labels for a given table.
        
        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
        
        Returns:
            list: List of field labels for file attachment fields.
        """
        app_id, table_id = self._ids(app_name, table_name)

        fmap = self.meta.get_field_map(app_id, table_id)
        return [label for label, meta in fmap.items() if meta.get('type') == 'file']

    async def _async_download_attachment(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        download_url: str,
        save_path: Path
    ):
        if save_path.exists():
            self.logger.warning(f"File already exists: {save_path.name}")
            return

        async with sem:
            try:
                async with session.get(download_url) as resp:
                    resp.raise_for_status()
                    async with aiofiles.open(save_path, 'wb') as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)
                self.logger.info(f"Downloaded: {save_path.name}")
            except Exception as e:
                self.logger.error(f"Failed to download {download_url}: {e}")

    async def _async_download_attachments(
        self,
        download_jobs: List[Dict[str, Any]],
        target_dir: str,
        max_concurrency: int = 8
    ) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(max_concurrency)
        output = []

        connector = aiohttp.TCPConnector(limit=None)
        async with aiohttp.ClientSession(
            connector=connector, 
            headers=self.transport.headers
        ) as session:
            tasks = []
            for job in download_jobs:
                rid = job['record_id']
                filename = job['file_name']
                url = job['url']
                save_path = Path(target_dir) / f"{rid}_{filename}"
                output.append({
                    "record_id": rid,
                    "file_name": filename,
                    "saved_path": str(save_path)
                })
                tasks.append(self._async_download_attachment(session, sem, url, save_path))
            await asyncio.gather(*tasks)
        return output

    def download_attachments_async(
        self,
        app_name: str,
        table_name: str,
        file_field_label: str,
        target_dir: str,
        where: Optional[str] = None,
        max_concurrency: int = 8
    ) -> List[Dict[str, Any]]:
        """
        Download all attachments in a given file field to disk asynchronously.

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            file_field_label (str): Name of the file attachment field.
            target_dir (str): Directory to save attachments.
            where (Optional[str]): Optional Quickbase formula query filter.
            max_concurrency (int): Max number of concurrent downloads (default 8).

        Returns:
            list: List of dicts with record_id, file_name, and saved_path.
        """
        app_id, table_id = self._ids(app_name, table_name)

        Path(target_dir).mkdir(parents=True, exist_ok=True)
        fmap     = self.meta.get_field_map(app_id, table_id)
        file_fid = fmap[file_field_label]['id']
        record_fid = 3  # Default Quickbase Record ID field

        query_body = {
            "from": table_id,
            "select": [record_fid, file_fid],
            "where": where
        }
        resp = self._request(
            method='POST',
            path='records/query',
            json_body=query_body
        )
        records = resp.get('data', [])

        # Build download jobs
        download_jobs = []
        for record in records:
            rid = record.get(str(record_fid), {}).get('value')
            file_info = record.get(str(file_fid), {}).get('value') or {}
            filename = file_info.get('fileName')
            url_path = file_info.get('url')

            if not (rid and filename and url_path):
                continue

            download_jobs.append({
                "record_id": rid,
                "file_name": filename,
                "url": f'https://api.quickbase.com/v1{url_path}'
            })

        if not download_jobs:
            self.logger.warning("No attachments found to download.")
            return []

        output = asyncio.run(
            self._async_download_attachments(
                download_jobs,
                target_dir,
                max_concurrency=max_concurrency
            )
        )

        self.logger.info(f"Downloaded {len(output)} attachments.")
        return output

    # ----------------
    # Utility
    # ----------------
    def get_field_id(self, app_id: str, table_id: str, field_label: str) -> int:
        return self.meta.get_field_id(app_id, table_id, field_label)

    def get_table_id(self, app_id: str, table_id: str) -> str:
        return self.meta.get_table_id(app_id, table_id)

    def get_field(self, app_id, table_id, field_id):
        return self.transport.get(
            f"fields/{field_id}",
            params={"tableId": self.meta.get_table_id(app_id, table_id)}
    )

    # ----------------
    # Config Debugging
    # ----------------
    def summarize_config(self, show_fields: bool = False):
        """
        Logs a hierarchical summary of the current configuration using a directory-style layout.

        Args:
            show_fields (bool): Whether to include a breakdown of all fields under each table.
        """
        if not self.meta.cache:
            self.logger.info("No config data available.")
            return
        self.logger.info("QuickBase Config Overview:")
        for app_name, app_data in self.meta.cache.items():
            self.logger.info(f"{app_name}")
            for table_name, table_data in app_data.get("tables", {}).items():
                tid = table_data.get('id')
                size = table_data.get('size', 'N/A')
                self.logger.info(f"  └── 🔹 {table_name}  [ID: {tid}, Size: {size} records]")
                if show_fields:
                    for field_label, meta in table_data.get("fields", {}).items():
                        fid = meta.get('id')
                        ftype = meta.get('type')
                        self.logger.info(f"      └── 🔹 {field_label} (ID: {fid}, Type: {ftype})")

    def dump_full_config(self):
        """
        Dumps the full in-memory config dictionary to the logger in JSON format.
        Useful for debugging large applications where fields/tables may be misconfigured.
        """
        try:
            config_str = json.dumps(self.meta.cache, indent=2)
            self.logger.info(f"Full QuickBase Config Dump:" + config_str)
        except Exception as e:
            self.logger.error(f"Failed to serialize config: {e}")