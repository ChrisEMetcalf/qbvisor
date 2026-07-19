import asyncio
import base64
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypeVar, cast, overload
from uuid import uuid4

import aiofiles
import pandas as pd
from dotenv import load_dotenv

from ._attachments import latest_attachment
from ._pagination import iter_intelligent_pages
from ._records.pagination import iter_record_pages_by_id
from ._records.upsert import normalize_upsert_response
from ._resources.apps import AppResource
from ._resources.fields import FieldResource
from ._resources.relationships import RelationshipResource
from ._resources.tables import TableResource
from .async_transport import AsyncQuickBaseTransport
from .backup import ApplicationBackup, BackupOptions
from .exceptions import QuickbaseBatchError, QuickbaseResponseError
from .helpers import sanitize_filenames
from .log_runner import get_logger
from .metadata import QuickBaseMetaCache
from .models import RelationshipSummary
from .schema import AppSpec, SchemaApplyResult, SchemaPlan
from .transport import QuickBaseTransport, RetryPolicy

logger = get_logger(__name__)

ResponseT = TypeVar("ResponseT")


def _normalize_utc_timestamp(value: datetime | str) -> str:
    """Validate a timestamp and normalize it to whole-second ISO-8601 UTC."""
    if isinstance(value, str):
        source = value.strip()
        try:
            parsed = datetime.fromisoformat(source.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("after must be a valid ISO-8601 timestamp") from error
    else:
        parsed = value
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("after must include a timezone")
    normalized = parsed.astimezone(UTC).replace(microsecond=0)
    return normalized.isoformat(timespec="seconds").replace("+00:00", "Z")


class QuickBaseClient:
    """
    High-level QuickBase client that composes transport, metadata, and file utilities.
    """

    def __init__(self, transport: QuickBaseTransport | None = None):
        load_dotenv()
        self._owns_transport = transport is None
        self.transport = transport if transport is not None else QuickBaseTransport()
        self.meta = QuickBaseMetaCache(self.transport)
        self.logger = get_logger(__name__)
        self._apps = AppResource(self)
        self._fields = FieldResource(self)
        self._relationships = RelationshipResource(self)
        self._tables = TableResource(self)

    def __enter__(self) -> "QuickBaseClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def close(self) -> None:
        """Close the transport created by this client."""
        if self._owns_transport:
            self.transport.close()

    def backup_app(
        self,
        app_name: str,
        output_dir: str | Path,
        *,
        options: BackupOptions | None = None,
    ) -> ApplicationBackup:
        """Create an atomic, versioned application backup under ``output_dir``."""
        from ._backup.workflow import create_application_backup

        return create_application_backup(
            self,
            app_name,
            output_dir,
            options=options or BackupOptions(),
        )

    def plan_app(
        self,
        spec: AppSpec,
        *,
        state_path: str | Path = ".qbvisor/state.json",
    ) -> SchemaPlan:
        """Compare a declarative app specification without mutating Quickbase or state."""
        from ._schema.planner import plan_application_schema

        return plan_application_schema(self, spec, state_path=state_path)

    def apply_app(self, plan: SchemaPlan) -> SchemaApplyResult:
        """Apply a reviewed app plan and publish state only after verification."""
        from ._schema.apply import apply_application_schema

        result = apply_application_schema(self, plan)
        app_resource = result.state.resource(plan.spec.address)
        if (
            result.quickbase_change_count
            and app_resource is not None
            and isinstance(app_resource.remote_id, str)
            and app_resource.remote_id in self.meta.app_ids.values()
        ):
            self.meta.invalidate_tables(app_resource.remote_id)
        return result

    # ----------------
    # Private: Map friendly names to IDs
    # ----------------
    @overload
    def _ids(self, app_name: str, table_name: None = None) -> tuple[str, None]: ...

    @overload
    def _ids(self, app_name: str, table_name: str) -> tuple[str, str]: ...

    def _ids(
        self,
        app_name: str,
        table_name: str | None = None,
    ) -> tuple[str, str | None]:
        """
        Map friendly names to IDs.
        """
        app_id = self.meta.get_app_id(app_name)
        if table_name is None:
            return app_id, None
        table_id = self.meta.get_table_id(app_id, table_name)
        return app_id, table_id

    @overload
    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        retry_policy: RetryPolicy | None = None,
    ) -> dict[str, Any]: ...

    @overload
    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        retry_policy: RetryPolicy | None = None,
        *,
        response_type: type[ResponseT],
    ) -> ResponseT: ...

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        retry_policy: RetryPolicy | None = None,
        *,
        response_type: type[Any] = dict,
    ) -> Any:
        """
        Centralized request handling with logging.
        """
        try:
            func = getattr(self.transport, method.lower())
            if retry_policy is not None:
                payload = func(
                    path,
                    params=params,
                    json_body=json_body,
                    retry_policy=retry_policy,
                )
            else:
                payload = func(path, params=params, json_body=json_body)
            if not isinstance(payload, response_type):
                expected = "JSON object" if response_type is dict else "JSON array"
                raise QuickbaseResponseError(
                    method,
                    path,
                    expected=expected,
                    actual=type(payload).__name__,
                )
            return cast(Any, payload)
        except Exception as e:
            self.logger.error(f"Error in {method} request to {path}: {e}")
            raise

    # ----------------
    # App Methods
    # ----------------
    def create_app(
        self,
        name: str,
        description: str | None = None,
        assign_token: bool = False,
        variables: list[dict[str, str]] | None = None,
        security_properties: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
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
        return self._apps.create(
            name,
            description=description,
            assign_token=assign_token,
            variables=variables,
            security_properties=security_properties,
        )

    def get_app(self, app_name: str) -> dict[str, Any]:
        """
        Get app metadata: GET /v1/apps/{appId}
        """
        return self._apps.get(app_name)

    def get_app_events(self, app_name: str) -> list[dict[str, Any]]:
        """List events configured in an app: GET /v1/apps/{appId}/events."""
        return self._apps.events(app_name)

    def get_app_roles(self, app_name: str) -> list[dict[str, Any]]:
        """List roles configured in an app: GET /v1/apps/{appId}/roles."""
        return self._apps.roles(app_name)

    def update_app(
        self,
        app_name: str,
        new_name: str | None = None,
        description: str | None = None,
        variables: list[dict[str, str]] | None = None,
        security_properties: dict[str, bool] | None = None,
    ) -> dict[str, Any]:
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
        return self._apps.update(
            app_name,
            new_name=new_name,
            description=description,
            variables=variables,
            security_properties=security_properties,
        )

    def delete_app(self, app_name: str) -> dict[str, Any]:
        """
        Delete an existing Quickbase application: DELETE /v1/apps/{appId}

        Args:
            app_name (str): The name of the application to delete. (required)

        Returns:
            dict: The deleted app's App ID.
        """
        return self._apps.delete(app_name)

    def copy_app(
        self,
        app_name: str,
        new_app_name: str,
        description: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Copy an existing Quickbase application: POST /v1/apps/{appId}/copy

        Args:
            app_name (str): The name of the application to copy. (required)
            new_app_name (str): The name for the new application. (required)
            description (str, optional): A description for the new application.
            properties (dict, optional): Additional properties for the new application.
        """
        return self._apps.copy(
            app_name,
            new_app_name,
            description=description,
            properties=properties,
        )

    # ----------------
    # Table Methods
    # ----------------
    def create_table(
        self,
        app_name: str,
        table_name: str,
        description: str | None = None,
        singular_record_name: str | None = None,
        plural_record_name: str | None = None,
    ) -> dict[str, Any]:
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
        return self._tables.create(
            app_name,
            table_name,
            description=description,
            singular_record_name=singular_record_name,
            plural_record_name=plural_record_name,
        )

    def get_tables_for_app(self, app_name: str) -> list[dict[str, Any]]:
        """
        List tables for a given app: GET /v1/tables?appId={appId}

        Args:
            app_name (str): The name of the application to list tables for. (required)

        Returns:
            list: A list of dictionaries containing metadata for each table in the app.
        """
        return self._tables.list(app_name)

    def get_table(
        self,
        app_name: str,
        table_name: str,
    ) -> dict[str, Any]:
        """
        Get table metadata: GET /v1/tables/{tableId}?appId={appId}

        Args:
            app_name (str): The name of the application to get the table from. (required)
            table_name (str): The name of the table to get. (required)

        Returns:
            dict: The table's metadata.
        """
        return self._tables.get(app_name, table_name)

    def update_table(
        self,
        app_name: str,
        table_name: str,
        new_table_name: str | None = None,
        singular_record_name: str | None = None,
        plural_record_name: str | None = None,
    ) -> dict[str, Any]:
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
        return self._tables.update(
            app_name,
            table_name,
            new_table_name=new_table_name,
            singular_record_name=singular_record_name,
            plural_record_name=plural_record_name,
        )

    def delete_table(self, app_name: str, table_name: str) -> dict[str, Any]:
        """
        Delete a table: DELETE /v1/tables/{tableId}?appId={appId}

        Args:
            app_name (str): The name of the application to delete the table from. (required)
            table_name (str): The name of the table to delete. (required)

        Returns:
            dict: The deleted table's Table ID.
        """
        return self._tables.delete(app_name, table_name)

    def get_all_relationships(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        """
        List relationships: GET /v1/tables/{tableId}/relationships

        Args:
            app_name (str): The name of the application to list relationships for. (required)
            table_name (str): The name of the table to list relationships for. (required)

        Returns:
            list: A list of dictionaries containing metadata for each relationship in the table.
        """
        return self._relationships.get_all(app_name, table_name)

    def create_relationship(
        self,
        app_name: str,
        table_name: str,
        parent_table_name: str,
        foreign_key_label: str | None = None,
        lookup_field_ids: list[int] | None = None,
        summary_fields: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
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
        return self._relationships.create(
            app_name,
            table_name,
            parent_table_name,
            foreign_key_label=foreign_key_label,
            lookup_field_ids=lookup_field_ids,
            summary_fields=summary_fields,
        )

    def update_relationship(
        self,
        app_name: str,
        table_name: str,
        relationship: str | int,
        *,
        lookup_fields: Sequence[str | int] | None = None,
        summary_fields: Sequence[RelationshipSummary] | None = None,
    ) -> dict[str, Any]:
        """Add lookup or summary fields to an existing table relationship."""
        return self._relationships.update(
            app_name,
            table_name,
            relationship,
            lookup_fields=lookup_fields,
            summary_fields=summary_fields,
        )

    def delete_relationship(
        self,
        app_name: str,
        table_name: str,
        related_field: str,
    ) -> Any | None:
        """
        Delete a relationship between two tables.

        Args:
            app_name (str): The name of the application.
            table_name (str): Child table (where the reference field lives).
            related_field (str): The label of the field in the child table that is a reference to the parent table.

        Returns:
            dict: The relationshipId that was deleted.
        """
        return self._relationships.delete(app_name, table_name, related_field)

    # ----------------
    # Report Methods
    # ----------------
    def get_reports_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        """
        List reports for a table: GET /v1/reports?tableId={tableId}

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.

        Returns:
            list: A list of dictionaries containing metadata for each report in the table.
        """
        _, table_id = self._ids(app_name, table_name)

        return self._request(
            method="GET", path="reports", params={"tableId": table_id}, response_type=list
        )

    def get_report(self, app_name: str, table_name: str, report_id: int) -> dict[str, Any]:
        """
        Get report metadata for an individual report: GET /v1/reports/{reportId}

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            report_id (int): Report ID.
        """
        _, table_id = self._ids(app_name, table_name)

        return self._request(
            method="GET", path=f"reports/{report_id}", params={"tableId": table_id}
        )

    def run_report(
        self,
        app_name: str,
        table_name: str,
        report_id: int,
        skip: int = 0,
        top: int | None = None,
    ) -> pd.DataFrame:
        """
        Run a report: POST /v1/reports/{reportId}/run

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            report_id (int): Report ID.
            skip (int): Number of records to skip. Default is 0.
            top (Optional[int]): Maximum total records to return. By default, the complete report is returned.

        Returns:
            pd.DataFrame: DataFrame containing the report data.
        """
        _, table_id = self._ids(app_name, table_name)

        path = f"reports/{report_id}/run"

        def fetch_page(page_skip: int, page_top: int | None) -> dict[str, Any]:
            params: dict[str, Any] = {"tableId": table_id}
            if page_skip:
                params["skip"] = page_skip
            if page_top is not None:
                params["top"] = page_top
            return self._request(
                method="POST",
                path=path,
                params=params,
                retry_policy=RetryPolicy.SAFE,
            )

        rows: list[dict[str, Any]] = []
        for response in iter_intelligent_pages(
            fetch_page,
            path=path,
            skip=skip,
            top=top,
        ):
            rows.extend(self._parse_report(response))
        return pd.DataFrame(rows)

    @staticmethod
    def _parse_report(resp: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Parse the report response to extract field labels and values.

        Args:
            resp (dict): The response from the report run.

        Returns:
            list: A list of dictionaries containing field labels and their corresponding values.
        """
        fields = resp.get("fields", [])
        data = resp.get("data", [])
        return [
            {f["label"]: rec.get(str(f["id"]), {}).get("value") for f in fields} for rec in data
        ]

    # ----------------
    # Field Methods
    # ----------------
    def get_fields_for_table(self, app_name: str, table_name: str) -> list[dict[str, Any]]:
        """List fields and permissions for a table: GET /v1/fields."""
        return self._fields.list_for_table(app_name, table_name)

    def create_field(
        self, app_name: str, table_name: str, label: str, field_type: str
    ) -> dict[str, Any]:
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
        return self._fields.create(app_name, table_name, label, field_type)

    def delete_fields(
        self, app_name: str, table_name: str, field_labels: list[str]
    ) -> dict[str, Any]:
        """
        Delete one or more fields from a table: DELETE /v1/fields

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            field_labels (List[str]): List of field labels to delete.

        Returns:
            dict: Response from the API.
        """
        return self._fields.delete(app_name, table_name, field_labels)

    def get_fields_usage(
        self,
        app_name: str,
        table_name: str,
        *,
        skip: int | None = None,
        top: int | None = None,
    ) -> list[dict[str, Any]]:
        """Return usage statistics for fields in a table: GET /v1/fields/usage."""
        return self._fields.usage(app_name, table_name, skip=skip, top=top)

    def get_field_usage(
        self,
        app_name: str,
        table_name: str,
        field: str | int,
    ) -> list[dict[str, Any]]:
        """Return usage statistics for one field: GET /v1/fields/usage/{fieldId}."""
        return self._fields.usage_for_field(app_name, table_name, field)

    # ----------------
    # Formula Methods
    # ----------------
    def run_formula(
        self, app_name: str, table_name: str, formula: str, record_id: int | None = None
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

        body: dict[str, Any] = {"from": table_id, "formula": formula}
        if record_id is not None:
            body["rid"] = record_id

        return self._request(
            method="POST",
            path="formula/run",
            json_body=body,
            retry_policy=RetryPolicy.SAFE,
        )

    # ----------------
    # Record Methods
    # ----------------
    def upsert_records(
        self,
        app_name: str,
        table_name: str,
        records: list[dict[str, Any]],
        merge_field_label: str | None = None,
        fields_to_return: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Upsert (insert or update) records: POST /v1/records

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            records (List[Dict[str, Any]]): List of record dicts.
            merge_field_label (Optional[str]): Field label to match for updates (optional).
            fields_to_return (Optional[List[str]]): Field labels to return in response (optional).

        Returns:
            dict: Validated write outcome with returned data, record ID groups, and line errors.
        """
        app_id, table_id = self._ids(app_name, table_name)

        def get_id(label: str) -> int:
            return self.meta.get_field_id(app_id, table_id, label)

        # Build the records array using field IDs
        api_records = []
        for rec in records:
            new_rec: dict[str, dict[str, Any]] = {}
            for label, val in rec.items():
                new_rec[str(get_id(label))] = {"value": val}
            api_records.append(new_rec)

        # Build request body
        body: dict[str, Any] = {"to": table_id, "data": api_records}

        if merge_field_label:
            body["mergeFieldId"] = get_id(merge_field_label)

        if fields_to_return:
            body["fieldsToReturn"] = [get_id(label) for label in fields_to_return]

        # Make the request
        resp = self._request(method="POST", path="records", json_body=body)

        return normalize_upsert_response(resp, record_count=len(records))

    def delete_records(self, app_name: str, table_name: str, where: str | list[int]) -> int:
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
            raise ValueError(
                "'where' must be either a Quickbase query string or a list of record IDs."
            )

        body = {"from": table_id, "where": where}

        resp = self._request(method="DELETE", path="records", json_body=body)

        return resp.get("numberDeleted", 0)

    def query_records(
        self,
        app_name: str,
        table_name: str,
        select_fields: list[str] | None = None,
        where: str | None = None,
        sort_by: list[tuple[str, str]] | None = None,  # e.g., [("Date", "ASC")]
        group_by: list[str] | None = None,  # e.g., ["Client"]
        skip: int = 0,
        top: int = 1000,
    ) -> dict[str, Any]:
        """
        Query records: POST /v1/records/query

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            select_fields (Optional[List[str]]): Field labels to return. If not provided, default columns will be returned.
            where (Optional[str]): Quickbase formula query string (optional).
            sort_by (Optional[List[Tuple[str, str]]]): Field labels and sort directions.
            group_by (Optional[List[str]]): Field labels to group by.
            skip (int): Number of records to skip. Default is 0.
            top (int): Number of records to return. Default is 1000.

        Returns:
            dict: Raw API response.
        """
        app_id, table_id = self._ids(app_name, table_name)

        def get_id(label: str) -> int:
            return self.meta.get_field_id(app_id, table_id, label)

        return self._query_records_by_ids(
            table_id,
            select_fields=[get_id(label) for label in select_fields] if select_fields else None,
            where=where,
            sort_by=[(get_id(label), order) for label, order in sort_by] if sort_by else None,
            group_by=[get_id(label) for label in group_by] if group_by else None,
            skip=skip,
            top=top,
        )

    def _query_records_by_ids(
        self,
        table_id: str,
        *,
        select_fields: Sequence[int] | None = None,
        where: str | None = None,
        sort_by: Sequence[tuple[int, str]] | None = None,
        group_by: Sequence[int] | None = None,
        skip: int = 0,
        top: int | None = 1000,
    ) -> dict[str, Any]:
        """Query a resolved table directly; backup pagination avoids repeated metadata lookups."""
        options: dict[str, Any] = {"skip": skip}
        if top is not None:
            options["top"] = top
        body: dict[str, Any] = {"from": table_id, "options": options}
        if select_fields:
            body["select"] = list(select_fields)
        if where:
            body["where"] = where
        if sort_by:
            body["sortBy"] = [
                {"fieldId": field_id, "order": order.upper()} for field_id, order in sort_by
            ]
        if group_by:
            body["groupBy"] = [
                {"fieldId": field_id, "grouping": "equal-values"} for field_id in group_by
            ]
        return self._request(
            method="POST",
            path="records/query",
            json_body=body,
            retry_policy=RetryPolicy.SAFE,
        )

    def records_modified_since(
        self,
        app_name: str,
        table_name: str,
        after: datetime | str,
        *,
        field_list: Sequence[str | int] | None = None,
        include_details: bool = False,
    ) -> dict[str, Any]:
        """Find records changed after an ISO-8601 timestamp."""
        timestamp = _normalize_utc_timestamp(after)
        app_id, table_id = self._ids(app_name, table_name)
        body: dict[str, Any] = {
            "from": table_id,
            "after": timestamp,
            "includeDetails": include_details,
        }
        if field_list:
            body["fieldList"] = [
                field if isinstance(field, int) else self.meta.get_field_id(app_id, table_id, field)
                for field in field_list
            ]
        return self._request(
            method="POST",
            path="records/modifiedSince",
            json_body=body,
            retry_policy=RetryPolicy.SAFE,
        )

    def query_dataframe(
        self,
        app_name: str,
        table_name: str,
        select_fields: list[str],
        where: str | None = None,
        sort_by: list[tuple[str, str]] | None = None,  # e.g. [('Date', 'ASC'), ('Name', 'DESC')]
        group_by: list[str] | None = None,  # e.g. ['Category']
        skip: int = 0,
        top: int | None = None,
    ) -> pd.DataFrame:
        """
        Query records and return as a DataFrame: POST /v1/records/query

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            select_fields (List[str]): List of field labels to select.
            where (Optional[str]): Quickbase formula query string (optional).
            sort_by (Optional[List[Tuple[str, str]]]): Field labels and sort directions.
            group_by (Optional[List[str]]): Field labels to group by.
            skip (int): Number of records to skip. Default is 0.
            top (Optional[int]): Maximum total records to return. By default, the complete query is returned.

        Returns:
            pd.DataFrame: DataFrame containing the queried records.
        """
        if not isinstance(skip, int) or isinstance(skip, bool) or skip < 0:
            raise ValueError("skip must be a non-negative integer")
        if top is not None and (not isinstance(top, int) or isinstance(top, bool) or top < 0):
            raise ValueError("top must be a non-negative integer or None")

        app_id, table_id = self._ids(app_name, table_name)

        def get_id(label: str) -> int:
            return self.meta.get_field_id(app_id, table_id, label)

        selected_fields = [(label, get_id(label)) for label in select_fields]
        selected_ids = [field_id for _, field_id in selected_fields]
        resolved_sort = [(get_id(label), order) for label, order in sort_by] if sort_by else None
        resolved_group = [get_id(label) for label in group_by] if group_by else None

        can_use_keyset = (
            bool(selected_ids)
            and len(set(selected_ids)) == len(selected_ids)
            and skip == 0
            and not resolved_sort
            and not resolved_group
        )
        if can_use_keyset:
            scan_ids = list(selected_ids)
            if 3 not in scan_ids:
                scan_ids.append(3)
            keyset_rows = [
                {
                    label: record.get(str(field_id), {}).get("value")
                    for label, field_id in selected_fields
                }
                for page in iter_record_pages_by_id(
                    self,
                    table_id,
                    select_fields=scan_ids,
                    where=where,
                    page_size=None,
                    record_limit=top if top is not None and top > 0 else None,
                )
                for record in page
            ]
            return pd.DataFrame(keyset_rows, columns=select_fields)

        def fetch_page(page_skip: int, page_top: int | None) -> dict[str, Any]:
            return self._query_records_by_ids(
                table_id,
                select_fields=selected_ids,
                where=where,
                sort_by=resolved_sort,
                group_by=resolved_group,
                skip=page_skip,
                top=page_top,
            )

        paged_rows: list[dict[str, Any]] = []
        columns: list[str] | None = None
        for response in iter_intelligent_pages(
            fetch_page,
            path="records/query",
            skip=skip,
            top=top,
        ):
            fields = response["fields"]
            if columns is None:
                columns = [field["label"] for field in fields]
            paged_rows.extend(
                {field["label"]: record.get(str(field["id"]), {}).get("value") for field in fields}
                for record in response["data"]
            )
        return pd.DataFrame(paged_rows, columns=columns)

    # ----------------
    # CSV Download
    # ----------------
    def download_records_to_csv(
        self,
        app_name: str,
        table_name: str,
        output_dir: str,
        where: str = "{3.GT.'0'}",
        chunk_size: int = 1000,
        record_limit: int | None = None,
        max_concurrency: int = 4,
    ) -> str:
        """
        Download all records matching a query into a CSV using stable Record ID# pages.

        Args:
            app_name (str): The name of the application.
            table_name (str): The name of the table.
            output_dir (str): Directory to save the CSV file.
            where (str): Quickbase formula query string. Default is "{3.GT.'0'}" (all records).
            chunk_size (int): Number of records to fetch per page, capped at 1000.
            record_limit (Optional[int]): Maximum number of records to download. Default is None (all records).
            max_concurrency (int): Retained for compatibility. Record pages are fetched sequentially.

        Returns:
            str: Path to the saved CSV file, or an empty string if no records found.
        """
        if chunk_size < 1:
            raise ValueError("chunk_size must be at least 1")
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if record_limit is not None and record_limit < 0:
            raise ValueError("record_limit cannot be negative")

        app_id, table_id = self._ids(app_name, table_name)

        # Preserve metadata field order so every page produces the same CSV columns.
        fmap = self.meta.get_field_map(app_id, table_id)
        field_columns = [(label, int(info["id"])) for label, info in fmap.items()]
        field_labels = [label for label, _ in field_columns]
        fids = [field_id for _, field_id in field_columns]
        tbl_info = self.meta.get_table(app_id, table_id)
        app_key = self.meta.normalize_app(app_id)
        friendly_name = next(
            (
                tn
                for tn, inf in self.meta.cache[app_key]["tables"].items()
                if inf["id"] == tbl_info["id"]
            ),
            table_id,
        )
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y-%m-%d")
        out_csv = output_path / f"{friendly_name}_{ts}.csv"
        temp_csv = output_path / f".{out_csv.name}.{uuid4().hex}.tmp"

        records_written = 0
        try:
            for page in iter_record_pages_by_id(
                self,
                table_id,
                select_fields=fids,
                where=where,
                page_size=min(chunk_size, 1000),
                record_limit=record_limit,
            ):
                rows = [
                    {
                        label: record.get(str(field_id), {}).get("value")
                        for label, field_id in field_columns
                    }
                    for record in page
                ]
                frame = pd.DataFrame(rows, columns=field_labels)
                first_page = records_written == 0
                frame.to_csv(
                    temp_csv,
                    mode="w" if first_page else "a",
                    header=first_page,
                    index=False,
                )
                records_written += len(frame)
                self.logger.info(f"Wrote CSV page with {len(frame)} records")

            if not records_written:
                self.logger.warning("No records for filter.")
                return ""

            temp_csv.replace(out_csv)
        except BaseException:
            temp_csv.unlink(missing_ok=True)
            raise

        self.logger.info(f"Wrote {records_written} records to {out_csv}")
        return str(out_csv)

    # ----------------
    # File-attachment Methods
    # ----------------
    def get_file_attachment_fields(self, app_name: str, table_name: str) -> list[str]:
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
        return [label for label, meta in fmap.items() if meta.get("type") == "file"]

    def delete_file(
        self,
        app_name: str,
        table_name: str,
        record_id: int,
        field: str | int,
        version_number: int,
    ) -> dict[str, Any]:
        """Delete one explicit attachment version; version 0 selects the latest."""
        if version_number < 0:
            raise ValueError("version_number cannot be negative")
        app_id, table_id = self._ids(app_name, table_name)
        field_id = (
            field if isinstance(field, int) else self.meta.get_field_id(app_id, table_id, field)
        )
        return self._request(
            method="DELETE",
            path=f"files/{table_id}/{record_id}/{field_id}/{version_number}",
        )

    async def _async_download_attachment(
        self,
        transport: AsyncQuickBaseTransport,
        sem: asyncio.Semaphore,
        download_url: str,
        save_path: Path,
        *,
        record_id: Any,
        file_name: str,
    ) -> tuple[dict[str, Any], Exception | None]:
        """
        Download a file attachment asynchronously.

        Args:
            transport: The asynchronous Quickbase transport.
            sem (asyncio.Semaphore): Semaphore to limit concurrent downloads.
            download_url (str): The URL to download the file from.
            save_path (Path): The local path to save the downloaded file.
        """
        result = {
            "record_id": record_id,
            "file_name": file_name,
            "saved_path": str(save_path),
        }
        if save_path.exists():
            self.logger.warning(f"File already exists: {save_path.name}")
            return {**result, "status": "skipped"}, None

        async with sem:
            temp_path = save_path.with_name(f".{save_path.name}.{uuid4().hex}.part")
            try:
                payload = await transport.get_file(download_url)
                async with aiofiles.open(temp_path, "wb") as file_handle:
                    await file_handle.write(payload)
                await asyncio.to_thread(temp_path.replace, save_path)
                self.logger.info(f"Downloaded: {save_path.name}")
                return {**result, "status": "downloaded", "bytes_written": len(payload)}, None
            except Exception as error:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError as cleanup_error:
                    self.logger.warning(
                        f"Could not remove temporary attachment {temp_path}: {cleanup_error}"
                    )
                self.logger.error(f"Failed to download {download_url}: {error}")
                return {
                    **result,
                    "status": "failed",
                    "error": str(error),
                }, error

    async def _async_download_attachments(
        self, download_jobs: list[dict[str, Any]], target_dir: str, max_concurrency: int = 8
    ) -> list[dict[str, Any]]:
        """
        Download multiple file attachments asynchronously.

        Args:
            download_jobs (List[Dict[str, Any]]): List of download jobs with record IDs and URLs.
            target_dir (str): Directory to save downloaded files.
            max_concurrency (int): Maximum number of concurrent downloads.

        Returns:
            List[Dict[str, Any]]: List of results for each download job.
        """
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")

        sem = asyncio.Semaphore(max_concurrency)
        async with AsyncQuickBaseTransport(self.transport) as transport:
            tasks = []
            for job in download_jobs:
                rid = job["record_id"]
                filename = str(job["file_name"])
                safe_filename = sanitize_filenames(filename).strip() or "attachment.bin"
                url = job["url"]
                name_parts = [str(rid)]
                if job.get("include_field_id"):
                    name_parts.append(str(job["field_id"]))
                name_parts.append(safe_filename)
                save_path = Path(target_dir) / "_".join(name_parts)
                tasks.append(
                    self._async_download_attachment(
                        transport,
                        sem,
                        url,
                        save_path,
                        record_id=rid,
                        file_name=filename,
                    )
                )
            outcomes = await asyncio.gather(*tasks)

        results = [result for result, _ in outcomes]
        errors = [error for _, error in outcomes if error is not None]
        if errors:
            raise QuickbaseBatchError("Attachment download", results, errors)
        return results

    def download_attachments_async(
        self,
        app_name: str,
        table_name: str,
        file_field_label: str,
        target_dir: str,
        where: str | None = None,
        max_concurrency: int = 4,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Download all attachments from ONE file field in a table, honoring 'where',
        paging through all matching records, and skipping rows without an attachment.

        Args:
            app_name (str): The name of the app.
            table_name (str): The name of the table.
            file_field_label (str): The label of the file field.
            target_dir (str): The directory to save downloaded files.
            where (Optional[str]): Optional filter for the records.
            max_concurrency (int): Maximum number of concurrent downloads.
            page_size (int): Number of records to fetch per request.

        Returns:
            List[Dict[str, Any]]: List of results for each download job.
        """
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")

        app_id, table_id = self._ids(app_name, table_name)
        Path(target_dir).mkdir(parents=True, exist_ok=True)

        fmap = self.meta.get_field_map(app_id, table_id)
        file_fid = int(fmap[file_field_label]["id"])
        record_fid = 3  # Record ID#

        select_ids = [record_fid, file_fid]
        download_jobs: list[dict[str, Any]] = []

        seen = with_file = skipped_empty = 0

        for rows in iter_record_pages_by_id(
            self,
            table_id,
            select_fields=select_ids,
            where=where,
            page_size=page_size,
        ):
            for rec in rows:
                seen += 1
                rid = rec.get(str(record_fid), {}).get("value")
                if not rid:
                    continue
                cell = rec.get(str(file_fid), {}) or {}
                attachment = latest_attachment(
                    cell.get("value"),
                    table_id=table_id,
                    record_id=int(rid),
                    field_id=file_fid,
                )
                if attachment is None:
                    skipped_empty += 1
                    continue

                full_url = (
                    f"{self.transport.base_url}/files/{table_id}/{int(rid)}/"
                    f"{file_fid}/{attachment.version_number}"
                )
                download_jobs.append(
                    {"record_id": rid, "file_name": attachment.file_name, "url": full_url}
                )
                with_file += 1

        self.logger.info(
            f"Scanned {seen} records; queued {with_file} downloads (skipped {skipped_empty} with no attachment)."
        )

        if not download_jobs:
            self.logger.warning("No attachments found to download.")
            return []

        output = asyncio.run(
            self._async_download_attachments(
                download_jobs, target_dir, max_concurrency=max_concurrency
            )
        )
        downloaded = sum(result["status"] == "downloaded" for result in output)
        skipped = sum(result["status"] == "skipped" for result in output)
        self.logger.info(f"Downloaded {downloaded} attachments; skipped {skipped} existing files.")
        return output

    def download_attachment_base64(
        self, app_name: str, table_name: str, record_id: int, file_field_label: str
    ) -> str | None:
        """
        Return the attachment as BASE64 (string) from a single record/field.

        Args:
            app_name: The name of the application.
            table_name: The name of the table.
            record_id: The ID of the record to download the attachment from.
            file_field_label: The label of the file field to download.

        Returns:
            Optional[str]: The attachment as BASE64 string, or None if not found.
        """
        app_id, table_id = self._ids(app_name, table_name)
        fmap = self.meta.get_field_map(app_id, table_id)
        file_fid = int(fmap[file_field_label]["id"])
        record_fid = 3

        query_body = {
            "from": table_id,
            "select": [record_fid, file_fid],
            "where": f"{{3.EX.'{record_id}'}}",
        }
        resp = self._request(
            method="POST",
            path="records/query",
            json_body=query_body,
            retry_policy=RetryPolicy.SAFE,
        )
        records = resp.get("data", [])
        if not records:
            self.logger.warning(f"No attachment found for record {record_id}.")
            return None

        attachment = latest_attachment(
            records[0].get(str(file_fid), {}).get("value"),
            table_id=table_id,
            record_id=record_id,
            field_id=file_fid,
        )
        if attachment is None:
            self.logger.warning(f"No attachment found for record {record_id}.")
            return None

        path = f"files/{table_id}/{int(record_id)}/{file_fid}/{attachment.version_number}"
        return base64.b64encode(self.transport.get_file(path)).decode("ascii")

    def download_table_attachments_async(
        self,
        app_name: str,
        table_name: str,
        target_dir: str,
        where: str | None = None,
        max_concurrency: int = 4,
        page_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """
        Download attachments from ALL file fields in the table, honoring 'where'.
        Pages through records and skips cells without an attachment.

        Args:
            app_name: The name of the application.
            table_name: The name of the table.
            target_dir: The directory to save downloaded attachments.
            where: Optional filter for the records to download attachments from.
            max_concurrency: Maximum number of concurrent downloads.
            page_size: Number of records to process per page.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries containing information about the downloaded attachments.
        """
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if not 1 <= page_size <= 1000:
            raise ValueError("page_size must be between 1 and 1000")

        app_id, table_id = self._ids(app_name, table_name)
        Path(target_dir).mkdir(parents=True, exist_ok=True)

        fmap = self.meta.get_field_map(app_id, table_id)
        file_fields = [
            (lbl, int(meta["id"])) for lbl, meta in fmap.items() if meta.get("type") == "file"
        ]
        if not file_fields:
            self.logger.info("No file attachment fields on this table.")
            return []

        select_ids = [3] + [fid for _, fid in file_fields]  # 3 = Record ID#
        download_jobs: list[dict[str, Any]] = []

        seen_cells = with_file = skipped_empty = 0

        for rows in iter_record_pages_by_id(
            self,
            table_id,
            select_fields=select_ids,
            where=where,
            page_size=page_size,
        ):
            for rec in rows:
                rid = rec.get("3", {}).get("value")
                if not rid:
                    continue

                for _, fid in file_fields:
                    seen_cells += 1
                    cell = rec.get(str(fid), {}) or {}
                    attachment = latest_attachment(
                        cell.get("value"),
                        table_id=table_id,
                        record_id=int(rid),
                        field_id=fid,
                    )
                    if attachment is None:
                        skipped_empty += 1
                        continue

                    url = (
                        f"{self.transport.base_url}/files/{table_id}/{int(rid)}/{fid}/"
                        f"{attachment.version_number}"
                    )
                    download_jobs.append(
                        {
                            "record_id": rid,
                            "field_id": fid,
                            "include_field_id": True,
                            "file_name": attachment.file_name,
                            "url": url,
                        }
                    )
                    with_file += 1

        self.logger.info(
            f"Scanned {seen_cells} file cells; queued {with_file} downloads (skipped {skipped_empty} empty)."
        )

        if not download_jobs:
            self.logger.warning("No attachments found to download.")
            return []

        return asyncio.run(
            self._async_download_attachments(
                download_jobs, target_dir, max_concurrency=max_concurrency
            )
        )

    # ----------------
    # Utility
    # ----------------
    def get_field_id(self, app_id: str, table_id: str, field_label: str) -> int:
        return self._fields.get_id(app_id, table_id, field_label)

    def get_table_id(self, app_id: str, table_id: str) -> str:
        return self._tables.get_id(app_id, table_id)

    def get_field(self, app_id, table_id, field_id):
        return self._fields.get(app_id, table_id, field_id)

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
                tid = table_data.get("id")
                size = table_data.get("size", "N/A")
                self.logger.info(f"  └── 🔹 {table_name}  [ID: {tid}, Size: {size} records]")
                if show_fields:
                    for field_label, meta in table_data.get("fields", {}).items():
                        fid = meta.get("id")
                        ftype = meta.get("type")
                        self.logger.info(f"      └── 🔹 {field_label} (ID: {fid}, Type: {ftype})")

    def dump_full_config(self):
        """
        Dumps the full in-memory config dictionary to the logger in JSON format.
        Useful for debugging large applications where fields/tables may be misconfigured.
        """
        try:
            config_str = json.dumps(self.meta.cache, indent=2)
            self.logger.info("Full QuickBase Config Dump:" + config_str)
        except Exception as e:
            self.logger.error(f"Failed to serialize config: {e}")
