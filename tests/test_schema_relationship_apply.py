from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

from qbvisor import (
    AppSpec,
    FieldSpec,
    QuickBaseClient,
    RelationshipSpec,
    SummaryFieldSpec,
    TableSpec,
)

APP_ID = "app_operations"
PROJECTS_ID = "tbl_projects"
DETAILS_ID = "tbl_details"


def relationship_spec() -> AppSpec:
    return AppSpec(
        key="operations",
        name="Operations",
        tables=[
            TableSpec(
                key="projects",
                name="Projects",
                fields=[
                    FieldSpec(key="name", label="Project Name", field_type="text"),
                    FieldSpec(key="budget", label="Budget", field_type="numeric"),
                ],
            ),
            TableSpec(
                key="details",
                name="Details",
                fields=[
                    FieldSpec(key="hours", label="Hours", field_type="numeric"),
                    FieldSpec(key="cost", label="Cost", field_type="numeric"),
                ],
            ),
        ],
        relationships=[
            RelationshipSpec(
                key="project_details",
                parent_table="projects",
                child_table="details",
                foreign_key_label="Related Project",
                lookup_fields=["name", "budget"],
                summary_fields=[
                    SummaryFieldSpec(
                        key="total_hours",
                        accumulation_type="SUM",
                        field="hours",
                        label="Total Hours",
                        where="{8.GT.0}",
                    ),
                    SummaryFieldSpec(
                        key="average_cost",
                        accumulation_type="AVG",
                        field="cost",
                        label="Average Cost",
                    ),
                ],
            )
        ],
    )


class RelationshipQuickbase:
    def __init__(self):
        self.app = {"id": APP_ID, "name": "Operations"}
        self.tables = [
            {"id": PROJECTS_ID, "name": "Projects"},
            {"id": DETAILS_ID, "name": "Details"},
        ]
        self.fields = {
            PROJECTS_ID: [
                {"id": 6, "label": "Project Name", "fieldType": "text", "properties": {}},
                {"id": 7, "label": "Budget", "fieldType": "numeric", "properties": {}},
            ],
            DETAILS_ID: [
                {"id": 8, "label": "Hours", "fieldType": "numeric", "properties": {}},
                {"id": 12, "label": "Cost", "fieldType": "numeric", "properties": {}},
            ],
        }
        self.relationships: list[dict[str, object]] = []
        self.calls: list[dict[str, object]] = []

    def request(
        self,
        *,
        method,
        path,
        params=None,
        json_body=None,
        response_type=dict,
        **_,
    ):
        self.calls.append(
            {"method": method, "path": path, "params": params, "json_body": json_body}
        )
        if method == "GET":
            result = self._get(path, params)
        elif method == "POST":
            result = self._post(path, params, json_body)
        else:
            raise AssertionError(f"Unexpected request {method} {path}")
        result = deepcopy(result)
        assert isinstance(result, response_type)
        return result

    def _get(self, path, params):
        if path == f"apps/{APP_ID}":
            return self.app
        if path == "tables":
            return self.tables
        if path == "fields":
            return self.fields[params["tableId"]]
        if path == f"tables/{DETAILS_ID}/relationships":
            return {"relationships": self.relationships}
        raise AssertionError(f"Unexpected GET {path}")

    def _post(self, path, params, body):
        if path == f"tables/{DETAILS_ID}/relationship":
            return self._create_relationship(body)
        if path == f"tables/{DETAILS_ID}/relationship/9":
            relationship = self.relationships[0]
            for source_id in body.get("lookupFieldIds", []):
                field_id = {6: 11, 7: 13}[source_id]
                field = {
                    "id": field_id,
                    "label": f"Lookup {source_id}",
                    "fieldType": "text" if source_id == 6 else "numeric",
                    "properties": {"lookupTargetFieldId": source_id},
                }
                self.fields[DETAILS_ID].append(field)
                relationship["lookupFields"].insert(0, {"id": field_id, "label": field["label"]})
            for definition in body.get("summaryFields", []):
                target_id = definition["summaryFid"]
                field_id = {8: 10, 12: 14}[target_id]
                field = {
                    "id": field_id,
                    "label": definition["label"],
                    "fieldType": "numeric",
                    "properties": {
                        "summaryFunction": definition["accumulationType"],
                        "summaryTargetFieldId": target_id,
                    },
                }
                self.fields[PROJECTS_ID].append(field)
                relationship["summaryFields"].insert(0, {"id": field_id, "label": field["label"]})
            return relationship
        if path in {"fields/9", "fields/10", "fields/14"}:
            field_id = int(path.split("/")[1])
            fields = self.fields[params["tableId"]]
            field = next(item for item in fields if item["id"] == field_id)
            field["label"] = body["label"]
            relationship = self.relationships[0]
            if field_id == 9:
                relationship["foreignKeyField"]["label"] = body["label"]
            else:
                summary = next(
                    item for item in relationship["summaryFields"] if item["id"] == field_id
                )
                summary["label"] = body["label"]
            return field
        raise AssertionError(f"Unexpected POST {path}")

    def _create_relationship(self, body):
        foreign = {
            "id": 9,
            "label": body["foreignKeyField"]["label"],
            "fieldType": "numeric",
            "properties": {"foreignKey": True},
        }
        self.fields[DETAILS_ID].append(foreign)
        lookup_ids = {6: 11, 7: 13}
        lookup_fields = []
        for source_id in body.get("lookupFieldIds", []):
            field_id = lookup_ids[source_id]
            field = {
                "id": field_id,
                "label": f"Lookup {source_id}",
                "fieldType": "text" if source_id == 6 else "numeric",
                "properties": {"lookupTargetFieldId": source_id},
            }
            self.fields[DETAILS_ID].append(field)
            lookup_fields.append({"id": field_id, "label": field["label"]})

        summary_ids = {8: 10, 12: 14}
        summary_fields = []
        for definition in body.get("summaryFields", []):
            target_id = definition["summaryFid"]
            field_id = summary_ids[target_id]
            field = {
                "id": field_id,
                "label": definition["label"],
                "fieldType": "numeric",
                "properties": {
                    "summaryFunction": definition["accumulationType"],
                    "summaryTargetFieldId": target_id,
                },
            }
            self.fields[PROJECTS_ID].append(field)
            summary_fields.append({"id": field_id, "label": field["label"]})

        relationship = {
            "id": 9,
            "parentTableId": PROJECTS_ID,
            "childTableId": DETAILS_ID,
            "isCrossApp": False,
            "foreignKeyField": {"id": 9, "label": foreign["label"]},
            "lookupFields": list(reversed(lookup_fields)),
            "summaryFields": list(reversed(summary_fields)),
        }
        self.relationships.append(relationship)
        return relationship


def client_for(api: RelationshipQuickbase) -> QuickBaseClient:
    client = QuickBaseClient.__new__(QuickBaseClient)
    client.meta = SimpleNamespace(
        app_ids={"Operations": APP_ID},
        invalidate_tables=Mock(),
    )
    client._request = Mock(side_effect=api.request)
    return client


def mutation_calls(api: RelationshipQuickbase) -> list[dict[str, object]]:
    return [call for call in api.calls if call["method"] == "POST"]


def test_apply_creates_relationship_and_binds_reversed_generated_field_responses(tmp_path):
    api = RelationshipQuickbase()
    client = client_for(api)
    state_path = tmp_path / "state.json"
    spec = relationship_spec()
    plan = client.plan_app(spec, state_path=state_path)

    result = client.apply_app(plan)

    posts = mutation_calls(api)
    assert [(post["path"], post["json_body"]) for post in posts] == [
        (
            f"tables/{DETAILS_ID}/relationship",
            {
                "parentTableId": PROJECTS_ID,
                "foreignKeyField": {"label": "Related Project"},
            },
        ),
        (f"tables/{DETAILS_ID}/relationship/9", {"lookupFieldIds": [7]}),
        (f"tables/{DETAILS_ID}/relationship/9", {"lookupFieldIds": [6]}),
        (
            f"tables/{DETAILS_ID}/relationship/9",
            {
                "summaryFields": [
                    {
                        "accumulationType": "AVG",
                        "summaryFid": 12,
                        "label": "Average Cost",
                    }
                ]
            },
        ),
        (
            f"tables/{DETAILS_ID}/relationship/9",
            {
                "summaryFields": [
                    {
                        "accumulationType": "SUM",
                        "summaryFid": 8,
                        "label": "Total Hours",
                        "where": "{8.GT.0}",
                    }
                ]
            },
        ),
    ]
    resources = {resource.address: resource for resource in result.state.resources}
    assert resources["apps.operations.relationships.project_details.lookups.name"].remote_id == 11
    assert resources["apps.operations.relationships.project_details.lookups.budget"].remote_id == 13
    assert (
        resources["apps.operations.relationships.project_details.summaries.total_hours"].remote_id
        == 10
    )
    assert (
        resources["apps.operations.relationships.project_details.summaries.average_cost"].remote_id
        == 14
    )
    assert (
        resources["apps.operations.relationships.project_details.summaries.total_hours"].attributes[
            "where"
        ]
        == "{8.GT.0}"
    )
    assert result.verification.quickbase_change_count == 0
    assert result.verification.state_change_count == 0


def test_apply_renames_foreign_key_and_summary_fields_by_stable_ids(tmp_path):
    api = RelationshipQuickbase()
    client = client_for(api)
    state_path = tmp_path / "state.json"
    spec = relationship_spec()
    client.apply_app(client.plan_app(spec, state_path=state_path))
    api.calls.clear()
    api.relationships[0]["foreignKeyField"]["label"] = "Wrong Foreign Key"
    next(item for item in api.fields[DETAILS_ID] if item["id"] == 9)["label"] = "Wrong Foreign Key"
    next(item for item in api.relationships[0]["summaryFields"] if item["id"] == 10)["label"] = (
        "Wrong Summary"
    )
    next(item for item in api.fields[PROJECTS_ID] if item["id"] == 10)["label"] = "Wrong Summary"

    plan = client.plan_app(spec, state_path=state_path)
    result = client.apply_app(plan)

    assert [(call["path"], call["json_body"]) for call in mutation_calls(api)] == [
        ("fields/9", {"label": "Related Project"}),
        ("fields/10", {"label": "Total Hours"}),
    ]
    assert result.verification.quickbase_change_count == 0
    assert result.state.serial == 2


def test_apply_adds_missing_generated_fields_to_an_existing_relationship(tmp_path):
    api = RelationshipQuickbase()
    client = client_for(api)
    state_path = tmp_path / "state.json"
    full_spec = relationship_spec()
    relationship = full_spec.relationships[0]
    base_spec = AppSpec(
        key=full_spec.key,
        name=full_spec.name,
        tables=full_spec.tables,
        relationships=[
            RelationshipSpec(
                key=relationship.key,
                parent_table=relationship.parent_table,
                child_table=relationship.child_table,
                foreign_key_label=relationship.foreign_key_label,
            )
        ],
    )
    client.apply_app(client.plan_app(base_spec, state_path=state_path))
    api.calls.clear()

    result = client.apply_app(client.plan_app(full_spec, state_path=state_path))

    posts = mutation_calls(api)
    assert [(post["path"], post["json_body"]) for post in posts] == [
        (f"tables/{DETAILS_ID}/relationship/9", {"lookupFieldIds": [7]}),
        (f"tables/{DETAILS_ID}/relationship/9", {"lookupFieldIds": [6]}),
        (
            f"tables/{DETAILS_ID}/relationship/9",
            {
                "summaryFields": [
                    {
                        "accumulationType": "AVG",
                        "summaryFid": 12,
                        "label": "Average Cost",
                    }
                ]
            },
        ),
        (
            f"tables/{DETAILS_ID}/relationship/9",
            {
                "summaryFields": [
                    {
                        "accumulationType": "SUM",
                        "summaryFid": 8,
                        "label": "Total Hours",
                        "where": "{8.GT.0}",
                    }
                ]
            },
        ),
    ]
    assert result.state.serial == 2
    assert result.verification.quickbase_change_count == 0
