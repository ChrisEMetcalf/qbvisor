import importlib.util
from copy import deepcopy
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "audit_quickbase_oas.py"
SPEC = importlib.util.spec_from_file_location("audit_quickbase_oas", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
AUDIT = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(AUDIT)

_build_manifest = AUDIT._build_manifest
_manifests_equivalent = AUDIT._manifests_equivalent


def sample_document() -> dict:
    return {
        "swagger": "2.0",
        "info": {"version": "1.0.0"},
        "paths": {
            "/apps/{appId}": {
                "get": {
                    "operationId": "getApp",
                    "summary": "Get an app",
                    "tags": ["Apps"],
                    "responses": {
                        "200": {
                            "description": "Success",
                            "x-amf-mediaType": "application/json",
                            "schema": {"type": "object"},
                        }
                    },
                }
            },
            "/apps/{appId}/roles": {
                "get": {
                    "operationId": "getRoles",
                    "summary": "Get app roles",
                    "tags": ["Apps"],
                    "responses": {
                        "200": {
                            "description": "Success",
                            "x-amf-mediaType": "application/json",
                            "schema": {"type": "array", "items": {"type": "object"}},
                        }
                    },
                }
            },
        },
    }


def build_manifest(*, retrieved_at: str = "2026-07-18T00:00:00+00:00") -> dict:
    return _build_manifest(
        sample_document(),
        source_sha256="abc123",
        source_headers={"etag": "first", "last-modified": "yesterday"},
        retrieved_at=retrieved_at,
        supported_operations=(("GET", "/apps/{appId}"),),
    )


def test_manifest_records_supported_and_missing_operations_by_resource():
    manifest = build_manifest()

    assert manifest["coverage"] == {
        "totalOperations": 2,
        "supportedOperations": 1,
        "missingOperations": 1,
        "byTag": [{"tag": "Apps", "total": 2, "supported": 1, "missing": 1}],
    }
    assert manifest["availableOperations"] == [
        {
            "method": "GET",
            "path": "/apps/{appId}",
            "operationId": "getApp",
            "tag": "Apps",
            "summary": "Get an app",
            "supported": True,
            "successShapes": ["object"],
        },
        {
            "method": "GET",
            "path": "/apps/{appId}/roles",
            "operationId": "getRoles",
            "tag": "Apps",
            "summary": "Get app roles",
            "supported": False,
            "successShapes": ["array"],
        },
    ]


def test_manifest_comparison_ignores_retrieval_metadata_only():
    tracked = build_manifest()
    refreshed = build_manifest(retrieved_at="2026-07-19T00:00:00+00:00")
    refreshed["source"]["etag"] = "second"
    refreshed["source"]["lastModified"] = "today"

    assert _manifests_equivalent(tracked, refreshed)

    changed = deepcopy(refreshed)
    changed["availableOperations"][0]["summary"] = "Changed upstream"
    assert not _manifests_equivalent(tracked, changed)
