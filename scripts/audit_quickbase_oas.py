#!/usr/bin/env python3
"""Fetch and audit the official Quickbase OpenAPI specification.

The raw specification is intentionally cached outside version control. The derived
manifest is small, reviewable, and records the exact upstream document used.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OAS_URL = "https://developer.quickbase.com/quickbase.json"
CACHE_PATH = PROJECT_ROOT / ".cache" / "quickbase" / "quickbase.json"
CACHE_METADATA_PATH = PROJECT_ROOT / ".cache" / "quickbase" / "source-metadata.json"
MANIFEST_PATH = PROJECT_ROOT / "docs" / "api" / "quickbase-oas-manifest.json"

# These operations are called by the current public client or metadata cache. File
# downloads are listed even though their response is not handled by the JSON transport.
SUPPORTED_OPERATIONS = (
    ("POST", "/apps"),
    ("GET", "/apps/{appId}"),
    ("POST", "/apps/{appId}"),
    ("DELETE", "/apps/{appId}"),
    ("POST", "/apps/{appId}/copy"),
    ("GET", "/tables"),
    ("POST", "/tables"),
    ("GET", "/tables/{tableId}"),
    ("POST", "/tables/{tableId}"),
    ("DELETE", "/tables/{tableId}"),
    ("GET", "/tables/{tableId}/relationships"),
    ("POST", "/tables/{tableId}/relationship"),
    ("DELETE", "/tables/{tableId}/relationship/{relationshipId}"),
    ("GET", "/reports"),
    ("GET", "/reports/{reportId}"),
    ("POST", "/reports/{reportId}/run"),
    ("GET", "/fields"),
    ("POST", "/fields"),
    ("DELETE", "/fields"),
    ("GET", "/fields/{fieldId}"),
    ("POST", "/fields/{fieldId}"),
    ("POST", "/formula/run"),
    ("POST", "/records"),
    ("DELETE", "/records"),
    ("POST", "/records/query"),
    ("GET", "/files/{tableId}/{recordId}/{fieldId}/{versionNumber}"),
)

EXPECTED_SUCCESS_SHAPES = {
    ("GET", "/tables"): "array",
    ("GET", "/reports"): "array",
    ("GET", "/fields"): "array",
    ("GET", "/files/{tableId}/{recordId}/{fieldId}/{versionNumber}"): "binary",
}
DEFAULT_SUCCESS_SHAPE = "object"
HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete"})


def _load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as source:
        payload = json.load(source)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return payload


def _resolve_ref(document: dict[str, Any], value: Any) -> Any:
    seen: set[str] = set()
    while isinstance(value, dict) and isinstance(value.get("$ref"), str):
        ref = value["$ref"]
        if not ref.startswith("#/") or ref in seen:
            break
        seen.add(ref)
        resolved: Any = document
        for part in ref[2:].split("/"):
            resolved = resolved[part.replace("~1", "/").replace("~0", "~")]
        value = resolved
    return value


def _schema_shape(document: dict[str, Any], schema: Any) -> str:
    schema = _resolve_ref(document, schema)
    if not isinstance(schema, dict):
        return "unspecified"
    schema_type = schema.get("type")
    if isinstance(schema_type, str):
        return schema_type
    for composition in ("allOf", "oneOf", "anyOf"):
        members = schema.get(composition)
        if isinstance(members, list):
            shapes = {_schema_shape(document, member) for member in members}
            shapes.discard("unspecified")
            if len(shapes) == 1:
                return shapes.pop()
            if shapes:
                return "|".join(sorted(shapes))
    if "properties" in schema:
        return "object"
    return "unspecified"


def _response_entry(document: dict[str, Any], response: Any) -> dict[str, Any]:
    response = _resolve_ref(document, response)
    if not isinstance(response, dict):
        return {"description": "", "shape": "unspecified"}
    schema = response.get("schema")
    media_type = response.get("x-amf-mediaType")
    shape = (
        "binary"
        if media_type == "application/octet-stream"
        else "empty"
        if schema is None
        else _schema_shape(document, schema)
    )
    entry: dict[str, Any] = {
        "description": response.get("description", ""),
        "shape": shape,
    }
    if isinstance(media_type, str):
        entry["mediaType"] = media_type
    if isinstance(schema, dict) and isinstance(schema.get("$ref"), str):
        entry["schemaRef"] = schema["$ref"]
    return entry


def _build_manifest(
    document: dict[str, Any],
    *,
    source_sha256: str,
    source_headers: dict[str, str],
    retrieved_at: str,
    supported_operations: tuple[tuple[str, str], ...] = SUPPORTED_OPERATIONS,
) -> dict[str, Any]:
    paths = document.get("paths")
    if not isinstance(paths, dict):
        raise ValueError("Quickbase specification does not contain a paths object")

    supported_set = set(supported_operations)
    operations: list[dict[str, Any]] = []
    for method, path in supported_operations:
        path_item = paths.get(path)
        operation = path_item.get(method.lower()) if isinstance(path_item, dict) else None
        if not isinstance(operation, dict):
            operations.append({"method": method, "path": path, "missing": True})
            continue
        responses = operation.get("responses", {})
        response_manifest = {
            str(code): _response_entry(document, response) for code, response in responses.items()
        }
        operations.append(
            {
                "method": method,
                "path": path,
                "operationId": operation.get("operationId"),
                "responses": response_manifest,
            }
        )

    available_operations: list[dict[str, Any]] = []
    tag_counts: dict[str, dict[str, int]] = {}
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in HTTP_METHODS or not isinstance(operation, dict):
                continue
            normalized_method = method.upper()
            is_supported = (normalized_method, path) in supported_set
            tags = operation.get("tags", [])
            tag = str(tags[0]) if isinstance(tags, list) and tags else "Untagged"
            responses = operation.get("responses", {})
            success_shapes = sorted(
                {
                    _response_entry(document, response).get("shape", "unspecified")
                    for code, response in responses.items()
                    if str(code).isdigit() and 200 <= int(code) < 300
                }
            )
            available_operations.append(
                {
                    "method": normalized_method,
                    "path": path,
                    "operationId": operation.get("operationId"),
                    "tag": tag,
                    "summary": operation.get("summary", ""),
                    "supported": is_supported,
                    "successShapes": success_shapes,
                }
            )
            counts = tag_counts.setdefault(tag, {"total": 0, "supported": 0, "missing": 0})
            counts["total"] += 1
            counts["supported" if is_supported else "missing"] += 1

    available_operations.sort(key=lambda item: (item["tag"], item["path"], item["method"]))
    supported_count = sum(operation["supported"] for operation in available_operations)

    info = document.get("info", {})
    return {
        "source": {
            "url": OAS_URL,
            "retrievedAt": retrieved_at,
            "sha256": source_sha256,
            "etag": source_headers.get("etag"),
            "lastModified": source_headers.get("last-modified"),
            "oasVersion": document.get("swagger") or document.get("openapi"),
            "apiVersion": info.get("version") if isinstance(info, dict) else None,
        },
        "coverage": {
            "totalOperations": len(available_operations),
            "supportedOperations": supported_count,
            "missingOperations": len(available_operations) - supported_count,
            "byTag": [{"tag": tag, **counts} for tag, counts in sorted(tag_counts.items())],
        },
        "availableOperations": available_operations,
        "supportedOperations": operations,
    }


def _manifests_equivalent(tracked: dict[str, Any], current: dict[str, Any]) -> bool:
    """Compare API content while ignoring retrieval-only source metadata."""
    tracked_copy = json.loads(json.dumps(tracked))
    current_copy = json.loads(json.dumps(current))
    for manifest in (tracked_copy, current_copy):
        source = manifest.get("source")
        if isinstance(source, dict):
            source.pop("retrievedAt", None)
            source.pop("etag", None)
            source.pop("lastModified", None)
    return tracked_copy == current_copy


def _audit(manifest: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    operations = manifest.get("supportedOperations", [])
    for operation in operations:
        method = operation["method"]
        path = operation["path"]
        label = f"{method} {path}"
        if operation.get("missing"):
            issues.append(f"{label} is missing from the official specification")
            continue
        expected = EXPECTED_SUCCESS_SHAPES.get((method, path), DEFAULT_SUCCESS_SHAPE)
        responses = operation.get("responses", {})
        successes = [
            response
            for code, response in responses.items()
            if code.isdigit() and 200 <= int(code) < 300
        ]
        if not successes:
            issues.append(f"{label} has no documented success response")
            continue
        actual = {response.get("shape") for response in successes}
        if actual != {expected}:
            issues.append(
                f"{label} success shape changed: expected {expected}, found {sorted(actual)}"
            )
    return issues


def _fetch() -> tuple[bytes, dict[str, str]]:
    request = Request(OAS_URL, headers={"User-Agent": "qbvisor-oas-audit/1"})
    with urlopen(request, timeout=60) as response:  # noqa: S310
        return response.read(), {key.lower(): value for key, value in response.headers.items()}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="download the current official OAS")
    parser.add_argument("--write", action="store_true", help="update the tracked derived manifest")
    args = parser.parse_args()

    source_headers: dict[str, str] = {}
    retrieved_at: str
    if args.refresh:
        raw, source_headers = _fetch()
        retrieved_at = datetime.now(UTC).isoformat(timespec="seconds")
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_bytes(raw)
        CACHE_METADATA_PATH.write_text(
            json.dumps({"retrievedAt": retrieved_at, "headers": source_headers}, indent=2) + "\n",
            encoding="utf-8",
        )
    elif not CACHE_PATH.exists():
        parser.error(f"cached OAS not found; run with --refresh ({CACHE_PATH})")
    elif CACHE_METADATA_PATH.exists():
        cache_metadata = _load_json(CACHE_METADATA_PATH)
        retrieved_at = str(cache_metadata["retrievedAt"])
        raw_headers = cache_metadata.get("headers", {})
        if isinstance(raw_headers, dict):
            source_headers = {str(key): str(value) for key, value in raw_headers.items()}
    else:
        retrieved_at = datetime.fromtimestamp(CACHE_PATH.stat().st_mtime, tz=UTC).isoformat(
            timespec="seconds"
        )

    raw = CACHE_PATH.read_bytes()
    document = _load_json(CACHE_PATH)
    manifest = _build_manifest(
        document,
        source_sha256=hashlib.sha256(raw).hexdigest(),
        source_headers=source_headers,
        retrieved_at=retrieved_at,
    )
    issues = _audit(manifest)
    if not args.write:
        if not MANIFEST_PATH.exists():
            issues.append(f"tracked manifest is missing: {MANIFEST_PATH}")
        elif not _manifests_equivalent(_load_json(MANIFEST_PATH), manifest):
            issues.append("tracked manifest is stale; rerun with --write")
    if issues:
        for issue in issues:
            print(f"ERROR: {issue}", file=sys.stderr)
        return 1

    if args.write:
        MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST_PATH.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")

    print(
        f"Audited {len(SUPPORTED_OPERATIONS)} operations against "
        f"sha256:{manifest['source']['sha256'][:12]}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
