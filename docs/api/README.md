# Quickbase API contract

qbvisor audits its operation coverage against Quickbase's official [OpenAPI document](https://developer.quickbase.com/quickbase.json). The tracked [response manifest](quickbase-oas-manifest.json) records the source URL, retrieval time, SHA-256 checksum, all documented operations, and every documented response for the operations the client currently calls. The coverage summary and per-operation `supported` flag make missing endpoints explicit.

The current client covers 33 of the 67 operations in the source document. Apps, fields, files, formulas, records, reports, and tables have complete coverage. Operations for administration, identity, analytics, audit, document templates, and Solutions remain visible in the ledger rather than being implied as supported.

The raw specification is stored at `.cache/quickbase/quickbase.json` and is intentionally excluded from version control. Refresh and audit it with:

```bash
uv run python scripts/audit_quickbase_oas.py --refresh --write
```

## Response rules

- The transport returns any valid JSON value without wrapping it.
- High-level methods enforce the documented top-level response shape. Apps, individual resources, relationships, record operations, formulas, and report runs return objects. Table, report, field, app-event, app-role, and field-usage collections return arrays.
- Empty successful responses preserve the existing `{}` compatibility behavior.
- A `207` upsert response remains a successful object. Callers can inspect `metadata.lineErrors` alongside successfully processed records.
- Quickbase error objects preserve `message` and `description`. Exceptions also preserve the HTTP status, `Retry-After`, and `qb-api-ray` header when present.

## File responses

The OpenAPI document describes `GET /files/{tableId}/{recordId}/{fieldId}/{versionNumber}` as an
`application/octet-stream` response. The persistent sandbox currently returns `text/plain` with a
base64-encoded body instead. qbvisor uses the response media type to support both contracts:

- `text/plain` must contain valid base64 and is decoded to file bytes;
- `application/octet-stream` is preserved byte-for-byte;
- malformed base64 text raises `QuickbaseResponseError` with `qb-api-ray` when Quickbase provides
  it.

This distinction avoids heuristically decoding raw binary data that only happens to resemble
base64. Both synchronous and concurrent attachment paths apply the same interpretation.

## Rate limiting

Quickbase's [rate-limit guidance](https://developer.quickbase.com/rateLimit) instructs clients to wait for `Retry-After` before retrying. qbvisor applies a valid delay to both reads and mutations because the server has explicitly told the client when to replay the request. If the header is missing or invalid, qbvisor raises `QuickbaseRateLimitError` immediately. It does not invent a mutation retry delay.

Connection failures, timeouts, and temporary gateway failures remain different: only operations classified as safe are replayed after those uncertain failures.
