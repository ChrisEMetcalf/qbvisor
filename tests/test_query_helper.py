import json

import pytest

from qbvisor.metadata import QuickBaseMetaCache
from qbvisor.query_helper import QueryHelper


class DummyMeta:
    def __init__(self, field_map=None):
        self.field_map = field_map or {
            "F1": {"id": 1},
            "F2": {"id": 2},
        }

    def get_field_map(self, a, t):
        return self.field_map


class DummyClient:
    def __init__(self, meta=None):
        self.meta = meta or DummyMeta()


class FakeTransport:
    def __init__(self):
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, params))
        if path == "tables":
            return [{"id": "T1", "name": "Table", "size": 0}]
        if path == "tables/T1":
            return {"nextRecordId": 1}
        if path == "fields":
            return [{"id": 7, "label": "Invoice Status", "fieldType": "text"}]
        raise ValueError(path)


def test_eq_and_expr():
    q = QueryHelper(DummyClient(), "App", "Table")
    expr = q.eq("F1", "X")
    assert expr == "{1.EX.'X'}"


def test_fid_resolves_exact_and_mixed_case_labels():
    q = QueryHelper(
        DummyClient(
            DummyMeta(
                {
                    "Invoice Status": {"id": 7},
                    "Straße": {"id": 8},
                }
            )
        ),
        "App",
        "Table",
    )

    assert q.fid("Invoice Status") == "7"
    assert q.fid("iNVOICE sTATUS") == "7"
    assert q.fid("STRASSE") == "8"


def test_fid_rejects_unknown_label_with_table_context():
    q = QueryHelper(DummyClient(), "App", "Invoices")

    with pytest.raises(
        ValueError,
        match=r"Field label 'Missing' not found in table 'Invoices'\.",
    ):
        q.fid("Missing")


def test_fid_prefers_exact_case_and_rejects_ambiguous_case_insensitive_match():
    q = QueryHelper(
        DummyClient(
            DummyMeta(
                {
                    "Status": {"id": 7},
                    "STATUS": {"id": 8},
                }
            )
        ),
        "App",
        "Table",
    )

    assert q.fid("Status") == "7"
    assert q.fid("STATUS") == "8"
    with pytest.raises(
        ValueError,
        match=(
            r"Field label 'status' is ambiguous in table 'Table'\. "
            r"Matches: \['STATUS', 'Status'\]\. Use the exact field label\."
        ),
    ):
        q.fid("status")


def test_cached_and_uncached_metadata_resolve_mixed_case_without_refetch(monkeypatch):
    monkeypatch.setenv("QB_APP_IDS", json.dumps({"App": "app-id"}))
    transport = FakeTransport()
    client = DummyClient(QuickBaseMetaCache(transport))

    uncached = QueryHelper(client, "App", "Table")
    assert uncached.fid("iNVOICE sTATUS") == "7"
    assert transport.calls == [
        ("tables", {"appId": "app-id"}),
        ("tables/T1", {"appId": "app-id"}),
        ("fields", {"tableId": "T1", "includeFieldPerms": "true"}),
    ]

    cached = QueryHelper(client, "app-id", "T1")
    assert cached.fid("INVOICE STATUS") == "7"
    assert len(transport.calls) == 3


def test_and_or_not():
    q = QueryHelper(DummyClient(), "App", "Table")
    a = q.eq("F1", "X")
    b = q.eq("F2", "Y")
    assert q.and_(a, b) == "AND".join([a, b])
    assert q.or_(a, b) == "OR".join([a, b])
    assert q.not_(a) == "NOT " + a


@pytest.mark.parametrize(
    "op,method",
    [
        ("EX", "eq"),
        ("XEX", "neq"),
        ("CT", "contains"),
        ("XCT", "not_contains"),
        ("HAS", "has"),
        ("XHAS", "not_has"),
        ("SW", "starts_with"),
        ("XSW", "not_starts_with"),
        ("LT", "less_than"),
        ("LTE", "less_than_or_equal"),
        ("GT", "greater_than"),
        ("GTE", "greater_than_or_equal"),
        ("BF", "before"),
        ("OBF", "on_or_before"),
        ("AF", "after"),
        ("OAF", "on_or_after"),
    ],
)
def test_supported_operators(op, method):
    q = QueryHelper(DummyClient(), "A", "T")
    func = getattr(q, method)
    s = func("F1", 123)
    assert s.startswith("{1." + op)
