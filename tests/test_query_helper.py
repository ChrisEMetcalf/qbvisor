import pytest
from pyqb_client.query_helper import QueryHelper

class DummyMeta:
    def get_field_map(self, a, t):
        return {
            "F1": {"id": 1},
            "F2": {"id": 2},
        }

class DummyClient:
    def __init__(self):
        self.meta = DummyMeta()

def test_eq_and_expr():
    q = QueryHelper(DummyClient(), "App", "Table")
    expr = q.eq("F1", "X")
    assert expr == "{1.EX.'X'}"

def test_and_or_not():
    q = QueryHelper(DummyClient(), "App", "Table")
    a = q.eq("F1", "X")
    b = q.eq("F2", "Y")
    assert q.and_(a, b) == "AND".join([a, b])
    assert q.or_(a, b) == "OR".join([a, b])
    assert q.not_(a) == "NOT " + a

@pytest.mark.parametrize("op,method", [
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
])
def test_supported_operators(op, method):
    q = QueryHelper(DummyClient(), "A", "T")
    func = getattr(q, method)
    s = func("F1", 123)
    assert s.startswith("{1."+op)