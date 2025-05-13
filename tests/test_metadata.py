import json
import pytest
from pyqb_client.metadata import QuickBaseMetaCache, QuickBaseInputError

class FakeTransport:
    def __init__(self, tables, fields):
        self._tables = tables
        self._fields = fields
    def get(self, path, params=None):
        if path == "tables":
            return {"tables": self._tables}
        if path.startswith("tables/") and "relationships" not in path:
            # table metadata
            tbl = self._tables[0]
            return {"nextRecordId": tbl["size"]+1}
        if path == "fields":
            return {"fields": self._fields}
        if path.endswith("/relationships"):
            return {"relationships":[]}
        raise ValueError(path)

def make_cache(tmp_path, monkeypatch):
    data = {"A":"ida","B":"idb"}
    monkeypatch.setenv("QB_APP_IDS", json.dumps(data))
    return QuickBaseMetaCache(FakeTransport(
        tables=[{"id":"T1","name":"Tab","size":5}],
        fields=[{"id":1,"label":"F1","fieldType":"text"}]
    ))

def test_get_app_id(monkeypatch, tmp_path):
    cache = make_cache(tmp_path, monkeypatch)
    assert cache.get_app_id("A") == "ida"
    with pytest.raises(QuickBaseInputError):
        cache.get_app_id("X")

def test_get_table_and_field(monkeypatch, tmp_path):
    cache = make_cache(tmp_path, monkeypatch)
    tbl = cache.get_table("A", "Tab")
    assert tbl["id"] == "T1" and tbl["size"] == 5
    fmap = cache.get_field_map("A", "Tab")
    assert fmap["F1"]["id"] == 1
