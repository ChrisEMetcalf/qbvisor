import json

import pytest

from qbvisor.metadata import QuickBaseInputError, QuickBaseMetaCache


class FakeTransport:
    def __init__(self, tables, fields):
        self._tables = tables
        self._fields = fields
        self.calls = []

    def get(self, path, params=None):
        self.calls.append((path, params))
        if path == "tables":
            return self._tables
        if path.startswith("tables/") and "relationships" not in path:
            # table metadata
            table_id = path.removeprefix("tables/")
            tbl = next(table for table in self._tables if table["id"] == table_id)
            return {"nextRecordId": tbl["size"] + 1}
        if path == "fields":
            return self._fields
        if path.endswith("/relationships"):
            return {"relationships": []}
        raise ValueError(path)


def make_cache(tmp_path, monkeypatch):
    data = {"A": "ida", "B": "idb"}
    monkeypatch.setenv("QB_APP_IDS", json.dumps(data))
    return QuickBaseMetaCache(
        FakeTransport(
            tables=[{"id": "T1", "name": "Tab", "size": 5}],
            fields=[
                {"id": 1, "label": "F1", "fieldType": "text"},
                {"id": 2, "label": "F2", "fieldType": "numeric"},
            ],
        )
    )


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


def test_repeated_field_resolution_reuses_table_and_field_metadata(monkeypatch, tmp_path):
    cache = make_cache(tmp_path, monkeypatch)

    for _ in range(1_000):
        assert cache.get_field_id("A", "Tab", "f1") == 1
        assert cache.get_field_id("ida", "T1", "F2") == 2

    assert cache.transport.calls == [
        ("tables", {"appId": "ida"}),
        ("tables/T1", {"appId": "ida"}),
        ("fields", {"tableId": "T1", "includeFieldPerms": "true"}),
    ]


def test_empty_field_map_is_cached(monkeypatch, tmp_path):
    cache = make_cache(tmp_path, monkeypatch)
    cache.transport._fields = []

    assert cache.get_field_map("A", "Tab") == {}
    assert cache.get_field_map("A", "Tab") == {}

    assert [path for path, _ in cache.transport.calls].count("fields") == 1


def test_invalidate_fields_discards_only_the_selected_table_fields(monkeypatch, tmp_path):
    cache = make_cache(tmp_path, monkeypatch)
    cache.get_field_map("A", "Tab")

    cache.invalidate_fields("ida", "T1")

    assert cache.cache["A"]["tables"]["Tab"]["fields"] == {}

    assert cache.get_field_id("A", "Tab", "F1") == 1
    assert [path for path, _ in cache.transport.calls].count("tables") == 1
    assert [path for path, _ in cache.transport.calls].count("tables/T1") == 1
    assert [path for path, _ in cache.transport.calls].count("fields") == 2


def test_invalidate_tables_refreshes_the_table_catalog(monkeypatch, tmp_path):
    cache = make_cache(tmp_path, monkeypatch)
    assert cache.get_table_id("A", "Tab") == "T1"

    cache.transport._tables = [{"id": "T2", "name": "Renamed", "size": 3}]
    cache.invalidate_tables("ida")

    assert cache.get_table_id("A", "renamed") == "T2"
    assert [path for path, _ in cache.transport.calls].count("tables") == 2
    assert [path for path, _ in cache.transport.calls].count("tables/T2") == 1
