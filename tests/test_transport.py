import pytest
import requests
from pyqb_client.transport import QuickBaseTransport

class DummyResponse:
    def __init__(self, status, json_data=None):
        self.status_code = status
        self._json = json_data or {}
        self.headers = {}

    def raise_for_status(self):
        if self.status_code > 400:
            raise requests.HTTPError(f"HTTP Error: {self.status_code}")
    
    def json(self):
        return self._json
    
def test_make_request_success(monkeypatch):
    qt = QuickBaseTransport(realm_hostname="test", auth_token="dummy")
    def fake_get(url, **kwargs):
        return DummyResponse(200, {"foo": "bar"})
    monkeypatch.setattr(requests, "get", fake_get)
    out = qt.get("test/path", params={"a": 1})
    assert out == {"foo": "bar"}

def test_make_request_retry(monkeypatch):
    qt = QuickBaseTransport(realm_hostname="test", auth_token="dummy")
    calls = {"n": 0}
    def flaky(url, headers, params, json):
        calls["n"] += 1
        if calls["n"] < 2:
            return DummyResponse(502)
        return DummyResponse(200, {"ok": True})

    monkeypatch.setattr(requests, "post", flaky)
    out = qt.post("p", params={}, json_body={})
    assert out == {"ok": True}
    assert calls["n"] == 2
