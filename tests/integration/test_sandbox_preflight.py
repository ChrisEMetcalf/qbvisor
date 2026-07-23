from __future__ import annotations

import pytest
from conftest import (
    _require_operational_opt_ins,
    _stop_sandbox_setup,
    _validated_sandbox_config,
)


def _config(**overrides: str) -> dict[str, str | None]:
    config: dict[str, str | None] = {
        "QBVISOR_TEST_REALM": "example.quickbase.com",
        "QBVISOR_TEST_TOKEN": "token-not-printed",
        "QBVISOR_TEST_APP_ID": "bqexample",
    }
    config.update(overrides)
    return config


def test_sandbox_preflight_accepts_bare_identifiers():
    config = _validated_sandbox_config(_config())

    assert config.realm == "example.quickbase.com"
    assert config.app_id == "bqexample"


def test_sandbox_preflight_rejects_app_mapping_without_echoing_it():
    mapping = '{"Sandbox":"bqexample"}'

    with pytest.raises(ValueError, match="not a JSON mapping") as caught:
        _validated_sandbox_config(_config(QBVISOR_TEST_APP_ID=mapping))

    assert mapping not in str(caught.value)


def test_sandbox_preflight_rejects_realm_url_without_echoing_it():
    realm_url = "https://example.quickbase.com/path"

    with pytest.raises(ValueError, match="bare realm hostname") as caught:
        _validated_sandbox_config(_config(QBVISOR_TEST_REALM=realm_url))

    assert realm_url not in str(caught.value)


def test_sandbox_preflight_rejects_unicode_app_id_without_echoing_it():
    unicode_app_id = "bqéxample"

    with pytest.raises(ValueError, match="bare ASCII alphanumeric") as caught:
        _validated_sandbox_config(_config(QBVISOR_TEST_APP_ID=unicode_app_id))

    assert unicode_app_id not in str(caught.value)


def test_explicit_operational_mode_fails_when_live_opt_ins_are_missing(monkeypatch):
    monkeypatch.setenv("QBVISOR_RUN_OPERATIONAL", "1")
    monkeypatch.delenv("QBVISOR_RUN_INTEGRATION", raising=False)
    monkeypatch.delenv("QBVISOR_ALLOW_SANDBOX_MUTATIONS", raising=False)

    with pytest.raises(pytest.fail.Exception, match="requires explicit opt-in") as caught:
        _require_operational_opt_ins()

    assert "QBVISOR_RUN_INTEGRATION=1" in str(caught.value)
    assert "QBVISOR_ALLOW_SANDBOX_MUTATIONS=1" in str(caught.value)


def test_explicit_operational_mode_fails_on_missing_configuration():
    with pytest.raises(pytest.fail.Exception, match="missing QBVISOR_TEST_REALM"):
        _stop_sandbox_setup(
            "Persistent sandbox is not configured; missing QBVISOR_TEST_REALM",
            operational_requested=True,
        )


def test_ordinary_live_collection_preserves_skip_behavior():
    with pytest.raises(pytest.skip.Exception, match="Set QBVISOR_RUN_INTEGRATION=1"):
        _stop_sandbox_setup(
            "Set QBVISOR_RUN_INTEGRATION=1 to run live sandbox tests",
            operational_requested=False,
        )
