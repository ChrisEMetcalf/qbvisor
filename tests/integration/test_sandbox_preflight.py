from __future__ import annotations

import pytest
from conftest import _validated_sandbox_config


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
