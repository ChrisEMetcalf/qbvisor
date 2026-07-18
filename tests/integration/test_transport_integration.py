import pytest

from qbvisor import QuickbaseHTTPError
from qbvisor.transport import QuickBaseTransport

pytestmark = pytest.mark.integration


def test_reads_configured_sandbox_app(sandbox_transport, sandbox_config):
    payload = sandbox_transport.get(f"apps/{sandbox_config.app_id}")

    assert isinstance(payload, dict)
    assert payload["id"] == sandbox_config.app_id


def test_tables_endpoint_returns_documented_top_level_array(
    sandbox_transport: QuickBaseTransport, sandbox_config
):
    payload = sandbox_transport.get("tables", params={"appId": sandbox_config.app_id})

    assert isinstance(payload, list)
    assert all(isinstance(table, dict) for table in payload)


def test_sandbox_error_includes_quickbase_diagnostic_id(sandbox_transport):
    with pytest.raises(QuickbaseHTTPError) as caught:
        sandbox_transport.get("apps/qbvisor-invalid-app-id")

    assert 400 <= caught.value.status_code < 500
    assert caught.value.message
    assert caught.value.description
    assert caught.value.qb_api_ray
