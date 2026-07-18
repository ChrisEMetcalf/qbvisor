import os

import pytest

from qbvisor import QuickbaseHTTPError, QuickBaseTransport

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def sandbox_transport():
    variable_names = (
        "QBVISOR_TEST_REALM",
        "QBVISOR_TEST_TOKEN",
        "QBVISOR_TEST_APP_ID",
    )
    config = {name: os.getenv(name) for name in variable_names}
    missing = [name for name, value in config.items() if not value]
    if missing:
        pytest.skip(f"Persistent sandbox is not configured; missing {', '.join(missing)}")

    transport = QuickBaseTransport(
        realm_hostname=config["QBVISOR_TEST_REALM"],
        auth_token=config["QBVISOR_TEST_TOKEN"],
    )
    try:
        yield transport, config["QBVISOR_TEST_APP_ID"]
    finally:
        transport.close()


def test_reads_configured_sandbox_app(sandbox_transport):
    transport, app_id = sandbox_transport

    payload = transport.get(f"apps/{app_id}")

    assert payload["id"] == app_id


def test_sandbox_error_includes_quickbase_diagnostic_id(sandbox_transport):
    transport, _ = sandbox_transport

    with pytest.raises(QuickbaseHTTPError) as caught:
        transport.get("apps/qbvisor-invalid-app-id")

    assert caught.value.status_code in {400, 404}
    assert caught.value.qb_api_ray
