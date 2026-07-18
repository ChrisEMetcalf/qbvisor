from datetime import UTC, datetime
from email.utils import format_datetime
from unittest.mock import Mock

import pytest
import requests

from qbvisor import (
    QuickbaseConfigurationError,
    QuickbaseConnectionError,
    QuickbaseHTTPError,
    QuickbaseRateLimitError,
    QuickbaseResponseError,
    QuickbaseTimeoutError,
)
from qbvisor.transport import DEFAULT_TIMEOUT, QuickBaseTransport, RetryPolicy


def response(
    status_code: int,
    payload: object | None = None,
    *,
    headers: dict[str, str] | None = None,
    content: bytes | None = None,
) -> Mock:
    result = Mock(spec=requests.Response)
    result.status_code = status_code
    result.headers = headers or {}
    result.content = content if content is not None else (b"json" if payload is not None else b"")
    result.json.return_value = payload
    return result


def transport(session: Mock, **kwargs: object) -> QuickBaseTransport:
    return QuickBaseTransport(
        realm_hostname="example.quickbase.com",
        auth_token="secret-token",
        session=session,
        **kwargs,
    )


def test_missing_credentials_preserves_os_error_compatibility(monkeypatch):
    monkeypatch.delenv("QB_REALM_HOSTNAME", raising=False)
    monkeypatch.delenv("QB_REALM_API_KEY", raising=False)

    with pytest.raises(QuickbaseConfigurationError) as caught:
        QuickBaseTransport()

    assert isinstance(caught.value, OSError)


def test_reuses_session_and_applies_default_timeout():
    session = Mock(spec=requests.Session)
    session.request.side_effect = [response(200, {"first": True}), response(200, {"second": True})]
    client = transport(session)

    assert client.get("apps/one") == {"first": True}
    assert client.get("apps/two") == {"second": True}

    assert session.request.call_count == 2
    assert session.request.call_args.kwargs["timeout"] == DEFAULT_TIMEOUT
    assert session.request.call_args.kwargs["headers"]["Authorization"] == "secret-token"


def test_injected_session_is_not_closed():
    session = Mock(spec=requests.Session)

    transport(session).close()

    session.close.assert_not_called()


def test_owned_session_is_closed(monkeypatch):
    session = Mock(spec=requests.Session)
    monkeypatch.setattr(requests, "Session", Mock(return_value=session))

    with QuickBaseTransport("example.quickbase.com", "secret-token"):
        pass

    session.close.assert_called_once_with()


def test_http_error_exposes_quickbase_context_without_request_credentials():
    session = Mock(spec=requests.Session)
    session.request.return_value = response(
        400,
        {"message": "Invalid input", "description": "A field ID is invalid"},
        headers={"qb-api-ray": "ray-400"},
    )

    with pytest.raises(QuickbaseHTTPError) as caught:
        transport(session).post("records", json_body={"data": []})

    error = caught.value
    assert error.status_code == 400
    assert error.qb_api_ray == "ray-400"
    assert "secret-token" not in str(error)
    assert session.request.call_count == 1


def test_safe_request_retries_gateway_failure_with_backoff():
    session = Mock(spec=requests.Session)
    session.request.side_effect = [response(503), response(200, {"ok": True})]
    sleep = Mock()

    result = transport(session, sleep=sleep, jitter=lambda _low, _high: 1.25).get("apps/one")

    assert result == {"ok": True}
    assert session.request.call_count == 2
    sleep.assert_called_once_with(1.25)


def test_safe_request_reports_attempts_after_connection_retries_are_exhausted():
    session = Mock(spec=requests.Session)
    session.request.side_effect = requests.ConnectionError("offline")

    with pytest.raises(QuickbaseConnectionError) as caught:
        transport(session, max_attempts=3, sleep=Mock()).get("apps/one")

    assert caught.value.attempts == 3
    assert session.request.call_count == 3


@pytest.mark.parametrize("failure", [requests.ConnectionError("offline"), requests.Timeout("slow")])
def test_mutation_is_not_replayed_after_uncertain_failure(failure):
    session = Mock(spec=requests.Session)
    session.request.side_effect = failure

    expected = (
        QuickbaseTimeoutError if isinstance(failure, requests.Timeout) else QuickbaseConnectionError
    )
    with pytest.raises(expected) as caught:
        transport(session).post("records", json_body={"data": []})

    assert caught.value.attempts == 1
    assert session.request.call_count == 1


def test_mutation_is_not_replayed_after_gateway_failure():
    session = Mock(spec=requests.Session)
    session.request.return_value = response(503, {"message": "Unavailable"})

    with pytest.raises(QuickbaseHTTPError) as caught:
        transport(session).post("records", json_body={"data": []})

    assert caught.value.status_code == 503
    assert session.request.call_count == 1


def test_rate_limit_is_retried_for_mutation_using_retry_after_seconds():
    session = Mock(spec=requests.Session)
    session.request.side_effect = [
        response(429, headers={"Retry-After": "3", "QB-API-Ray": "ray-429"}),
        response(200, {"ok": True}),
    ]
    sleep = Mock()

    result = transport(session, sleep=sleep).post("records", json_body={"data": []})

    assert result == {"ok": True}
    sleep.assert_called_once_with(3.0)


def test_retry_after_http_date_is_respected():
    session = Mock(spec=requests.Session)
    retry_at = datetime(2026, 7, 18, 18, 0, 5, tzinfo=UTC)
    session.request.side_effect = [
        response(429, headers={"Retry-After": format_datetime(retry_at, usegmt=True)}),
        response(200, {"ok": True}),
    ]
    sleep = Mock()

    transport(session, sleep=sleep, clock=lambda: retry_at.timestamp() - 5).get("apps/one")

    sleep.assert_called_once_with(5.0)


def test_exhausted_rate_limit_raises_specific_error_with_diagnostics():
    session = Mock(spec=requests.Session)
    session.request.return_value = response(
        429,
        {"message": "Too many requests"},
        headers={"Retry-After": "0", "qb-api-ray": "ray-final"},
    )

    with pytest.raises(QuickbaseRateLimitError) as caught:
        transport(session, max_attempts=2, sleep=Mock()).get("apps/one")

    assert caught.value.retry_after == "0"
    assert caught.value.qb_api_ray == "ray-final"
    assert session.request.call_count == 2


def test_empty_success_response_returns_empty_mapping():
    session = Mock(spec=requests.Session)
    session.request.return_value = response(204)

    assert transport(session).delete("records") == {}


def test_invalid_success_json_raises_response_error_with_ray():
    session = Mock(spec=requests.Session)
    invalid_response = response(200, headers={"qb-api-ray": "ray-invalid"}, content=b"not json")
    invalid_response.json.side_effect = requests.JSONDecodeError("invalid", "not json", 0)
    session.request.return_value = invalid_response

    with pytest.raises(QuickbaseResponseError) as caught:
        transport(session).get("apps/one")

    assert caught.value.qb_api_ray == "ray-invalid"


def test_read_like_post_can_opt_into_safe_connection_retries():
    session = Mock(spec=requests.Session)
    session.request.side_effect = [requests.ConnectionError("offline"), response(200, {"data": []})]

    result = transport(session, sleep=Mock()).post(
        "records/query", json_body={"from": "table"}, retry_policy=RetryPolicy.SAFE
    )

    assert result == {"data": []}
    assert session.request.call_count == 2
