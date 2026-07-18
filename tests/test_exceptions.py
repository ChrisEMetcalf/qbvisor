from qbvisor import (
    QuickbaseConnectionError,
    QuickbaseHTTPError,
    QuickbaseResponseError,
)


def test_http_error_exposes_safe_quickbase_context():
    error = QuickbaseHTTPError(
        method="POST",
        path="records/query",
        status_code=400,
        message="Bad request",
        description="The request body is invalid",
        qb_api_ray="ray-123",
        retry_after=None,
    )

    assert error.status_code == 400
    assert error.message == "Bad request"
    assert error.description == "The request body is invalid"
    assert error.qb_api_ray == "ray-123"
    assert str(error) == (
        "POST records/query returned 400: Bad request: "
        "The request body is invalid (qb-api-ray: ray-123)"
    )
    assert "Authorization" not in str(error)


def test_connection_error_records_attempt_count_without_credentials():
    error = QuickbaseConnectionError("GET", "apps/app_id", 3)

    assert error.attempts == 3
    assert str(error) == "GET apps/app_id failed after 3 attempt(s)"


def test_response_error_includes_diagnostic_identifier():
    error = QuickbaseResponseError("GET", "apps/app_id", "ray-456")

    assert str(error) == ("GET apps/app_id returned an invalid JSON response (qb-api-ray: ray-456)")
