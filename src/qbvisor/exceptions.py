class QuickbaseError(Exception):
    """Base class for errors raised by qbvisor."""


class QuickbaseConfigurationError(QuickbaseError):
    """Raised when required client configuration is missing or invalid."""


class QuickbaseConnectionError(QuickbaseError):
    """Raised when qbvisor cannot complete an HTTP connection."""

    def __init__(self, method: str, path: str, attempts: int):
        self.method = method
        self.path = path
        self.attempts = attempts
        super().__init__(f"{method} {path} failed after {attempts} attempt(s)")


class QuickbaseTimeoutError(QuickbaseConnectionError):
    """Raised when a Quickbase request exceeds its configured timeout."""


class QuickbaseHTTPError(QuickbaseError):
    """Raised when Quickbase returns an unsuccessful HTTP response."""

    def __init__(
        self,
        *,
        method: str,
        path: str,
        status_code: int,
        message: str | None = None,
        description: str | None = None,
        qb_api_ray: str | None = None,
        retry_after: str | None = None,
    ):
        self.method = method
        self.path = path
        self.status_code = status_code
        self.message = message
        self.description = description
        self.qb_api_ray = qb_api_ray
        self.retry_after = retry_after

        summary = message or "Quickbase request failed"
        detail = f": {description}" if description else ""
        ray = f" (qb-api-ray: {qb_api_ray})" if qb_api_ray else ""
        super().__init__(f"{method} {path} returned {status_code}: {summary}{detail}{ray}")


class QuickbaseRateLimitError(QuickbaseHTTPError):
    """Raised when Quickbase rate limiting remains after retries are exhausted."""


class QuickbaseResponseError(QuickbaseError):
    """Raised when a successful Quickbase response cannot be decoded."""

    def __init__(self, method: str, path: str, qb_api_ray: str | None = None):
        self.method = method
        self.path = path
        self.qb_api_ray = qb_api_ray
        ray = f" (qb-api-ray: {qb_api_ray})" if qb_api_ray else ""
        super().__init__(f"{method} {path} returned an invalid JSON response{ray}")
