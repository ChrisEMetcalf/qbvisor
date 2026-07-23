"""Secret-free diagnostics and recovery helpers for live operational tests."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from time import perf_counter
from typing import Any

OPERATIONAL_PREFIX = "qbvisor-operational-"
REQUIRED_OPERATIONAL_CHECKS = frozenset(
    {"recovery", "read", "upsert", "attachment", "backup", "schema-plan"}
)
_SAFE_LABEL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$")
_SAFE_CANDIDATE = re.compile(
    r"^(?:local|scheduled-main|v[0-9]+\.[0-9]+\.[0-9]+(?:[.-][0-9A-Za-z.-]+)?)$"
)


class IntentionalCleanupFailure(RuntimeError):
    """Failure used to prove that the next operational run recovers an orphan."""


class CleanupCountMismatch(AssertionError):
    """Raised when a mutation did not delete the expected number of records."""


class IncompleteOperationalRun(AssertionError):
    """Raised when an explicitly requested operational run did not fully pass."""


def safe_run_id(value: str | None = None) -> str:
    """Derive an opaque run ID without persisting arbitrary environment text."""

    if value is None:
        return f"run-{uuid.uuid4().hex[:16]}"
    candidate = value.strip()
    if _SAFE_LABEL.fullmatch(candidate) is None:
        raise ValueError(
            "QBVISOR_OPERATIONAL_RUN_ID must contain only letters, digits, dot, underscore, "
            "or hyphen and be at most 80 characters"
        )
    if re.fullmatch(r"run-[0-9a-f]{16}", candidate):
        return candidate
    return f"run-{sha256(candidate.encode('utf-8')).hexdigest()[:16]}"


def safe_candidate_ref(value: str | None) -> str | None:
    """Return a display-safe candidate ref without copying arbitrary environment text."""

    if not value:
        return None
    candidate = value.strip()
    if _SAFE_CANDIDATE.fullmatch(candidate) is None:
        raise ValueError(
            "QBVISOR_CANDIDATE_REF must be local, scheduled-main, or a v-prefixed "
            "semantic-version tag"
        )
    return candidate


def recover_operational_records(
    *,
    delete_matching: Callable[[], int],
    count_remaining: Callable[[], int],
) -> int:
    """Delete prior operational records and prove that the prefix is empty."""

    deleted = delete_matching()
    remaining = count_remaining()
    if remaining:
        raise AssertionError(
            f"Operational recovery left {remaining} prefixed record(s) in the sandbox"
        )
    return deleted


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _diagnostic(error: BaseException) -> dict[str, Any]:
    """Select actionable exception metadata while deliberately omitting messages and values."""

    result: dict[str, Any] = {"errorType": type(error).__name__}
    status_code = getattr(error, "status_code", None)
    if isinstance(status_code, int):
        result["httpStatus"] = status_code
    qb_api_ray = getattr(error, "qb_api_ray", None)
    if isinstance(qb_api_ray, str) and _SAFE_LABEL.fullmatch(qb_api_ray):
        result["qbApiRay"] = qb_api_ray
    return result


class OperationalDiagnostics:
    """Persist a small whitelist of operational outcomes after every phase."""

    def __init__(
        self,
        output_path: Path,
        *,
        run_id: str,
        candidate_ref: str | None = None,
    ) -> None:
        self.output_path = output_path
        self.summary: dict[str, Any] = {
            "schemaVersion": 1,
            "runId": safe_run_id(run_id),
            "candidateRef": safe_candidate_ref(candidate_ref),
            "startedAt": _timestamp(),
            "status": "running",
            "recoveredRecords": None,
            "checks": {},
        }
        self.write()

    def write(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.output_path.with_suffix(f"{self.output_path.suffix}.tmp")
        temporary.write_text(
            json.dumps(self.summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(self.output_path)

    def record_recovery(self, deleted: int) -> None:
        self.summary["recoveredRecords"] = deleted
        self.write()

    def _record(
        self,
        name: str,
        *,
        started: float,
        error: BaseException | None = None,
        deleted: int | None = None,
        cleanup_error: BaseException | None = None,
    ) -> None:
        check: dict[str, Any] = {
            "status": "passed" if error is None and cleanup_error is None else "failed",
            "seconds": round(perf_counter() - started, 4),
        }
        if error is not None:
            check.update(_diagnostic(error))
        if deleted is not None:
            check["deletedRecords"] = deleted
        if cleanup_error is not None:
            check["cleanup"] = _diagnostic(cleanup_error)
        checks = self.summary["checks"]
        assert isinstance(checks, dict)
        checks[name] = check
        self.write()

    @contextmanager
    def check(self, name: str) -> Iterator[None]:
        """Record a read-only check and preserve its original failure."""

        started = perf_counter()
        try:
            yield
        except BaseException as error:
            self._record(name, started=started, error=error)
            raise
        else:
            self._record(name, started=started)

    @contextmanager
    def mutating_check(
        self,
        name: str,
        *,
        cleanup: Callable[[], int],
        expected_deleted: Callable[[], int | None],
        fail_cleanup: bool = False,
    ) -> Iterator[None]:
        """Run cleanup after a mutation, verify its count, and preserve primary failures."""

        started = perf_counter()
        primary_error: BaseException | None = None
        cleanup_error: BaseException | None = None
        deleted: int | None = None
        try:
            yield
        except BaseException as error:
            primary_error = error

        if fail_cleanup:
            cleanup_error = IntentionalCleanupFailure(
                "Intentional cleanup failure; unset QBVISOR_OPERATIONAL_FAIL_CLEANUP_ONCE "
                "and rerun to prove prefix recovery"
            )
        else:
            try:
                deleted = cleanup()
                expected = expected_deleted()
                if expected is not None and deleted != expected:
                    cleanup_error = CleanupCountMismatch(
                        f"Operational cleanup deleted {deleted} record(s); expected {expected}"
                    )
            except BaseException as error:
                cleanup_error = error

        self._record(
            name,
            started=started,
            error=primary_error,
            deleted=deleted,
            cleanup_error=cleanup_error,
        )
        if primary_error is not None:
            if cleanup_error is not None:
                primary_error.add_note(
                    "Operational cleanup also failed; inspect the secret-free summary artifact"
                )
            raise primary_error.with_traceback(primary_error.__traceback__)
        if cleanup_error is not None:
            raise cleanup_error.with_traceback(cleanup_error.__traceback__)

    def finish(self, *, require_complete: bool = False) -> None:
        checks = self.summary["checks"]
        assert isinstance(checks, dict)
        missing = sorted(REQUIRED_OPERATIONAL_CHECKS - checks.keys())
        failed = sorted(
            name
            for name in REQUIRED_OPERATIONAL_CHECKS & checks.keys()
            if checks[name].get("status") != "passed"
        )
        self.summary["completedAt"] = _timestamp()
        self.summary["missingChecks"] = missing
        self.summary["failedChecks"] = failed
        self.summary["status"] = "passed" if not missing and not failed else "failed"
        self.write()
        if require_complete and self.summary["status"] != "passed":
            raise IncompleteOperationalRun(
                "Operational run did not complete every required check successfully; "
                "inspect the secret-free summary artifact"
            )
