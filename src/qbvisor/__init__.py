"""Public package interface for the qbvisor Quickbase SDK."""

from pathlib import Path

from dotenv import load_dotenv

from ._version import __version__

# Try to load .env from common locations
possible_paths = [
    Path(__file__).resolve().parents[2] / ".env",  # repo root if src/qbvisor/
    Path(__file__).resolve().parents[1] / ".env",  # one level up
    Path.cwd() / ".env",  # where the script is run from
]

for dotenv_path in possible_paths:
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
        break

from .backup import (
    BACKUP_FORMAT,
    BACKUP_FORMAT_VERSION,
    ApplicationBackup,
    AttachmentVersionMode,
    BackupArtifact,
    BackupArtifactKind,
    BackupManifest,
    BackupOptions,
    BackupTable,
    BackupVerification,
)
from .client import QuickBaseClient
from .exceptions import (
    BackupConsistencyError,
    BackupIntegrityError,
    QuickbaseBatchError,
    QuickbaseConfigurationError,
    QuickbaseConnectionError,
    QuickbaseError,
    QuickbaseHTTPError,
    QuickbaseRateLimitError,
    QuickbaseResponseError,
    QuickbaseSchemaApplyError,
    QuickbaseSchemaConflictError,
    QuickbaseSchemaLockError,
    QuickbaseSchemaStalePlanError,
    QuickbaseSchemaStateError,
    QuickbaseTimeoutError,
)
from .helpers import (
    ensure_temp_dir,
    generate_timestamped_folder,
    sanitize_filenames,
    summarize_file_sizes,
)
from .log_runner import LoggingConfigurator, get_logger
from .models import RelationshipAccumulation, RelationshipSummary
from .query_helper import QueryHelper
from .schema import (
    SCHEMA_STATE_FORMAT,
    SCHEMA_STATE_FORMAT_VERSION,
    AppSpec,
    FieldSpec,
    FormulaFieldType,
    FormulaSpec,
    RelationshipSpec,
    SchemaAction,
    SchemaApplyResult,
    SchemaAttributeChange,
    SchemaChange,
    SchemaPlan,
    SchemaResourceKind,
    SchemaState,
    SchemaStateAction,
    StateResource,
    SummaryFieldSpec,
    TableSpec,
)
from .transport import QuickBaseTransport, RetryPolicy

# Expose file download utilities directly on the client

__all__ = [
    "__version__",
    "QuickBaseClient",
    "BACKUP_FORMAT",
    "BACKUP_FORMAT_VERSION",
    "ApplicationBackup",
    "AttachmentVersionMode",
    "BackupArtifact",
    "BackupArtifactKind",
    "BackupManifest",
    "BackupOptions",
    "BackupTable",
    "BackupVerification",
    "BackupConsistencyError",
    "BackupIntegrityError",
    "QuickBaseTransport",
    "RetryPolicy",
    "QuickbaseError",
    "QuickbaseBatchError",
    "QuickbaseConfigurationError",
    "QuickbaseConnectionError",
    "QuickbaseTimeoutError",
    "QuickbaseHTTPError",
    "QuickbaseRateLimitError",
    "QuickbaseResponseError",
    "QuickbaseSchemaApplyError",
    "QuickbaseSchemaConflictError",
    "QuickbaseSchemaLockError",
    "QuickbaseSchemaStateError",
    "QuickbaseSchemaStalePlanError",
    "QueryHelper",
    "SCHEMA_STATE_FORMAT",
    "SCHEMA_STATE_FORMAT_VERSION",
    "AppSpec",
    "FieldSpec",
    "FormulaFieldType",
    "FormulaSpec",
    "RelationshipSpec",
    "SchemaAction",
    "SchemaApplyResult",
    "SchemaAttributeChange",
    "SchemaChange",
    "SchemaPlan",
    "SchemaResourceKind",
    "SchemaState",
    "SchemaStateAction",
    "StateResource",
    "SummaryFieldSpec",
    "TableSpec",
    "RelationshipAccumulation",
    "RelationshipSummary",
    "sanitize_filenames",
    "ensure_temp_dir",
    "generate_timestamped_folder",
    "summarize_file_sizes",
    "LoggingConfigurator",
    "get_logger",
]
