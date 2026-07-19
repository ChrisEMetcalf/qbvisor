# Exceptions

Catch the narrowest exception that supports a meaningful recovery action. The [logging and error
guide](../logging-and-errors.md) describes retry and partial-result behavior.

::: qbvisor.exceptions
    options:
      members:
        - QuickbaseError
        - QuickbaseConfigurationError
        - QuickbaseConnectionError
        - QuickbaseTimeoutError
        - QuickbaseHTTPError
        - QuickbaseRateLimitError
        - QuickbaseBatchError
        - QuickbaseResponseError
        - QuickbaseSchemaStateError
        - QuickbaseSchemaApplyError
        - QuickbaseSchemaConflictError
        - QuickbaseSchemaStalePlanError
        - QuickbaseSchemaLockError
        - BackupConsistencyError
        - BackupIntegrityError
