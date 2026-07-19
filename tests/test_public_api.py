import qbvisor


def test_existing_public_exports_remain_available():
    expected_exports = {
        "QuickBaseClient",
        "ApplicationBackup",
        "BackupManifest",
        "BackupOptions",
        "BackupArtifactKind",
        "BackupVerification",
        "BackupConsistencyError",
        "BackupIntegrityError",
        "AppSpec",
        "FieldSpec",
        "FormulaFieldType",
        "FormulaSpec",
        "RelationshipSpec",
        "SchemaAttributeChange",
        "SchemaApplyResult",
        "SchemaChange",
        "SchemaPlan",
        "SchemaState",
        "StateResource",
        "SummaryFieldSpec",
        "TableSpec",
        "QuickBaseTransport",
        "QuickbaseBatchError",
        "QuickbaseSchemaApplyError",
        "QuickbaseSchemaConflictError",
        "QuickbaseSchemaLockError",
        "QuickbaseSchemaStateError",
        "QuickbaseSchemaStalePlanError",
        "RetryPolicy",
        "QueryHelper",
        "RelationshipAccumulation",
        "RelationshipSummary",
        "sanitize_filenames",
        "ensure_temp_dir",
        "generate_timestamped_folder",
        "summarize_file_sizes",
        "LoggingConfigurator",
        "get_logger",
    }

    assert expected_exports <= set(qbvisor.__all__)
    for name in expected_exports:
        assert hasattr(qbvisor, name)
