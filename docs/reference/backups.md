# Backup models

`QuickBaseClient.backup_app()` returns an `ApplicationBackup`. Use `BackupOptions` to control
attachment completeness, page size, concurrency, and consistency enforcement.

::: qbvisor.backup
    options:
      members:
        - BackupOptions
        - ApplicationBackup
        - BackupVerification
        - BackupManifest
        - BackupTable
        - BackupArtifact
        - AttachmentVersionMode
        - BackupArtifactKind
