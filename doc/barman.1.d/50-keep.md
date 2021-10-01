keep *SERVER_NAME* *BACKUP_ID*
:   Flag the specified backup as an archival backup which should be
    kept forever, regardless of any retention policies in effect.
    See the [Backup ID shortcuts](#shortcuts) section below for available
    shortcuts.

    --target *RECOVERY_TARGET*
    :   Specify the recovery target for the archival backup.
        Possible values for *RECOVERY_TARGET* are:

         - *full*: The backup can always be used to recover to the latest point
           in time. To achieve this, Barman will retain all WALs needed to
           ensure consistency of the backup and all subsequent WALs.
         - *standalone*: The backup can only be used to recover the server to
           its state at the time the backup was taken. Barman will only retain
           the WALs needed to ensure consistency of the backup.

    --status
    :   Report the archival status of the backup. This will either be the
        recovery target of *full* or *standalone* for archival backups or
        *nokeep* for backups which have not been flagged as archival.

    --release
    :   Release the keep flag from this backup. This will remove its archival
        status and make it available for deletion, either directly or by
        retention policy.
