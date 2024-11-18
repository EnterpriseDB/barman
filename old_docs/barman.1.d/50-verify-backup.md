verify-backup *SERVER_NAME* *BACKUP_ID*
:   Executes `pg_verifybackup` against a backup manifest file (available since Postgres 13).
    For rsync backups, it can be used with generate-manifest command.
    Requires `pg_verifybackup` installed on the backup server
