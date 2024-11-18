check-backup *SERVER_NAME* *BACKUP_ID*
:   Make sure that all the required WAL files to check
    the consistency of a physical backup (that is, from the
    beginning to the end of the full backup) are correctly
    archived. This command is automatically invoked by the
    `cron` command and at the end of every backup operation.
