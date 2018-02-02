backup_method
:   Configure the method barman used for backup execution.
    If set to `rsync` (default), barman will execute backup using the `rsync`
    command. If set to `postgres` barman will use the `pg_basebackup` command
    to execute the backup. Global/Server.
