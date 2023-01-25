backup_method
:   Configure the method barman used for backup execution.
    If set to `rsync` (default), barman will execute backup using the `rsync`
    command over SSH (requires `ssh_command`).
    If set to `postgres` barman will use the `pg_basebackup` command to execute the backup.
    If set to `local-rsync`, barman will assume to be running on the same server
    as the PostgreSQL instance and with the same user, then execute `rsync` for the
    file system copy.
    If set to `snapshot`, barman will use the API for the cloud provider defined in
    the `snapshot_provider` option to create snapshots of disks specified in the
    `snapshot_disks` option and save only the backup label and metadata to its own
    storage.
    Global/Server.
