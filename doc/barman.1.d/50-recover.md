recover *\[OPTIONS\]* *SERVER_NAME* *BACKUP_ID* *DESTINATION_DIRECTORY*
:   Recover a backup in a given directory (local or remote, depending
    on the `--remote-ssh-command` option settings).
    See the [Backup ID shortcuts](#shortcuts) section below for available shortcuts.

    --target-tli *TARGET_TLI*
    :   Recover the specified timeline.

    --target-time *TARGET_TIME*
    :   Recover to the specified time.

        You can use any valid unambiguous representation
        (e.g: "YYYY-MM-DD HH:MM:SS.mmm").

    --target-xid *TARGET_XID*
    :   Recover to the specified transaction ID.

    --target-name *TARGET_NAME*
    :   Recover to the named restore point previously created with
        the `pg_create_restore_point(name)` (for PostgreSQL 9.1 and above users).

    --target-immediate
    :   Recover ends when a consistent state is reached (end of the base
        backup)

    --exclusive
    :   Set target xid to be non inclusive.

    --target-action *ACTION*
    :   Trigger the specified action once the recovery target is reached.
        Possible actions are: `pause` (PostgreSQL 9.1 and above),
        `shutdown` (PostgreSQL 9.5 and above) and `promote` (ditto).
        This option requires a target to be defined, with one of the
        above options.

    --tablespace *NAME:LOCATION*
    :   Specify tablespace relocation rule.

    --remote-ssh-command *SSH_COMMAND*
    :   This options activates remote recovery, by specifying the
        secure shell command to be launched on a remote host.
        This is the equivalent of the "ssh_command" server option
        in the configuration file for remote recovery.
        Example: 'ssh postgres@db2'.

    --retry-times *RETRY_TIMES*
    :   Number of retries of data copy during base backup after
        an error. Overrides value of the parameter `basebackup_retry_times`,
        if present in the configuration file.

    --no-retry
    :   Same as `--retry-times 0`

    --retry-sleep
    :   Number of seconds of wait after a failed copy, before retrying.
        Overrides value of the parameter `basebackup_retry_sleep`,
        if present in the configuration file.

    -j , --jobs
    :   Number of parallel workers to copy files during recovery. Overrides
        value of the parameter `parallel_jobs`, if present in the
        configuration file. Works only for servers configured through `rsync`/SSH.

    --get-wal, --no-get-wal
    :   Enable/Disable usage of `get-wal` for WAL fetching during recovery.
        Default is based on `recovery_options` setting.

    --network-compression, --no-network-compression
    :   Enable/Disable network compression during remote recovery.
        Default is based on `network_compression` configuration setting.

    --standby-mode
    :   Specifies whether to start the PostgreSQL server as a standby.
        Default is undefined.

