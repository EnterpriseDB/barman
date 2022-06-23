recover *\[OPTIONS\]* *SERVER_NAME* *BACKUP_ID* *DESTINATION_DIRECTORY*
:   Recover a backup in a given directory (local or remote, depending
    on the `--remote-ssh-command` option settings).
    See the [Backup ID shortcuts](#shortcuts) section below for available shortcuts.

    --target-tli *TARGET_TLI*
    :   Recover the specified timeline. The special values `current` and
        `latest` can be used in addition to a numeric timeline ID.
        The default behaviour for PostgreSQL versions >= 12 is to recover
        to the `latest` timeline in the WAL archive. The default for
        PostgreSQL versions < 12 is to recover along the timeline which
        was current when the backup was taken.

    --target-time *TARGET_TIME*
    :   Recover to the specified time.

        You can use any valid unambiguous representation
        (e.g: "YYYY-MM-DD HH:MM:SS.mmm").

    --target-xid *TARGET_XID*
    :   Recover to the specified transaction ID.

    --target-lsn *TARGET_LSN*
    :   Recover to the specified LSN (Log Sequence Number). Requires PostgreSQL 10 or above.

    --target-name *TARGET_NAME*
    :   Recover to the named restore point previously created with
        the `pg_create_restore_point(name)` (for PostgreSQL 9.1 and above users).

    --target-immediate
    :   Recover ends when a consistent state is reached (end of the base
        backup)

    --exclusive
    :   Set target (time, XID or LSN) to be non inclusive.

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

    --bwlimit KBPS
    :   maximum transfer rate in kilobytes per second.
        A value of 0 means no limit.
        Overrides 'bandwidth_limit' configuration option. Default is undefined.

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

    --recovery-staging-path *STAGING_PATH*
    :   A path to a location on the recovery host (either the barman server
        or a remote host if --remote-ssh-command is also used) where files
        for a compressed backup will be staged before being uncompressed to
        the destination directory. Backups will be staged in their own directory
        within the staging path according to the following naming convention:
        "barman-staging-SERVER_NAME-BACKUP_ID". The staging directory within
        the staging path will be removed at the end of the recovery process.
        This option is *required* when recovering from compressed backups and
        has no effect otherwise.
