backup *SERVER_NAME*
:   Perform a backup of `SERVER_NAME` using parameters specified in the
    configuration file. Specify `all` as `SERVER_NAME` to perform a backup
    of all the configured servers.

    --immediate-checkpoint
    :   forces the initial checkpoint to be done as quickly as possible.
        Overrides value of the parameter `immediate_checkpoint`, if present
        in the configuration file.

    --no-immediate-checkpoint
    :   forces to wait for the checkpoint.
        Overrides value of the parameter `immediate_checkpoint`, if present
        in the configuration file.

    --reuse-backup [INCREMENTAL_TYPE]
    :   Overrides `reuse_backup` option behaviour. Possible values for
        `INCREMENTAL_TYPE` are:

        - *off*: do not reuse the last available backup;
        - *copy*: reuse the last available backup for a server and
           create a copy of the unchanged files (reduce backup time);
        - *link*: reuse the last available backup for a server and
           create a hard link of the unchanged files (reduce backup time
           and space);

        `link` is the default target if `--reuse-backup` is used and
        `INCREMENTAL_TYPE` is not explicited.

    --retry-times
    :   Number of retries of base backup copy, after an error.
        Used during both backup and recovery operations.
        Overrides value of the parameter `basebackup_retry_times`,
        if present in the configuration file.

    --no-retry
    :   Same as `--retry-times 0`

    --retry-sleep
    :   Number of seconds of wait after a failed copy, before retrying.
        Used during both backup and recovery operations.
        Overrides value of the parameter `basebackup_retry_sleep`,
        if present in the configuration file.

    -j , --jobs
    :   Number of parallel workers to copy files during backup. Overrides
        value of the parameter `parallel_jobs`, if present in the
        configuration file.
