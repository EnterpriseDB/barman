backup *SERVER_NAME*
:   Perform a backup of `SERVER_NAME` using parameters specified in the
    configuration file. Specify `all` as `SERVER_NAME` to perform a backup
    of all the configured servers. You can also specify `SERVER_NAME` multiple
    times to perform a backup of the specified servers -- e.g. `barman backup
    SERVER_1_NAME SERVER_2_NAME`.

    --name
    :   a friendly name for this backup which can be used in place of the
        backup ID in barman commands.

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
        `INCREMENTAL_TYPE` is not explicit.

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

    -j, --jobs
    :   Number of parallel workers to copy files during backup. Overrides
        value of the parameter `parallel_jobs`, if present in the
        configuration file.

    --jobs-start-batch-period
    :   The time period in seconds over which a single batch of jobs will be
        started. Overrides the value of `parallel_jobs_start_batch_period`, if
        present in the configuration file. Defaults to 1 second.

    --jobs-start-batch-size
    :   Maximum number of parallel workers to start in a single batch.
        Overrides the value of `parallel_jobs_start_batch_size`, if present in
        the configuration file. Defaults to 10 jobs.

    --bwlimit KBPS
    :   maximum transfer rate in kilobytes per second.
        A value of 0 means no limit.
        Overrides 'bandwidth_limit' configuration option. Default is undefined.

    --wait, -w
    :   wait for all required WAL files by the base backup to be archived

    --wait-timeout
    :   the time, in seconds, spent waiting for the required WAL
        files to be archived before timing out

    --keepalive-interval
    :   an interval, in seconds, at which a hearbeat query will be sent to the
        server to keep the libpq connection alive during an Rsync backup. Default
        is 60. A value of 0 disables it.

    --manifest
    :   forces the creation of a backup manifest file at the end of a backup. 
        Overrides value of the parameter `autogenerate_manifest`, 
        from the configuration file. 
        Works with rsync backup method and strategies only

    --no-manifest
    :   disables the automatic creation of a backup manifest file 
        at the end of a backup. 
        Overrides value of the parameter `autogenerate_manifest`, 
        from the configuration file. 
        Works with rsync backup method and strategies only
