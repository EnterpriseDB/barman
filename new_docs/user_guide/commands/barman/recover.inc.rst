.. _commands-barman-recover:

``barman recover``
""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    recover
        [ --aws-region AWS_REGION } ]
        [ --azure-resource-group AZURE_RESOURCE_GRP ]
        [ --bwlimit KBPS ]
        [ --exclusive ]
        [ --gcp-zone GCP_ZONE ]
        [ --get-wal | --no-get-wal ]
        [ { -j | --jobs } PARALLEL_WORKERS ]
        [ --jobs-start-batch-period SECONDS ]
        [ --jobs-start-batch-size NUMBER ]
        [ --local-staging-path PATH ]
        [ --network-compression | --no-network-compression ]
        [ --no-retry ]
        [ --recovery-conf-filename FILENAME ]
        [ --recovery-staging-path PATH ]
        [ --remote-ssh-command STRING ]
        [ --retry-sleep SECONDS ]
        [ --retry-times NUMBER ]
        [ --snapshot-recovery-instance INSTANCE_NAME ]
        [ --standby-mode ]
        [ --tablespace NAME:LOCATION ]
        [ --target-action { pause | shutdown | promote } ]
        [ --target-immediate ]
        [ --target-lsn LSN ]
        [ --target-name RESTORE_POINT_NAME ]
        [ --target-time TIMESTAMP ]
        [ --target-tli TLI ]
        [ --target-xid XID ]
        SERVER_NAME BACKUP_ID DESTINATION_DIR

Description
^^^^^^^^^^^

Execute a PostreSQL server recovery operation. Barman will recover the backup from a
server in the destination directory. The recovery can be performed locally (on the
barman node itself) or remotely (on another machine accessible via SSH). The location is
determined by whether or not the ``--remote-ssh-command`` option is used. More
information on this command can be found in the :ref:`recovery` section. You can use a
shortcut instead of ``BACKUP_ID``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``BACKUP_ID``
    Id of the backup in barman catalog.

``DESTINATION_DIR``
    Destination directory to recover.

``--aws-region``
    Specify the AWS region where the instance and disks for snapshot recovery are
    located. This option allows you to override the ``aws_region`` value in the Barman
    configuration.

``--azure-resource-group``
    Specify the Azure resource group containing the instance and disks for snapshot
    recovery. This option allows you to override the ``azure_resource_group`` value in
    the Barman configuration.

``--bwlimit``
    Specify the maximum transfer rate in kilobytes per second. A value of ``0``
    indicates no limit. This setting overrides the ``bandwidth_limit`` configuration
    option.

``--exclusive``
    Set target (time, XID or LSN) to be non inclusive.

``--gcp-zone``
    Specify the GCP zone where the instance and disks for snapshot recovery are located.
    This option allows you to override the ``gcp_zone`` value in the Barman
    configuration.

``--get-wal`` / ``--no-get-wal``
    Enable/disable usage of ``get-wal`` for WAL fetching during recovery. Default is based on
    ``recovery_options`` setting.

``-j`` / ``--jobs``
    Specify the number of parallel workers to use for copying files during the backup.
    This setting overrides the ``parallel_jobs`` parameter if it is specified in the
    configuration file.

``--jobs-start-batch-period``
    Specify the time period, in seconds, for starting a single batch of jobs. This value
    overrides the ``parallel_jobs_start_batch_period`` parameter if it is set in the
    configuration file. The default is ``1`` second.

``--jobs-start-batch-size``
    Specify the maximum number of parallel workers to initiate in a single batch. This
    value overrides the ``parallel_jobs_start_batch_size`` parameter if it is defined in
    the configuration file. The default is ``10`` workers.

``--local-staging-path``
    Specify path on the Barman host where the chain of backups will be combined before
    being copied to the destination directory. The contents created within the staging
    path will be removed upon completion of the recovery process. This option is
    necessary for recovering from block-level incremental backups and has no effect
    otherwise.
    
``--network-compression`` / ``--no-network-compression``
    Enable/disable network compression during remote recovery. Default is based on
    ``network_compression`` configuration setting.

``--no-retry``
    There will be no retry in case of an error. It is the same as setting
    ``--retry-times 0``.

``--recovery-conf-filename``
    Specify the name of the file where Barman should write recovery options when
    recovering backups for Postgres versions 12 and later. By default, this is set to
    ``postgresql.auto.conf``. However, if ``--recovery-conf-filename`` is specified,
    recovery options will be written to the specified value instead. While the default
    value is suitable for most Postgres installations, this option allows you to specify
    an alternative location if Postgres is managed by tools that alter the configuration
    mechanism (for example, if ``postgresql.auto.conf`` is symlinked to ``/dev/null``).

``--recovery-staging-path``
    Specify a path on the recovery host where files for a compressed backup will be
    staged before being uncompressed to the destination directory. Backups will be
    staged in their own directory within the staging path, following the naming
    convention: ``barman-staging-SERVER_NAME-BACKUP_ID``. This staging directory will be
    removed after the recovery process is complete. This option is mandatory for
    recovering from compressed backups and has no effect otherwise.

``--remote-ssh-command``
    This option enables remote recovery by specifying the secure shell command to
    execute on a remote host. It functions similarly to the ``ssh_command`` server
    option in the configuration file for remote recovery, that is, ``'ssh USER@SERVER'``.

``--retry-sleep``
    Specify the number of seconds to wait after a failed copy before retrying. This
    setting applies to both backup and recovery operations and overrides the
    ``basebackup_retry_sleep`` parameter if it is defined in the configuration file.

``--retry-times``
    Specify the number of times to retry the base backup copy in case of an error. This
    applies to both backup and recovery operations and overrides the
    ``basebackup_retry_times`` parameter if it is set in the configuration file.

``--snapshot-recovery-instance``
    Specify the name of the instance where the disks recovered from the snapshots are
    attached. This option is necessary when recovering backups created with
    ``backup_method=snapshot``.
    
``--standby-mode``
    Whether to start the Postgres server as a standby.

``--tablespace``
    Specify tablespace relocation rule. ``NAME`` is the tablespace name and ``LOCATION``
    is the recovery host destination path to recover the tablespace.

``--target-action``
    Trigger the specified action when the recovery target is reached. This option
    requires defining a target along with one of these actions. The possible values are:

    * ``pause``: Once recovery target is reached, the server is started in pause state,
      allowing users to inspect the instance
    * ``promote``: Once recovery target is reached, the server will exit recovery and is
      promoted as a master.
    * ``shutdown``: Once recovery target is reached, the server is shut down.

``--target-immediate``
    Recover is completed when a consistent state is reached (end of the base backup).

``--target-lsn``
    Recover to the specified LSN (Log Sequence Number). Requires Postgres 10 or above.
    
``--target-name``
    Recover to the specified name of a restore point previously created with the
    ``pg_create_restore_point(name)``.

``--target-time``
    Recover to the specified time. Use the format ``YYYY-MM-DD HH:MM:SS.mmm``.

``--target-tli``
    Recover the specified timeline. You can use the special values ``current`` and
    ``latest`` in addition to a numeric timeline ID. For Postgres versions 12 and above,
    the default is to recover to the latest timeline in the WAL archive. For Postgres
    versions below 12, the default is to recover to the timeline that was current at the
    time the backup was taken.

``--target-xid``
    Recover to the specified transaction ID.

.. only:: man

    Shortcuts
    ^^^^^^^^^

    Use shortcuts instead of ``BACKUP_ID``.
    
    .. list-table::
        :widths: 25 100
        :header-rows: 1
    
        * - **Shortcut**
          - **Description**
        * - **first/oldest**
          - Oldest available backup for the server, in chronological order.
        * - **last/latest**
          - Most recent available backup for the server, in chronological order.
        * - **last-full/latest-full**
          - Most recent full backup eligible for a block-level incremental backup using the
            ``--incremental`` option.
        * - **last-failed**
          - Most recent backup that failed, in chronological order.