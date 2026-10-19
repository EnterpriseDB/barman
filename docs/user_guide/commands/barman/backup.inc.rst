.. _commands-barman-backup:

``barman backup``
"""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    backup 
        [ --bwlimit KBPS ]
        [ --cloud-staging-directory PATH ]
        [ --cloud-staging-max-size SIZE ]
        [ { -h | --help } ]
        [ --incremental BACKUP_ID ]
        [ --immediate-checkpoint ]
        [ { -j | --jobs } PARALLEL_WORKERS ]
        [ --jobs-start-batch-period PERIOD ]
        [ --jobs-start-batch-size SIZE ]
        [ --keepalive-interval SECONDS ]
        [ --manifest ]
        [ --name NAME ]
        [ --no-immediate-checkpoint ]
        [ --no-manifest ]
        [ --no-retry ]
        [ --retry-sleep SECONDS ]
        [ --retry-times NUMBER ]
        [ --reuse-backup { off | copy | link } ]
        [ { --wait | -w } ]
        [ --wait-timeout SECONDS ]
        SERVER_NAME [ SERVER_NAME ... ]

Description
^^^^^^^^^^^

Execute a PostreSQL server backup. Barman will use the parameters specified in the Global
and Server configuration files. Specify ``all`` shortcut instead of the server name to
execute backups from all servers configured in the Barman node. You can also specify
multiple server names in sequence to execute backups for specific servers.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``--bwlimit``
    Specify the maximum transfer rate in kilobytes per second. A value of 0 indicates no
    limit. This setting overrides the ``bandwidth_limit`` configuration option.

``--cloud-staging-directory``
    A staging directory for when sending backups to a cloud object storage using
    ``backup_method = postgres``. It is used as a temporary location for storing chunks
    of the backup before they are sent to the cloud. Defaults to
    ``/tmp/barman/cloud-staging``.

``--cloud-staging-max-size``
    The maximum size that ``--cloud-staging-directory`` can grow to before Barman stops
    generating new backup chunks. This is used to prevent the staging directory from
    growing in case the speed of uploading chunks does not keep up with the speed of
    streaming them from Postgres. The default value is ``30Gi``.

    Extremely low values are discouraged as they may lead to performance degradation.
    We recommend a minimum of at least ``10Gi``. The optimal value will depend on the
    transfer speed of the backup to Barman and from Barman to the cloud, as well as the
    size of the backup.

    The accepted format is ``n{k|Ki|M|Mi|G|Gi|T|Ti}`` and case-sensitive, where ``n``
    is an integer greater than zero, with an optional SI or IEC suffix. k stands for
    kilo with k = 1000, while Ki stands for kilobytes Ki = 1024. The rest of the options
    have the same reasoning for greater units of measure.

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--incremental``
    Execute a block-level incremental backup. You must provide a ``BACKUP_ID`` or a
    shortcut to a previous backup, which will serve as the parent backup for the
    incremental backup.
    
    .. note::
        The backup to be and the parent backup must have ``backup_method=postgres``.
    
``--immediate-checkpoint``
    Forces the initial checkpoint to be executed as soon as possible, overriding any
    value set for the ``immediate_checkpoint`` parameter in the configuration file.

``-j`` / ``--jobs``
    Specify the number of parallel workers to use for copying files during the backup.
    This setting overrides the ``parallel_jobs`` parameter if it's specified in the
    configuration file.

``--jobs-start-batch-period``
    Specify the time period, in seconds, for starting a single batch of jobs. This value
    overrides the ``parallel_jobs_start_batch_period`` parameter if it is set in the
    configuration file. The default is ``1`` second.

``--jobs-start-batch-size``
    Specify the maximum number of parallel workers to initiate in a single batch. This
    value overrides the ``parallel_jobs_start_batch_size`` parameter if it is defined in
    the configuration file. The default is ``10`` workers.

``--keepalive-interval``
    Specify an interval, in seconds, for sending a heartbeat query to the server to keep
    the libpq connection active during a Rsync backup. The default is ``60`` seconds. A
    value of ``0`` disables the heartbeat.

``--manifest``
    Forces the creation of a backup manifest file upon completing a backup. Overrides the
    ``autogenerate_manifest`` parameter from the configuration file. Applicable only to
    rsync backup strategy.

``--name``
    Specify a friendly name for this backup which can be used in place of the backup ID
    in barman commands. The name resolves to the backup regardless of its status. If more
    than one backup has the same name, commands fail and list the matching backup IDs.

``--no-immediate-checkpoint``
    Forces the backup to wait for the checkpoint to be executed overriding any value set
    for the ``immediate_checkpoint`` parameter in the configuration file.

``--no-manifest``
    Disables the automatic creation of a backup manifest file upon completing a backup.
    This setting overrides the ``autogenerate_manifest`` parameter from the configuration
    file and applies only to rsync backup strategy.

``--no-retry``
    There will be no retry in case of an error. It is the same as setting
    ``--retry-times 0``.

``--retry-sleep``
    Specify the number of seconds to wait after a failed copy before retrying. This
    setting applies to both backup and recovery operations and overrides the
    ``basebackup_retry_sleep`` parameter if it is defined in the configuration file.

``--retry-times``
    Specify the number of times to retry the base backup copy in case of an error. This
    applies to both backup and recovery operations and overrides the
    ``basebackup_retry_times`` parameter if it is set in the configuration file.

``--reuse-backup``
    Overrides the behavior of the ``reuse_backup`` option configured in the configuration
    file. The possible values are:

    * ``off``: Do not reuse the last available backup.
    * ``copy``: Reuse the last available backup for a server and create copies of
      unchanged files (reduces backup time).
    * ``link`` (default): Reuse the last available backup for a server and create
      hard links to unchanged files (saves both backup time and space).

    .. note::
        This will only have any effect if the last available backup was
        executed with ``backup_method=rsync``.

``--wait`` / ``-w``
    Wait for all necessary WAL files required by the base backup to be archived.

``--wait-timeout``
    Specify the duration, in seconds, to wait for the required WAL files to be archived
    before timing out.

.. only:: man

    Shortcuts
    ^^^^^^^^^

    Use shortcuts instead of ``SERVER_NAME``.

    .. list-table::
        :widths: 25 100
        :header-rows: 1
    
        * - **Shortcut**
          - **Description**
        * - **all**
          - All available servers
