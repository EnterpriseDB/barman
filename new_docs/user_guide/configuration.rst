.. _configuration:

Configuration Reference
=======================

Barman follows a `convention over configuration` approach, which simplifies configuration
by allowing some options to be defined globally and overridden at the server level. This
means you can set a default behavior for your servers and then customize specific servers
as needed. This design reduces the need for excessive configuration while maintaining
flexibility.

.. _configuration-usage:

Usage
-----

Proper configuration is critical for its effective operation. Barman uses different types
of configuration files to manage global settings, server-specific settings, and
model-specific settings that comprises three scopes:
 
1. **Global Configuration**: It comprises of one file with a set of configurations for the
barman system, such as the main directory, system user, log file, and other general
options. Default location is ``/etc/barman.conf`` and it can be overridden on a per-user
level by ``~/.barman.conf`` or by specifying a ``.conf`` file using the ``-c`` /
``--config`` with the :ref:`barman command <commands-barman>` directly in the CLI.

2. **Server Configuration**: It comprises of one or multiple files with a set of
configurations for a PostgreSQL server that you want to keep track and interact for
backup, recovery and/or replication. Default location is ``/etc/barman.d`` and must use
the ``.conf`` suffix. You may have one or multiple files for servers. You can override the
default location by setting the ``configuration_files_directory`` option in the global
configuration file and placing the files in that particular location.

3. **Model Configuration**: It comprises of one or multiple files with a set of
configurations overrides that can be applied to Barman servers within the same cluster as
the model. These overrides can be implemented using the barman ``config-switch`` command.
Default location is ``/etc/barman.d`` and must use the ``.conf`` suffix. The same
``configuration_files_directory`` override option from the server configuration applies for
models. You may have one or multiple files for models.

.. note::
  Historically, you could have a single configuration file containing global, server, and
  model options, but, for maintenance reasons, this approach is deprecated.

Configuration files follow the ``INI`` format and consist of sections denoted by headers
in square brackets. Each section can include various options.

Models and servers must have unique identifiers, and reserved words cannot be used as
names.

**Reserved Words**

The following reserved words cannot be used as server or model names:

* ``barman``: Identifies the global section.
* ``all``: A special shortcut for executing commands on all managed servers.

**Parameter Types**

Configuration options can be of the following types:

* **String**: Textual data (e.g., file paths, names).
* **Enum**: Enumerated values, often limited to predefined choices.
* **Integer**: Numeric values.
* **Boolean**: Can be ``on``, ``true``, ``1`` (true) or ``off``, ``false``, ``0`` 
  (false).

  .. note::
    Some enums allow ``off``, but not ``false``.

.. _configuration-options:

Options
-------

Options in the configuration files can have specific or shared scopes. The following
configuration options are used not only for configuring how Barman will execute backups
and recoveries, but also for configuring various aspects of how Barman interacts with the
configured PostgreSQL servers to be able to apply your Backup and Recovery, and
High-Availability strategies.

.. _configuration-options-general:

General
"""""""

These are general configurations options.

**active**

When this option is set to ``true`` (default), the server operates fully. If set to
``false``, the server is restricted to diagnostic use only, meaning that operational
commands such as backup execution or WAL archiving are temporarily disabled. When
incorporating a new server into Barman, we recommend initially setting
``active=false``. Verify that barman check shows no issues before activating the
server. This approach helps prevent excessive error logging in Barman during the
initial setup.

Scope: Server / Model.

**archiver**

This option enables log file shipping through Postgres' ``archive_command`` for a
server. When set to ``true``, Barman expects continuous archiving to be configured and
will manage WAL files that Postgres stores in the incoming directory
(``incoming_wals_directory``), including their checks, handling, and compression. When
set to ``false`` (default), continuous archiving is disabled. 
  
.. note:: 
  If neither ``archiver`` nor ``streaming_archiver`` is configured, Barman will
  automatically set this option to ``true`` to maintain compatibility with the
  previous default behavior where archiving was enabled by default.

Scope: Global / Server / Model.

**archiver_batch_size**

This option enables batch processing of WAL files for the archiver process by setting
it to a value greater than ``0``. If not set, the archiver will use unlimited
(default) processing mode for the WAL queue. With batch processing enabled, the
archiver process will handle a maximum of ``archiver_batch_size`` WAL segments per
run. This value must be an integer.

Scope: Global / Server / Model.

**bandwidth_limit**

Specifies the maximum transfer rate in kilobytes per second for backup and recovery
operations. A value of ``0`` indicates no limit (default).

.. note::
  Applies only when ``backup_method = postgres | rsync``.

Scope: Global / Server / Model.

**barman_home**

Designates the main data directory for Barman. Defaults to ``/var/lib/barman``.

Scope: Global.

**barman_lock_directory**

Specifies the directory for lock files. The default is ``barman_home``.

.. note::
  The ``barman_lock_directory`` should be on a non-network local filesystem.

Scope: Global.

**basebackup_retry_sleep**

Sets the number of seconds to wait after a failed base backup copy before retrying.
Default is ``30`` seconds. Must be a non-negative integer.

.. note::
  This applies to both backup and recovery operations.

Scope: Global / Server / Model.

**basebackup_retry_times**

Defines the number of retry attempts for a base backup copy after an error occurs.
Default is ``0`` (no retries). Must be a non-negative integer.

.. note::
  This applies to both backup and recovery operations.

Scope: Global / Server / Model.

**check_timeout**

Sets the maximum execution time in seconds for a Barman check command per server. Set
to ``0`` to disable the timeout. Default is ``30`` seconds. Must be a non-negative
integer.

Scope: Global / Server / Model.

**cluster**

Tag the server or model to an associated cluster name. Barman uses this association to
override configuration for all servers/models in this cluster. If omitted for servers,
it defaults to the server's name.

.. note::
  Must be specified for configuration models to group applicable servers.

Scope: Server / Model.

**config_changes_queue**

Designates the filesystem location for Barman's queue that handles configuration changes
requested via the barman ``config-update`` command. This queue manages the
serialization and retry of configuration change requests. By default, Barman writes to
a file named ``cfg_changes.queue`` under ``barman_home``.

Scope: Global.

**configuration_files_directory**

Designates the directory where server/model configuration files will be read by Barman.
Defaults to ``/etc/barman.d``.

Scope: Global.

**conninfo**

Specifies the connection string used by Barman to connect to the PostgreSQL server.
This is a libpq connection string. Commonly used keys include: ``host``, ``hostaddr``,
``port``, ``dbname``, ``user`` and ``password``. See the `PostgreSQL documentation <https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_
for details.

Scope: Server / Model.

**create_slot**

Determines whether Barman should automatically create a replication slot, if it's not
already present, for streaming of WAL files. When set to ``auto`` and ``slot_name`` 
is defined, Barman will attempt to create the slot automatically. When set to 
``manual`` (default), the replication slot must be created manually.

Scope: Global / Server / Model.

**description**

Provides a human-readable description of a server.

Scope: Server / Model.

**errors_directory**

The directory where WAL files that were errored while being archived by Barman are 
stored. This includes duplicate WAL files (e.g., an archived WAL file that has already
been streamed) and unexpected files found in the WAL archive directory.

The purpose of placing the files in this directory is so someone can later review why they 
failed to be archived and take appropriate actions (dispose of, store somewhere else, 
replace the duplicate file archived before, etc.)

Scope: Server.

**forward_config_path**

Determines whether a passive node should forward its configuration file path to its
primary node during ``cron`` or ``sync-info`` commands. Set to ``true`` if Barman is
invoked with the ``-c`` / ``--config`` option and the configuration paths are identical
on both passive and primary Barman servers. Defaults to ``false``.

Scope: Global / Server / Model.

**immediate_checkpoint**

Controls how Postgres handles checkpoints at the start of a backup. Set to ``false``
(default) to allow the checkpoint to complete according to
``checkpoint_completion_target``. Set to ``true`` for an immediate checkpoint, where
Postgres completes the checkpoint as quickly as possible.

Scope: Global / Server / Model.

**keepalive_interval**

Sets the interval in seconds for sending a heartbeat query to keep the libpq
connection active during an rsync backup. Default is ``60`` seconds. Setting this to
``0`` disables the heartbeat.

Scope: Global / Server / Model.

**lock_directory_cleanup**

Enables automatic cleanup of unused lock files in the ``barman_lock_directory``.

Scope: Global.

**log_file**

Specifies the location of Barman's log file. Defaults to ``/var/log/barman/barman.log``.

Scope: Global.

**log_level**

Sets the level of logging. Options include: ``DEBUG``, ``INFO``, ``WARNING``,
``ERROR`` and ``CRITICAL``.

Scope: Global.

**minimum_redundancy**

Specifies the minimum number of backups to retain. Default is ``0``.

Scope: Global / Server / Model.

**model**

When set to ``true``, turns a server section from a configuration file into a model for
a cluster. There is no ``false`` option in this case. If you want to simulate a 
``false`` option, comment out (``#model=true``) or remove the option in the
configuration. Defaults to the server name.

Scope: Model.

**network_compression**

Enables or disables data compression for network transfers. Set to ``false`` (default)
to disable compression, or ``true`` to enable it and reduce network usage.

Scope: Global / Server / Model.

.. _configuration-parallel-jobs:

**parallel_jobs**

Controls the number of parallel workers used to copy files during backup or recovery.
Default is ``1``.

.. note::
  Applies only when ``backup_method = rsync``.

Scope: Global / Server / Model.

**parallel_jobs_start_batch_period**

Specifies the time interval in seconds over which a single batch of parallel jobs will
start. Default is ``1`` second. This means that if ``parallel_jobs_start_batch_size``
is ``10`` and ``parallel_jobs_start_batch_period`` is ``1``, this will yield an
effective rate limit of ``10`` jobs per second, because there is a maximum of ``10``
jobs that can be started within ``1`` second.

.. note::
  Applies only when ``backup_method = rsync``.

Scope: Global / Server / Model.

**parallel_jobs_start_batch_size**

Defines the maximum number of parallel jobs to start in a single batch. Default is
``10`` jobs. This means that if ``parallel_jobs_start_batch_size``
is ``10`` and ``parallel_jobs_start_batch_period`` is ``2``, this will yield a maximum
of ``10`` jobs that can be started within ``2`` seconds.

.. note::
  Applies only when ``backup_method = rsync``.

Scope: Global / Server / Model.

**path_prefix**

Lists one or more absolute paths, separated by colons, where Barman looks for
executable files. These paths are checked before the PATH environment variable. This
option can be set for each server and needs to point to the ``bin`` directory for the
appropriate ``PG_MAJOR_VERSION``.

Scope: Global / Server / Model.

**primary_checkpoint_timeout**

Time to wait for new WAL files before forcing a checkpoint on the primary server.
Defaults to ``0``.

Scope: Server / Model.

**primary_conninfo**

Connection string for Barman to connect to the primary PostgreSQL server during a
standby backup.

Scope: Server / Model.

**primary_ssh_command**

SSH command for connecting to the primary server if Barman is passive.

Scope: Global / Server / Model.

**slot_name**

Replication slot name for the ``receive-wal`` command when ``streaming_archiver`` is
enabled. 

Scope: Global / Server / Model.

**ssh_command**

SSH command used by Barman to connect to the PostreSQL server.

Scope: Server / Model.

**streaming_archiver**

Enables Postgres' streaming protocol for WAL files. Defaults to ``false``.

.. note:: 
  If neither ``archiver`` nor ``streaming_archiver`` is configured, Barman will
  automatically set ``archiver`` option to ``true`` to maintain compatibility with the
  previous default behavior where archiving was enabled by default.

Scope: Global / Server / Model.

**streaming_archiver_batch_size**

Batch size for processing WAL files in streaming archiver. Defaults to ``0``.

Scope: Global / Server / Model.

**streaming_archiver_name**

Application name for the ``receive-wal`` command. Defaults to ``barman_receive_wal``.

Scope: Global / Server / Model.

**streaming_backup_name**

Application name for the ``pg_basebackup`` command. Defaults to
``barman_streaming_backup``.

Scope: Global / Server / Model.

**streaming_conninfo**

Connection string for streaming replication protocol. Defaults to ``conninfo``.

Scope: Server / Model.

**tablespace_bandwidth_limit**

Maximum transfer rate for specific tablespaces for backup and recovery operations.
A value of ``0`` indicates no limit (default).

.. note::
  Applies only when ``backup_method = rsync``.

Scope: Global / Server / Model.

.. _configuration-options-backups:

Backups
"""""""

These configurations options are related to how Barman will execute backups.

**autogenerate_manifest**

This is a boolean option that allows for the automatic creation of backup manifest
files. The manifest file, which is a JSON document, lists all files included in the
backup. It is generated upon completion of the backup and saved in the backup
directory. The format of the manifest file adheres to the specifications outlined in the
`PostgreSQL documentation <https://www.postgresql.org/docs/current/backup-manifest-format.html>`_
and is compatible with the ``pg_verifybackup`` tool. Default is ``false``.

.. note::
  This option is ignored if the ``backup_method`` is not ``rsync``.
  
Scope: Global / Server / Model.

**backup_compression**

Specifies the compression method for the backup process. It can be set to ``gzip``,
``lz4``, ``zstd``, or ``none``. Ensure that the CLI tool for the chosen compression
method is available on both the Barman and PostgreSQL servers. 
  
.. note::
  Note that ``lz4`` and ``zstd`` require Postgres version 15 or later. Unsetting this
  option or using ``none`` results in an uncompressed archive (default). Only
  supported when ``backup_method = postgres``.

Scope: Global / Server / Model.

**backup_compression_format**

Determines the format ``pg_basebackup`` should use when saving compressed backups.
Options are ``plain`` or ``tar``, with ``tar`` as the default if unset. The ``plain``
format is available only if Postgres version 15 or later is in use and
``backup_compression_location`` is set to ``server``.
  
.. note::
  Only supported when ``backup_method = postgres``.

Scope: Global / Server / Model.

**backup_compression_level**

Defines the level of compression for backups as an integer. The permissible values
depend on the compression method specified in ``backup_compression``.
  
.. note::
  Only supported when ``backup_method = postgres``.

Scope: Global / Server / Model.

**backup_compression_location**

Specifies where compression should occur during the backup: either ``client`` or
``server``. The ``server`` option is available only if Postgres version 15 or later is
being used.

.. note::
  Only supported when ``backup_method = postgres``.

Scope: Global / Server / Model.

**backup_compression_workers**

Sets the number of threads used for compression during the backup process. This is
applicable only when ``backup_compression=zstd``. The default value is 0, which uses
the standard compression behavior.

.. note::
  Only supported when ``backup_method = postgres``.

Scope: Global / Server / Model.

**backup_directory**

Specifies the directory where backup data for a server will be stored. Defaults to
``<barman_home>/<server_name>``.

Scope: Server.

**backup_method**

Defines the method Barman uses to perform backups. Options include:

* ``rsync`` (default): Executes backups using the rsync command over SSH (requires
  ``ssh_command``).
* ``postgres``: Uses the ``pg_basebackup`` command for backups.
* ``local-rsync``: Assumes Barman runs on the same server and as the same user as
  the PostgreSQL database, performing an rsync file system copy.
* ``snapshot``: Utilizes the API of the cloud provider specified in the
  ``snapshot_provider`` option to create disk snapshots as defined in
  ``snapshot_disks`` and saves only the backup label and metadata to its own
  storage.

Scope: Global / Server / Model.

**backup_options**

Controls how Barman interacts with Postgres during backups. This is a comma-separated
list that can include:

* ``concurrent_backup`` (default): Uses concurrent backup, recommended for
  Postgres versions 9.6 and later, and supports backups from standby servers.
* ``exclusive_backup``: Uses the deprecated exclusive backup method. Only for Postgres 
  versions older than 15.
* ``external_configuration``: Suppresses warnings about external configuration files
  during backup execution.

.. note::
  ``exclusive_backup`` and ``concurrent_backup`` cannot be used together.

Scope: Global / Server / Model.

**basebackups_directory**

Specifies the directory where base backups are stored. Defaults to
``<backup_directory>/base``.

Scope: Server.

**reuse_backup**

Controls incremental backup support when using ``backup_method=rsync`` by reusing the
last available backup. The options are:

* ``off`` (default): Standard full backup.
* ``copy``: File-level incremental backup, by reusing the last backup for a server and
  creating a copy of the unchanged files (just for backup time reduction)
* ``link``: File-level incremental backup, by reusing the last backup for a server and
  creating a hard link of the unchanged files (for backup space and time reduction)

.. note::
  This option will be ignored when ``backup_method=postgres``.

Scope: Global / Server / Model.

.. _configuration-options-cloud-backups:

Cloud Backups
"""""""""""""

These configuration options are related to how Barman will execute backups in the cloud.

**aws_await_snapshots_timeout**

Specifies the duration in seconds to wait for AWS snapshots to be created before a
timeout occurs. The default value is ``3600`` seconds. This must be a positive
integer.

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = aws``.

Scope: Global / Server / Model.

**aws_profile**

The name of the AWS profile to use when authenticating with AWS (e.g. ``INI`` section
in AWS credentials file).

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = aws``.

Scope: Global / Server / Model.

**aws_region**

Indicates the AWS region where the EC2 VM and storage volumes, as defined by
``snapshot_instance`` and ``snapshot_disks``, are located.

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = aws``.

Scope: Global / Server / Model.

**azure_credential**

Specifies the type of Azure credential to use for authentication, either ``azure-cli``
or ``managed-identity``. If not provided, the default Azure authentication method will
be used.

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = azure``.

Scope: Global / Server / Model.

**azure_resource_group**

Specifies the name of the Azure resource group containing the compute instance and
disks defined by ``snapshot_instance`` and ``snapshot_disks``.

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = azure``.

Scope: Global / Server / Model.

**azure_subscription_id**

Identifies the Azure subscription that owns the instance and storage volumes defined by
``snapshot_instance`` and ``snapshot_disks``.

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = azure``.

Scope: Global / Server / Model.

**gcp_project**

Specifies the ID of the GCP project that owns the instance and storage volumes defined
by ``snapshot_instance`` and ``snapshot_disks``.

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = gcp``.

Scope: Global / Server / Model.

**gcp_zone**

Indicates the availability zone where the compute instance and disks are located for
snapshot backups.

.. note::
  Only supported when ``backup_method = snapshot`` and ``snapshot_provider = gcp``.

Scope: Server / Model.

**snapshot_disks**

This option is a comma-separated list of disks to include in cloud snapshot backups.
  
.. note::
  Required when ``backup_method = snapshot``.

  Ensure that the ``snapshot_disks`` list includes all disks that store Postgres data,
  as any data not on these listed disks will not be included in the backup and will be
  unavailable during recovery.

Scope: Server / Model.

**snapshot_instance**

The name of the VM or compute instance where the storage volumes are attached.
  
.. note::
  Required when ``backup_method = snapshot``.

Scope: Server / Model.

**snapshot_provider**

The name of the cloud provider to use for creating snapshots. Supported value:
``aws``, ``azure`` and ``gcp``.
  
.. note::
  Required when ``backup_method = snapshot``.

Scope: Global / Server / Model.

.. _configuration-options-hook-scripts:

Hook Scripts
""""""""""""

These configuration options are related to the pre or post execution of hook scripts.

**post_archive_retry_script**

Specifies a hook script to run after a WAL file is archived. Barman will retry this
script until it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or ``ABORT_STOP``
(63). In a post-archive scenario, ``ABORT_STOP`` has the same effect as
``ABORT_CONTINUE``.

Scope: Global / Server.

**post_archive_script**

Specifies a hook script to run after a WAL file is archived, following the
``post_archive_retry_script``.

Scope: Global / Server.

**post_backup_retry_script**

Specifies a hook script to run after a base backup. Barman will retry this script until
it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or ``ABORT_STOP`` (63). In a
post-backup scenario, ``ABORT_STOP`` has the same effect as ``ABORT_CONTINUE``.

Scope: Global / Server.

**post_backup_script**

Specifies a hook script to run after a base backup, following the
``post_backup_retry_script``.

Scope: Global / Server.

**post_delete_retry_script**

Specifies a hook script to run after deleting a backup. Barman will retry this script
until it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or ``ABORT_STOP`` (63). In
a post-delete scenario, ``ABORT_STOP`` has the same effect as ``ABORT_CONTINUE``.

Scope: Global / Server.

**post_delete_script**

Specifies a hook script to run after deleting a backup, following the
``post_delete_retry_script``.

Scope: Global / Server.

**post_recovery_retry_script**

Specifies a hook script to run after a recovery. Barman will retry this script until it
returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or ``ABORT_STOP`` (63). In a
post-recovery scenario, ``ABORT_STOP`` has the same effect as ``ABORT_CONTINUE``.

Scope: Global / Server.

**post_recovery_script**

Specifies a hook script to run after a recovery, following the
``post_recovery_retry_script``.

Scope: Global / Server.

**post_wal_delete_retry_script**

Specifies a hook script to run after deleting a WAL file. Barman will retry this script
until it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or ``ABORT_STOP`` (63). In
a post-WAL-delete scenario, ``ABORT_STOP`` has the same effect as ``ABORT_CONTINUE``.

Scope: Global / Server.

**post_wal_delete_script**

Specifies a hook script to run after deleting a WAL file, following the
``post_wal_delete_retry_script``.

Scope: Global / Server.

**pre_archive_retry_script**

Specifies a hook script that runs before a WAL file is archived during maintenance,
following the ``pre_archive_script``. As a retry hook script, Barman will repeatedly
execute the script until it returns either ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62),
or ``ABORT_STOP`` (63). Returning ``ABORT_STOP`` will escalate the failure and halt
the WAL archiving process.

Scope: Global / Server.

**pre_archive_script**

Specifies a hook script launched before a WAL file is archived by maintenance.

Scope: Global / Server.

**pre_backup_retry_script**

Specifies a hook script that runs before a base backup, following the
``pre_backup_script``. As a retry hook script, Barman will attempt to execute the
script repeatedly until it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or
``ABORT_STOP`` (63). Returning ``ABORT_STOP`` will escalate the failure and interrupt
the backup process.

Scope: Global / Server.

**pre_backup_script**

Specifies a hook script to run before starting a base backup.

Scope: Global / Server.

**pre_delete_retry_script**

Specifies a retry hook script to run before backup deletion, following the
``pre_delete_script``. As a retry hook script, Barman will attempt to execute the
script repeatedly until it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or
``ABORT_STOP`` (63). Returning ``ABORT_STOP`` will escalate the failure and interrupt
the backup deletion.

Scope: Global / Server.

**pre_delete_script**

Specifies a hook script run before deleting a backup.

Scope: Global / Server.

**pre_recovery_retry_script**

Specifies a retry hook script to run before recovery, following the
``pre_recovery_script``. As a retry hook script, Barman will attempt to execute the
script repeatedly until it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or
``ABORT_STOP`` (63). Returning ``ABORT_STOP`` will escalate the failure and interrupt
the recover process.

Scope: Global / Server.

**pre_recovery_script**

Specifies a hook script run before starting a recovery.

Scope: Global / Server.

**pre_wal_delete_retry_script**

Specifies a retry hook script for WAL file deletion, executed before
``pre_wal_delete_script``. As a retry hook script, Barman will attempt to execute the
script repeatedly until it returns ``SUCCESS`` (0), ``ABORT_CONTINUE`` (62), or
``ABORT_STOP`` (63). Returning ``ABORT_STOP`` will escalate the failure and interrupt
the WAL file deletion.

Scope: Global / Server.

**pre_wal_delete_script**

Specifies a hook script run before deleting a WAL file.

Scope: Global / Server.

.. _configuration-options-wals:

Write-Ahead Logs (WAL)
""""""""""""""""""""""

These configuration options are related to how Barman will manage the Write-Ahead Logs
(WALs) of the PostreSQL servers.

**compression**

Specifies the standard compression algorithm for WAL files. Options include: ``gzip``,
``bzip2``, ``pigz``, ``pygzip``, ``pybzip2`` and ``custom``. 
  
.. note::
  All of these options require the module to be installed in the location where the
  compression will occur.

  The ``custom`` option is for custom compression, which requires you to set the
  following options as well:

  * ``custom_compression_filter``: a compression filter.
  * ``custom_decompression_filter``: a decompression filter
  * ``custom_compression_magic``: a hex string to identify a custom compressed wal
    file.

Scope: Global / Server / Model.

**custom_compression_filter**

Specifies a custom compression algorithm for WAL files. It must be a ``string`` that
will be used internally to create a bash command and it will prefix to the
following string ``> "$2" < "$1";``. Write to standard output and do not delete input
files.

.. tip::
  ``custom_compression_filter = "xz -c"``

  This is the same as running ``xz -c > "$2" < "$1";``.

Scope: Global / Server / Model.

**custom_compression_magic**

Defines a custom magic value to identify the custom compression algorithm used in WAL
files. If this is set, Barman will avoid applying custom compression to WALs that have
already been compressed with the specified algorithm. If not configured, Barman will
apply custom compression to all WAL files, even those pre-compressed.

.. tip::
  For example, in the ``xz`` compression algorithm, the magic number is used to detect
  the format of ``.xz`` files.

  For xz files, the magic number is the following sequence of bytes:
    Magic Number: ``FD 37 7A 58 5A 00``

  In hexadecimal representation, this can be expressed as:
    Hex String: ``fd377a585a00``

  Reference: `xz-file-format <https://tukaani.org/xz/xz-file-format-1.0.4.txt>`_

Scope: Global / Server / Model.

**custom_decompression_filter**

Specifies a custom decompression algorithm for compressed WAL files. It must be a
``string`` that will be used internally to create a bash command and it will
prefix to the following string ``> "$2" < "$1";``. It must correspond with the
compression algorithm used.

.. tip::
  ``custom_compression_filter = "xz -c -d"``

  This is the same as running ``xz -c -d > "$2" < "$1";``.

Scope: Global / Server / Model.

**incoming_wals_directory**

Specifies the directory where incoming WAL files are archived. Requires ``archiver`` to
be enabled. Defaults to ``<backup_directory>/incoming``.

Scope: Server.

**last_wal_maximum_age**

Defines the time frame within which the latest archived WAL file must fall. If the
latest WAL file is older than this period, the barman check command will report an
error. If left empty (default), the age of the WAL files is not checked. Format is the
same as ``last_backup_maximum_age``.

Scope: Global / Server / Model.

**max_incoming_wals_queue**

Defines the maximum number of WAL files allowed in the incoming queue (including both
streaming and archiving pools) before the barman check command returns an error.
Default is ``None`` (disabled).

Scope: Global / Server / Model.

**streaming_wals_directory**

Directory for streaming WAL files. Defaults to ``<backup_directory>/streaming``.

.. note::
  This option is applicable when ``streaming_archiver`` is activated.
  
Scope: Server.

**wal_conninfo**

This optional connection string is used by Barman for monitoring the status of the
replication slot used for receiving WALs. When specified, it takes precedence over
``wal_streaming_conninfo`` for these checks. If ``wal_conninfo`` is set, but
``wal_streaming_conninfo`` is not, ``wal_conninfo`` will be ignored. Both connection
strings must access a Postgres instance within the same cluster as defined by
``streaming_conninfo`` and ``conninfo``. Additionally, ``wal_streaming_conninfo`` must
support streaming replication connections, and either it or ``wal_conninfo`` (if used)
must have the necessary permissions to read settings and check replication slot
status, such as the ``pg_monitor`` role, both ``pg_read_all_settings`` and
``pg_read_all_stats`` roles, or ``superuser`` privileges.

Scope: Server / Model.

**wal_streaming_conninfo**

This connection string is used by Barman to connect to the PostgreSQL server for
receiving WAL segments via streaming replication and for checking the replication slot
status. If not specified, Barman defaults to using ``streaming_conninfo`` for these
tasks. ``wal_streaming_conninfo`` must connect to a Postgres instance within the
same cluster as defined by ``streaming_conninfo`` and ``conninfo``, and it must support
streaming replication. It, or the optional ``wal_conninfo``, must also have the
required permissions to read settings and check the replication slot status, such as
the ``pg_monitor`` role, both ``pg_read_all_settings`` and ``pg_read_all_stats``
roles, or ``superuser`` privileges.

Scope: Server / Model.

**wals_directory**

Directory containing WAL files. Defaults to ``<backup_directory>/wals``.

Scope: Server.

.. _configuration-options-restore:

Restore
"""""""

These configuration options are related to how Barman manages restoration backups.

**local_staging_path**

Specifies the local path for combining block-level incremental backups during recovery.
This location must have sufficient space to temporarily store the new synthetic backup.
Required for recovery from a block-level incremental backup.

.. note::
  Applies only when ``backup_method = postgres``.

Scope: Global / Server / Model.

**recovery_options**

Options for recovery operations. Currently, only ``get-wal`` is supported. This option
enables the creation of a basic ``restore_command`` in the recovery configuration,
which uses the barman ``get-wal`` command to retrieve WAL files directly from Barman's
WAL archive. This setting accepts a comma-separated list of values and defaults to
empty.

Scope: Global / Server / Model.

**recovery_staging_path**

Specifies the path on the recovery host for staging files from compressed backups. This
location must have sufficient space to temporarily store the compressed backup.

.. note::
  Applies only for commpressed backups.

Scope: Global / Server / Model.

.. _configuration-options-retention-policies:

Retention Policies
""""""""""""""""""

These configuration options are related to how Barman manages retention policies of the
backups.

**last_backup_maximum_age**

Defines the time frame within which the latest backup must fall. If the latest backup
is older than this period, the barman check command will report an error. If left
empty (default), the latest backup is always considered valid. The accepted format is
``"n {DAYS|WEEKS|MONTHS}"``, where ``n`` is an integer greater than zero.

Scope: Global / Server / Model.

**last_backup_minimum_size**

Specifies the minimum acceptable size for the latest successful backup. If the latest
backup is smaller than this size, the barman check command will report an error. If
left empty (default), the latest backup is always considered valid. The accepted
format is ``"n {k|Ki|M|Mi|G|Gi|T|Ti}"`` and case-sensitive, where ``n`` is an integer
greater than zero, with an optional SI or IEC suffix. k stands for kilo with k = 1000,
while Ki stands for kilobytes Ki = 1024. The rest of the options have the same
reasoning for greater units of measure.

Scope: Global / Server / Model.

**retention_policy**

Defines how long backups and WAL files should be retained. If this option is left blank,
no retention policies will be applied. Options include redundancy and recovery window
policies. 
  
.. code-block:: text

  retention_policy = {REDUNDANCY value | RECOVERY WINDOW OF value {DAYS | WEEKS | MONTHS}}

* ``retention_policy = REDUNDANCY 2`` will keep only 2 backups in the backup catalog
  automatically deleting the older one as new backups are created. The number must be
  a positive integer.
* ``retention_policy = RECOVERY WINDOW OF 2 DAYS`` will only keep backups needed to
  recover to any point in time in the last two days, automatically deleting backups
  that are older. The period number must be a positive integer, and   the following
  options can be applied to it: ``DAYS``, ``WEEKS``, ``MONTHS``.

Scope: Global / Server / Model.

**retention_policy_mode**

Mode for enforcing retention policies. Currently only supports ``auto``.

Scope: Global / Server / Model.

**wal_retention_policy**

Policy for retaining WAL files. Currently only ``main`` is available.

Scope: Global / Server / Model.

.. _configuration-configuration-models:

Configuration Models
--------------------

Configuration models provide a systematic approach to manage and apply configuration
overrides for Postgres servers by organizing them under a specific ``cluster`` name.

Purpose
"""""""

The primary goal of a configuration model is to simplify the management of configuration
settings for Postgres servers grouped by the same ``cluster``. By using a model, you can
apply a set of common configuration overrides, enhancing operational efficiency. They are
especially beneficial in clustered environments, allowing you to create various
configuration models that can be utilized during failover events.

Application
"""""""""""

The configurations defined in a model file can be applied to Postgres servers that share
the same ``cluster`` name specified in the model. Consequently, any server utilizing that
model can inherit these settings, promoting a consistent and adaptable configuration
across all servers. 

Usage
"""""

Model options can only be defined within a model section, which is identified in the same
way as a server section. It is important to ensure that there are no conflicts between
the identifiers of server sections and model sections.

To apply a configuration model, execute the
``barman config-switch SERVER_NAME MODEL_NAME``. This command facilitates the application
of the model's overrides to the relevant Barman server associated with the specified
cluster name.

If you wish to remove the overrides, the deletion of the model configuration file alone
will not have any effect, so you can do so by using the ``--reset`` argument with the
command, as follows: ``barman config-switch SERVER_NAME --reset``.

.. note::
  The ``config-switch`` command will only succeed if model name exists and is associated
  with the same ``cluster`` as the server. Additionally, there can be only one active
  model at a time; if you execute the command multiple times with different models, only
  the overrides defined in the last model will be applied.

  Not all options can be configured through models. Please review the scope of the
  available configurations to determine which settings apply to models.

Benefits
""""""""

* Consistency: Ensures uniform configuration across multiple Barman servers within a
  cluster.
* Efficiency: Simplifies configuration management by allowing centralized updates and
  overrides.
* Flexibility: Allows the use of multiple model files, providing the ability to define
  various sets of overrides as necessary.

.. _configuration-examples:

.. only:: html
  
  Examples
  --------

  Barman global configurations are common between all configured servers. So if you want to
  have specific configurations, you should move it to the server scope instead of the barman
  global scope.

  Next you can find a few examples of global, servers and models configurations with an
  explanation of the fields. 

  Global Configuration
  """"""""""""""""""""

  .. code-block:: text
    :caption: **/etc/barman.conf**
    :name: /etc/barman.conf

    [barman]

    barman_home = /var/lib/barman
    barman_user = barman
    configuration_files_directory = /etc/barman.d
    log_file = /var/log/barman/barman.log
    log_level = INFO

  **barman**
    * Set configuration that will be global.
    * Configure locations for ``barman_home``, ``configuration_files_directory``,
      ``log_file``, the ``barman_user`` and the ``log_level``.

  Server Configuration - Rsync
  """"""""""""""""""""""""""""

  .. code-block:: text
    :caption: **/etc/barman.d/pg_server1_rsync.conf**
    :name: /etc/barman.d/pg_server1_rsync.conf

    [server1]

    description =  "PostgreSQL server 1"
    conninfo = host=pg1 user=barman port=5432 dbname=databasename
    ssh_command = ssh postgres@pg1
    backup_method = rsync
    reuse_backup = link
    archiver = on
    parallel_jobs = 2
    minimum_redundancy = 2
    retention_policy = REDUNDANCY 4

  **server1**
    * Connect to Postgres from Barman using the ``conninfo``.
    * ``ssh_command`` is needed to correctly create an SSH connection from the Barman
      server to the PostgreSQL server when using rsync.
    * Set the ``backup_method`` as ``rsync`` and ``reuse_backup`` to enable file-level
      incremental backups.
    * Configure the ``archiver`` option to ship WALs using the ``archive_command``
      configured in the Postgres configuration file ``postgresql.conf``.
    * Jobs will use two workers for parallel processing.
    * Set the ``minimum_redundancy`` and the ``retention_policy`` for backups created
      from this server.

  Server Configuration - pg_basebackup
  """"""""""""""""""""""""""""""""""""

  .. code-block:: text
    :caption: **/etc/barman.d/pg_server2_streaming.conf**
    :name: /etc/barman.d/pg_server2_streaming.conf

    [server2]

    description =  "PostgreSQL server 2"
    conninfo = host=pg2 user=barman port=5432 dbname=databasename
    streaming_conninfo = host=pg2 user=streaming_barman port=5432 dbname=databasename
    backup_method = postgres
    streaming_archiver = on
    slot_name = barman
    create_slot = auto
    minimum_redundancy = 5
    retention_policy = RECOVERY WINDOW OF 7 DAYS
    local_staging_path = /var/lib/barman/staging
    cluster = streaming

  **server2**
    * Connect to Postgres using the ``conninfo``. This is used to check the status
      of replication slots.
    * Connect to Postgres using the ``streaming_conninfo``. This is used to create
      ``pg_receivewal`` processes to stream WAL segments.
    * Set the ``backup_method`` as ``postgres``.
    * Configure the ``streaming_archiver`` option to ship WALs using the streaming
      replication, the ``slot_name`` that will be created in the PostgreSQL server and
      ``create_slot`` as ``auto`` so Barman can automatically attempt to create the
      replication slot if not present.
    * Set the ``minimum_redundancy`` and the ``retention_policy`` for backups created
      from this server.
    * Recovery for block-level incremental backups will use the ``local_staging_path``
      as the intermediate location to combine the chain of backups.
    * Group this server into the ``streaming`` cluster to be used by models.

  Model Configuration 1
  """""""""""""""""""""

  .. code-block:: text
    :caption: **/etc/barman.d/mdl_streaming_switchover.conf**
    :name: /etc/barman.d/mdl_streaming_switchover.conf

    [server2:switch_over_streaming_conn_to_pg3]

    cluster = streaming
    model = true
    wal_conninfo = host=pg3 user=barman port=5432 dbname=databasename
    wal_streaming_conninfo = host=pg3 user=streaming_barman port=5432 dbname=databasename
    compression = gzip
    backup_compression = gzip
    recovery_staging_path = /var/lib/barman/recovery_staging
    retention_policy = RECOVERY WINDOW OF 14 DAYS

  **server2:switch_over_wal_streaming_conn_to_pg3**
    * Tag this model to a cluster named ``streaming`` to override configurations.
    * Configure this as a model (``model = true``).
    * ``wal_conninfo`` is set, so this connection will be used specifically for monitoring
      WAL streaming status and perform checks.
    * ``wal_streaming_conninfo`` is set, Barman will use this instead of
      ``streaming_conninfo`` when receiving WAL segments via streaming replication
      protocol. If ``wal_conninfo`` was unset, this option would also be used
      to monitor and check WAL streaming replication statuses.
    * WAL files will be compressed with ``gzip``.
    * All backups will be compressed with ``gzip``.
    * Recovery for compressed backups will use the ``recovery_staging_path`` as the
      intermediate location to uncompress the backup.
    * Set a ``retention_policy`` for backups that are grouped in the ``streaming``
      cluster.

  *In this example we have setup a model that switches the streaming connection to pg3,
  enables compression of backups and WAL files and changes the retention_policy.* **This is
  a way to stream WALs and backups from different hosts.**

  The final configuration will have the following settings:

  .. code-block:: text

    [server2]

    description =  "PostgreSQL server 2"
    conninfo = host=pg2 user=barman port=5432 dbname=databasename
    streaming_conninfo = host=pg2 user=streaming_barman port=5432 dbname=databasename
    backup_method = postgres
    streaming_archiver = on
    slot_name = barman
    create_slot = auto
    minimum_redundancy = 5
    retention_policy = RECOVERY WINDOW OF 14 DAYS
    local_staging_path = /var/lib/barman/staging
    wal_conninfo = host=pg3 user=barman port=5433 dbname=databasename 
    wal_streaming_conninfo = host=pg3 user=streaming_barman port=5433 dbname=databasename
    compression = gzip
    backup_compression = gzip
    recovery_staging_path = /var/lib/barman/recovery_staging

  Model Configuration 2
  """""""""""""""""""""

  .. code-block:: text
    :caption: **/etc/barman.d/mdl_streaming_failover**
    :name: /etc/barman.d/mdl_streaming_failover

    [server2:failover_conn_to_pg3]

    cluster = streaming
    model = true
    conninfo = host=pg3 user=barman port=5433 dbname=databasename
    streaming_conninfo = host=pg3 user=streaming_barman port=5433 dbname=databasename

  **server2:failover_conn_to_pg3**
    * Tag this model to a cluster named ``streaming`` to override configurations.
    * Configure this as a model (``model = true``).
    * ``conninfo`` is set, so it will be used to switch the Postgres connection to
      host ``pg3``.
    * ``streaming_conninfo`` is set, so it will be used to switch the Postgres streaming
      connection to host ``pg3``.

  *In this example we have setup a model that switches the Postgres connection and
  streaming connection upon a failover from pg2 to pg3.*

  The final configuration will have the following settings:

  .. code-block:: text

    [server2]

    description =  "PostgreSQL server 2"
    conninfo = host=pg3 user=barman port=5432 dbname=databasename
    streaming_conninfo = host=pg3 user=streaming_barman port=5432 dbname=databasename
    backup_method = postgres
    streaming_archiver = on
    slot_name = barman
    create_slot = auto
    minimum_redundancy = 5
    retention_policy = RECOVERY WINDOW OF 7 DAYS
    local_staging_path = /var/lib/barman/staging

  .. important::
    You will not see any in place changes in the configuration file. The overrides are
    applied internally and you can check the current server configuration by using the
    command ``barman show-servers SERVER_NAME`` for the complete list of settings.