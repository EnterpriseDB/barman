.. _backup:

Backups
=======

.. _backup-overview:

Overview
--------

The backup command is used to backup an entire Postgres server according to the
configuration file parameters. To use it, run:

``barman backup [OPTIONS] SERVER_NAME``

.. note::
    For detailed information on the backup command, refer to the
    :ref:`backup <commands-barman-backup>` command reference.

.. important::
    Any interaction you plan to have with Barman, you will have to assure that the
    server is correctly configured. Refer to :ref:`Quickstart <quickstart>` and
    :ref:`Configuration Reference <configuration>` sections for the steps you need to
    cover before trying to create any backup.

.. warning::
    Backup initiation will fail if WAL files are not correctly archived to Barman, either
    through the ``archiver`` or the ``streaming_archiver`` options.

Barman offers multiple backup methods for Postgres servers, each with its own approach
and requirements.

Prior to version 2.0, Barman relied solely on rsync for both standard backups and
file-level incremental backups. Streaming backups were introduced in this version.
Starting with version 3.11, Barman also supports block-level incremental backups through
the streaming connection.

.. important::
  For Postgres 15 and higher, ``exclusive`` backups are no longer supported. The only
  method for taking backups is through ``concurrent`` backup. If ``backup_options`` is
  unset, Barman will automatically set it to ``concurrent_backup``.

.. _backup-incremental-backups:

Incremental Backups
-------------------

Incremental backups involve using an existing backup as a reference to copy only the
data changes that have occurred since the last backup on the Postgres server.

The primary objectives of incremental backups in Barman are:

* Shorten the duration of the full backup process.
* Reduce disk space usage by eliminating redundant data across periodic backups (data
  deduplication).

Barman supports two types of incremental backups:

* File-level incremental backups (using ``rsync``)
* Block-level incremental backups (using ``pg_basebackup`` with Postgres 17)

.. note::
    Incremental backups of different types are not compatible with each other. For
    example, you cannot take a block-level incremental backup on top of an rsync backup,
    nor can you take a file-level incremental backup on top of a streaming backup created
    with ``pg_basebackup``.

.. _backup-managing-bandwidth-usage:

Managing Bandwidth Usage
------------------------

You can control I/O bandwidth usage with the ``bandwidth_limit`` option (global or per
server) by specifying a maximum rate in kilobytes per second. By default, this option is
set to ``0``, meaning there is no bandwidth limit.

If you need to manage I/O workload on specific tablespaces, use the
``tablespace_bandwidth_limit`` option (global or per server) to set limits for
individual tablespaces:

.. code-block:: text

    tablespace_bandwidth_limit = tbname:bwlimit[, tbname:bwlimit, ...]

This option takes a comma-separated list of tablespace name and bandwidth limit pairs
(in kilobytes per second).

When backing up a server, Barman will check for tablespaces listed in this option. If a
matching tablespace is found, the specified bandwidth limit will be applied. If no match
is found, the default bandwidth limit for the server will be used.

.. important::
    The ``bandwidth_limit`` option is available with ``rsync`` and ``postgres`` backup
    methods, but the ``tablespace_bandwidth_limit`` option is only applicable when using
    ``rsync``.

.. _backup-network-compression:

Network Compression
-------------------

You can reduce the size of data transferred over the network by using network compression. This
can be enabled with the ``network_compression`` option (global or per server):

.. code-block:: text

    network_compression = true | false

.. important::
    The ``network_compression`` option is not available with the ``postgres`` backup
    method.

Setting this option to ``true`` will enable data compression for network transfers
during both backup and recovery. By default, this option is set to ``false``.

.. _backup-backup-compression:

Backup Compression
------------------

Barman supports backup compression using the ``pg_basebackup`` tool. This feature can be
enabled with the ``backup_compression`` option (global or per server).

.. important::
    The ``backup_compression`` option, along with other options discussed here, is only
    available with the ``postgres`` backup method.

Compression Algorithms
""""""""""""""""""""""

Setting the ``backup_compression`` option will compress the backup using the specified
algorithm. Supported algorithms in Barman are: ``gzip``, ``lz4``, ``zstd``, and ``none``
(which results in an uncompressed backup).

.. code-block:: text

    backup_compression = gzip | lz4 | zstd | none

Barman requires the corresponding CLI utilities for the selected compression algorithm
to be installed on both the Barman server and Postgres server. These utilities can be
installed via system packages named ``gzip``, ``lz4``, and ``zstd`` on Debian, Ubuntu,
RedHat, CentOS, and SLES systems.

* On Ubuntu 18.04 (bionic), the ``lz4`` utility is available in the ``liblz4-tool``
  package.

* ``lz4`` and ``zstd`` are supported with Postgres 15 or higher.

.. important::
    If using ``backup_compression``, you must also set ``recovery_staging_path`` to
    enable recovery of compressed backups. Refer to the
    :ref:`Recovering Compressed backups <recovery-recovering-compressed-backups>`
    section for details.

Compression Workers
"""""""""""""""""""

You can use multiple threads to speed up compression by setting the
``backup_compression_workers`` option (default is ``0``):

.. code-block:: text

    backup_compression_workers = 2

.. note::
    This option is available only with ``zstd`` compression. ``zstd`` version must be
    1.5.0 or higher, or 1.4.4 or higher with multithreading enabled.

Compression Level
"""""""""""""""""

Specify the compression level with the ``backup_compression_level`` option. This should
be an integer value supported by the chosen compression algorithm. If not specified, the
default value for the algorithm will be used.

* For ``none`` compression, ``backup_compression_level`` must be set to ``0``.

* The available levels and default values depend on the chosen compression algorithm.
  Check the :ref:`backup configuration options <configuration-options-backups>` section
  for details.

* For Postgres versions prior to 15, ``gzip`` supports only
  ``backup_compression_level = 0``, which uses the default compression level.

Compression Location
""""""""""""""""""""

For Postgres 15 or higher, you can choose where compression occurs: on the ``server``
or the ``client``. Set the ``backup_compression_location`` option:

.. code-block:: text

    backup_compression_location = server | client

* ``server``: Compression occurs on the Postgres server, reducing network bandwidth
  but increasing server workload.
* ``client``: Compression is handled by ``pg_basebackup`` on the client side.

When ``backup_compression_location`` is set to ``server``, you can also configure
``backup_compression_format``:

.. code-block:: text

    backup_compression_format = plain | tar

* ``plain``: ``pg_basebackup`` decompresses data before writing to disk.
* ``tar``: Backups are written as compressed tarballs (default).

Depending on the chosen ``backup_compression`` and ``backup_compression_format``, you
may need to install additional tools on both the Postgres and Barman servers.

Refer to the table below to select the appropriate tools for your configuration.

.. list-table::
    :widths: 5 5 5 5
    :header-rows: 1
    
    * - **backup_compression**
      - **backup_compression_format**
      - **Postgres**
      - **Barman**
    * - gzip
      - plain
      - tar
      - None
    * - gzip
      - tar
      - tar
      - tar
    * - lz4
      - plain
      - tar, lz4
      - None
    * - lz4
      - tar
      - tar, lz4
      - tar, lz4
    * - zstd
      - plain
      - tar, zstd
      - None
    * - zstd
      - tar
      - tar, zstd
      - tar, zstd
    * - none
      - tar
      - tar
      - tar

.. _backup-immediate-checkpoint:

Immediate Checkpoint
--------------------

Before starting a backup, Barman requests a checkpoint, which can generate additional
workload. By default, this checkpoint is managed according to Postgres' workload control
settings, which may delay the backup.

You can modify this default behavior using the ``immediate_checkpoint`` configuration
option (default is ``false``).

If ``immediate_checkpoint`` is set to ``true``, Postgres will perform the checkpoint at
maximum speed without throttling, allowing the backup to begin as quickly as possible.
You can override this configuration at any time by using one of the following options
with the ``barman backup`` command:

* ``--immediate-checkpoint``: Forces an immediate checkpoint.
* ``--no-immediate-checkpoint``: Waits for the checkpoint to complete before starting
  the backup.

.. _backup-streaming-backup:

Streaming Backup
----------------

Barman can perform a backup of a Postgres server using a streaming connection with
``pg_basebackup``. 

.. important::
    ``pg_basebackup`` must be installed on the Barman server. It is recommended to use
    the latest version of ``pg_basebackup`` as it is backwards compatible. Multiple
    versions can be installed and specified using the ``path_prefix`` option in the
    configuration file.

To configure streaming backups, set the ``backup_method`` to ``postgres``:

.. code-block:: text

    backup_method = postgres

Block-level Incremental Backup
""""""""""""""""""""""""""""""

This type of backup uses the native incremental backup feature introduced in Postgres
17.

Block-level incremental backups deduplicate data at the page level in Postgres. This
means only pages modified since the last backup need to be stored, which is more
efficient, especially for large databases with frequent writes.

To perform block-level incremental backups in Barman, use the ``--incremental`` option
with the backup command. You must provide a backup ID or shortcut referencing a previous
backup (full or incremental) created with ``backup_method=postgres`` for deduplication.
Alternatively, you can use ``last-full`` or ``latest-full`` to reference the most recent
eligible full backup in the catalog.

Example command:

``barman backup --incremental BACKUP_ID SERVER_NAME``

To use block-level incremental backups in Barman, you must:

* Use Postgres 17 or later.
* This feature relies on WAL Summarization, so ``summarize_wal`` must be enabled on your
  database server before taking the initial full backup.
* Use ``backup_method=postgres``.

.. note::
    Compressed backups are currently not supported for block-level incremental backups
    in Barman.

.. important::
    If you enable ``data_checksums`` between block-level incremental backups, it's
    advisable to take a new full backup. Divergent checksum configurations can
    potentially cause issues during recovery.

.. _backup-rsync-backup:

Backup with Rsync through SSH
-----------------------------

Barman can perform a backup of a Postgres server using Rsync, which uses SSH as a
transport mechanism.

To configure a backup using rsync, include the following parameters in the Barman server
configuration file:

.. code-block:: text

    backup_method = rsync
    ssh_command = ssh postgres@pg

Here, ``backup_method`` activates the rsync backup method, and ``ssh_command`` specifies
the SSH connection details from the Barman server to the Postgres server.

.. note::
    Starting with Barman 3.11, a keep-alive mechanism is used for rsync-based backups.
    This mechanism sends a simple ``SELECT 1`` query over the libpq connection to
    prevent firewall or router disconnections due to idle connections. You can control or
    disable this mechanism using the ``keepalive_interval`` configuration option.

File-Level Incremental Backups
""""""""""""""""""""""""""""""

File-level incremental backups rely on rsync and alternatively hard links, so both the
operating system and file system where the backup data is stored must support these
features.

The core idea is that during a subsequent base backup, files that haven't changed since
the last backup are shared, which saves disk space. This is especially beneficial in
:term:`VLDB` and those with a high percentage of read-only historical tables.

You can enable rsync incremental backups through a global/server option called
``reuse_backup``, which manages the Barman backup command. It accepts three values:

* ``off``: Standard full backup (default).
* ``link``: File-level incremental backup that reuses the last backup and creates hard
  links for unchanged files, reducing both backup space and time.
* ``copy``: File-level incremental backup that reuses the last backup and creates copies
  of unchanged files, reducing backup time but not space.

Typically, you would set ``reuse_backup`` to ``link`` as follows:

.. code-block:: text

    reuse_backup = link

Setting this at the global level automatically enables incremental backups for all your
servers.

You can override this setting with the ``--reuse-backup`` runtime option when running
the Barman backup command. For example, to run a one-off incremental backup, use:

.. code-block:: text

    barman backup --reuse-backup=link <server_name>

.. note::
    Unlike block-level incremental backups, rsync file-level incremental backups are
    self-contained. If a parent backup is deleted, the integrity of other backups is not
    affected. Deduplication in rsync backups uses hard links, meaning that when a reused
    backup is deleted, you don't need to create a new full backup; shared files will
    remain on disk until the last backup that used those files is also deleted.
    Additionally, using ``reuse_backup = link`` or ``reuse_backup = copy`` for the
    initial backup has no effect, as it will still be treated as a full backup due to
    the absence of existing files to link or copy.

.. _backup-concurrent-backup-of-a-standby:

Concurrent Backup of a Standby
------------------------------

When performing a backup from a standby server, ensure the following configuration
options are set to point to the standby:

* ``conninfo``
* ``streaming_conninfo`` (if using ``backup_method = postgres`` or
  ``streaming_archiver = on``)
* ``ssh_command`` (if using ``backup_method = rsync``)
* ``wal_conninfo`` (connecting to the primary if ``conninfo`` is pointing to a standby)

The ``primary_conninfo`` option should point to the primary server. Barman will use
``primary_conninfo`` to trigger a new WAL switch on the primary, allowing the concurrent
backup from the standby to complete without waiting for a natural WAL switch.

.. note::
    It's crucial to configure ``primary_conninfo`` if backing up a standby during periods
    of minimal or no write activity on the primary.

In Barman 3.8.0 and later, if ``primary_conninfo`` is configured, you can also set the
``primary_checkpoint_timeout`` option. This specifies the maximum wait time (in seconds)
for a new WAL file before Barman forces a checkpoint on the primary. This timeout should
exceed the ``archive_timeout`` value set on the primary.

If ``primary_conninfo`` is not set, the backup will still proceed but will pause at the
stop backup stage until the last archived WAL segment is newer than the latest WAL
required by the backup.

Barman requires that WAL files and backup data originate from the same Postgres
cluster. If the standby is promoted to primary, the existing backups and WALs remain
valid. However, you should update the Barman configuration to use the new standby for
future backups and WAL retrieval.

.. note::
    In case of a failover on the Postgres cluster you can update the Barman
    configuration with :ref:`Configuration Models <configuration-configuration-models>`.

WALs can be retrieved from the standby via WAL streaming or WAL archiving. Refer to the
:ref:`concepts <concepts-postgres-backup-concepts-wal-archiving-and-wal-streaming>`
section for more details. If you want to start working with WAL streaming or WAL
archiving, refer to the quickstart section on
:ref:`streaming backups with wal streaming <quickstart-configuring-your-first-server-streaming-backups-with-wal-streaming>`
or
:ref:`rsync backups with wal archiving <quickstart-configuring-your-first-server-rsync-backups-with-wal-archiving>`.

.. note::
    For Postgres 10 and earlier, Barman cannot handle simultaneous WAL streaming and
    archiving on a standby. You must disable one if the other is in use, as WALs from
    Postgres 10 and earlier may differ at the binary level, leading to false-positive 
    detection issues in Barman.

.. _backup-managing-external-configuration-files:

Managing external configuration files
-------------------------------------

Barman handles :term:`external configuration files <External Configuration Files>`
differently depending on the backup method used. With the ``rsync`` method, external
files are copied into the PGDATA directory. However, with the ``postgres`` method,
external files are not copied, and a warning is issued to notify the user about those
files.

Refer to the :ref:`Managing external configuration files <recovery-managing-external-configuration-files>`
section in the recovery chapter to understand how external files are handled when
restoring a backup.

.. hint::
    Since Barman does not establish SSH connections to the PostgreSQL host when
    ``backup_method = postgres``, you may want to configure a post-backup hook
    and use the output of ``barman show-server`` command to back up the external
    configuration files on your own right after the backup is finished.


.. _backup-backups-on-immutable-storage:

Using an immutable storage for backups
--------------------------------------

Barman can be configured to store backups on immutable storage to protect against
malicious actors or accidental deletions. Such storage may also be referred to as WORM (Write Once,
Read Many) storage.

The main use case for these type of storage is to protect the backups from ransomware
attacks. By using immutable storage, the backups cannot be deleted or modified for a
specific period of time.

To configure Barman to store backups on immutable storage, you need to follow these
suggestions:

* Only the following two directories should be configured to be stored on the immutable
  storage path:
  
  * :ref:`basebackups_directory <configuration-options-backups-basebackups-directory>`:
    The directory where backups are stored.
  * :ref:`wal_directory <configuration-options-wals-wals-directory>`: The directory
    where WAL files are stored.
* All other directories should be stored on a regular storage path because they are used
  by Barman's internal process and don't hold data crucial for restoring the cluster.
  This can be accomplished by configuring the :ref:`barman_home <configuration-options-general-barman-home>`
  option to point to a regular storage in the global configuration, or the
  :ref:`backup_directory <configuration-options-backups-backup-directory>`
  option in the server section. This still requires that the options from the previous
  bullet points are set accordingly.
* The WAL file catalog should be stored on a regular storage path. This can be
  accomplished by configuring the :ref:`xlogdb_directory <configuration-options-wals-xlogdb-directory>`
  option to point to a regular storage.
* Retention policies should cover at least the full period in which the backed up files
  are immutable. This can be accomplished by setting the ``retention_policy`` option in
  the server section to a value that is greater than the immutable storage's period of
  immutability. This is to ensure that the backups are not deleted before the
  immutability period expires.

.. note::
    This option was included in Barman 3.12. Refer to the configuration option
    ``xlogdb_directory`` for more information.

.. _backup-cloud-snapshot-backups:

Cloud Snapshot Backups
----------------------

Barman can perform backups of Postgres servers deployed in specific cloud environments
by utilizing snapshots of storage volumes. In this setup, Postgres file backups are
represented as volume snapshots stored in the cloud, while Barman functions as the
storage server for Write-Ahead Logs (WALs) and the backup catalog. Despite the backup
data being stored in the cloud, Barman manages these backups similarly to traditional
ones created with ``rsync`` or ``postgres`` backup methods.

.. note::
    Additionally, snapshot backups can be created without a Barman server by using the
    ``barman-cloud-backup`` command directly on the Postgres server. Refer to the
    :ref:`barman-cli-cloud <barman-cloud-barman-cli-cloud>` section for more information
    on how to properly work with this option.

.. important::
    The following configuration options and equivalent command arguments (if applicable)
    are not available when using ``backup_method=snapshot``:

    * ``backup_compression`` 
    * ``bandwidth_limit`` (``--bwlimit``)
    * ``parallel_jobs`` (``--jobs``)
    * ``network_compression``
    * ``reuse_backup`` (``--reuse-backup``)

To configure a backup using snapshot, include the following parameters in the Barman server
configuration file:

.. code-block:: text

    backup_method = snapshot
    snapshot_provider = CLOUD_PROVIDER
    snapshot_instance = INSTANCE_NAME
    snapshot_disks = DISK_NAME1,DISK_NAME2

.. important::
    Ensure ``snapshot_disks`` includes all disks that store Postgres data. Any data
    stored on a disk not listed will not be backed up and will be unavailable during
    recovery.

Requirements and Configuration
""""""""""""""""""""""""""""""

To use the snapshot backup method with Barman, your deployment must meet these
requirements:

1. Postgres must be running on a compute instance provided by a supported cloud
   provider.
2. All critical data, including PGDATA and tablespace data, must be stored on storage
   volumes that support snapshots.
3. The ``findmnt`` command must be available on the Postgres host.

.. important::
    Configuration files stored outside of ``PGDATA`` will not be included in the snapshots.
    You will need to manage these files separately, using a configuration management
    system or other mechanisms.

Google Cloud Platform
"""""""""""""""""""""

To use snapshot backups on :term:`GCP` with Barman, please ensure the following:

1. **Python Libraries**

Install the ``google-cloud-compute`` and ``grpcio`` libraries for the Python
distribution used by Barman. These libraries are optional and not included by default.

Install them using pip:

.. code:: bash
  
    pip3 install grpcio google-cloud-compute

.. note::
    The ``google-cloud-compute`` library requires Python 3.7 or newer. GCP snapshots are
    not compatible with earlier Python versions.

2. **Disk Requirements**

The disks used in the ``snapshot`` backup must be zonal persistent disks. Regional
persistent disks are not supported at this time.

3. **Access Control**

Barman needs a service account with specific permissions. You can either attach this
account to the compute instance running Barman (recommended) or use the
``GOOGLE_APPLICATION_CREDENTIALS`` environment variable to specify a credentials
file.

.. important::
    Ensure the service account has the permissions listed below:

    * ``compute.disks.createSnapshot``
    * ``compute.disks.get``
    * ``compute.globalOperations.get``
    * ``compute.instances.get``
    * ``compute.snapshots.create``
    * ``compute.snapshots.delete``
    * ``compute.snapshots.list``

For provider specific credentials configurations, refer to the
`Google authentication methods <https://cloud.google.com/docs/authentication>`_ and
`service account impersonation <https://cloud.google.com/docs/authentication/use-service-account-impersonation>`_.

4. **Specific Configuration**

The fields ``gcp_project`` and ``gcp_zone`` are configuration options specific to GCP.

.. code-block:: text

    gcp_project = GCP_PROJECT_ID
    gcp_zone = ZONE

Microsoft Azure
"""""""""""""""

To use snapshot backups on Azure with Barman, ensure the following:

1. **Python Libraries**

The ``azure-mgmt-compute`` and ``azure-identity`` libraries must be available for the
Python distribution used by Barman. These libraries are optional and not included by
default.

Install them using pip:

.. code:: bash

    pip3 install azure-mgmt-compute azure-identity

.. note::
    The ``azure-mgmt-compute`` library requires Python 3.7 or later. Azure snapshots are
    not compatible with earlier Python versions.

2. **Disk Requirements**

All disks involved in the snapshot backup must be managed disks attached to the VM
instance as data disks.

3. **Access Control**

Barman needs to access Azure using credentials obtained via managed identity or CLI
login. 

The following environment variables are supported: ``AZURE_STORAGE_CONNECTION_STRING``,
``AZURE_STORAGE_KEY`` and ``AZURE_STORAGE_SAS_TOKEN``. You can also use the
``--credential`` option to specify either ``azure-cli`` or ``managed-identity``
credentials in order to authenticate via Azure Active Directory.

.. important::
    Ensure the credential has the permissions listed below:

    * ``Microsoft.Compute/disks/read``
    * ``Microsoft.Compute/virtualMachines/read``
    * ``Microsoft.Compute/snapshots/read``
    * ``Microsoft.Compute/snapshots/write``
    * ``Microsoft.Compute/snapshots/delete``

For provider specific credential configurations, refer to the
`Azure environment variables configurations <https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-data-operations-cli#set-environment-variables-for-authorization-parameters>`_
and `Identity Package <https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity?view=azure-python>`_.

4. **Specific Configuration**

The fields ``azure_subscription_id`` and ``azure_resource_group`` are configuration
options specific to Azure.

.. code-block:: text

    azure_subscription_id = AZURE_SUBSCRIPTION_ID
    azure_resource_group = AZURE_RESOURCE_GROUP
    
Amazon Web Services
"""""""""""""""""""

To use snapshot backups on :term:`AWS` with Barman, please ensure the following:

1. **Python Libraries**

The ``boto3`` library must be available for the Python distribution used by Barman. This
library is optional and not included by default.

Install it using pip:

.. code:: bash

    pip3 install boto3

2. **Disk Requirements**

All disks involved in the snapshot backup must be non-root EBS volumes attached to the
same VM instance and NVMe volumes are not supported.

3. **Access Control**

Barman needs to access AWS so you must configure the AWS credentials with the ``awscli``
tool as the postgres user, by entering the Access Key and Secret Key that must be
previously created in the IAM section of the AWS console.

.. important::
    Ensure you have the permissions listed below:

    * ``ec2:CreateSnapshot``
    * ``ec2:CreateTags``
    * ``ec2:DeleteSnapshot``
    * ``ec2:DescribeSnapshots``
    * ``ec2:DescribeInstances``
    * ``ec2:DescribeVolumes``

For provider specific credentials configurations, refer to the
`AWS boto3 configurations <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html>`_.

4. **Specific Configuration**

The fields ``aws_region``, ``aws_profile`` and ``aws_await_snapshots_timeout`` are
configuration options specific to AWS.

``aws_profile`` is the name of the AWS profile in the credentials file. If not used, the
default profile will be applied. If no credentials file exists, credentials will come from
the environment.

``aws_region`` overrides any region defined in the AWS profile.

``aws_await_snapshots_timeout`` is the timeout for waiting for snapshots to be created
(default is ``3600`` seconds).

When specifying ``snapshot_instance`` or ``snapshot_disks``, Barman accepts either the
instance/volume ID or the name of the resource. If you use a name, Barman will query AWS
for resources with a matching ``Name`` tag. If zero or multiple matches are found,
Barman will return an error.

.. code-block:: text

    aws_region = AWS_REGION
    aws_profile = AWS_PROFILE_NAME
    aws_await_snapshots_timeout = TIMEOUT_IN_SECONDS

5. **Ransomware Protection**

Ransomware protection is essential to secure data and maintain operational stability.
With Amazon EBS Snapshot Lock, snapshots are protected from deletion, providing an
immutable backup that safeguards against ransomware attacks. By locking snapshots,
unwanted deletions are prevented, ensuring reliable recovery options in case of
compromise. Barman can prevent unwanted deletion of backups by locking the snapshots
when creating the backup.

.. note::
    To delete a locked backup, you must first manually remove the lock in the AWS
    console.

To lock a snapshot during backup creation, you need to configure the following options:

1. Choose the snapshot lock mode: either ``compliance`` or ``governance``.
2. Set either the lock duration or the expiration date (not both). Lock duration is
   specified in days, ranging from 1 to 36,500. If you choose an expiration date, it must
   be at least 1 day after the snapshot creation date and time, using the format
   ``YYYY-MM-DDTHH:MM:SS.sssZ``.
3. Optionally, set a cool-off period (in hours), from 1 to 72. This option only applies
   when the lock mode is set to ``compliance``.

.. code-block:: text

    aws_snapshot_lock_mode = compliance | governance
    aws_snapshot_lock_duration = 1
    aws_snapshot_lock_cool_off_period = 1
    aws_snapshot_lock_expiration_date = "2024-10-07T21:53:00.606Z"

.. important::
    Ensure you have the permission listed below:

    * ``ec2:LockSnapshot``

For the concepts behing AWS Snapshot Lock, refer to the `Amazon EBS snapshot lock concepts <https://docs.aws.amazon.com/ebs/latest/userguide/snapshot-lock-concepts.html>`_.

Backup Process
""""""""""""""

Here is an overview of the snapshot backup process:

1. Barman performs checks to validate the snapshot options, instance, and disks.
    Before each backup and during the ``barman check`` command, the following checks are
    performed:

    * The compute instance specified by ``snapshot_instance`` and any provider-specific
      arguments exists.
    * The disks listed in ``snapshot_disks`` are present.
    * The disks listed in ``snapshot_disks`` are attached to the ``snapshot_instance``.
    * The disks listed in ``snapshot_disks`` are mounted on the ``snapshot_instance``.

2. Barman initiates the backup using the Postgres backup API.
3. The cloud provider API is used to create a snapshot for each specified disk. Barman
   waits until each snapshot reaches a state that guarantees application consistency
   before proceeding to the next disk.
4. Additional provider-specific details, such as the device name for each disk, and the
   mount point and options for each disk are recorded in the backup metadata.

Metadata
""""""""

Regardless of whether you provision recovery disks and instances using
infrastructure-as-code, ad-hoc automation, or manually, you will need to use Barman to
identify the necessary snapshots for a specific backup. You can do this with the barman
``show-backup`` command, which provides details for each snapshot included in the
backup.

For example:

.. code-block:: text

    Backup 20240813T200506:
      Server Name            : snapshot
      System Id              : 7402620047885836080
      Status                 : DONE
      PostgreSQL Version     : 160004
      PGDATA directory       : /opt/postgres/data
      Estimated Cluster Size : 22.7 MiB

      Server information:
        Checksums            : on

      Snapshot information:
        provider             : aws
        account_id           : 714574844897
        region               : sa-east-1

        device_name          : /dev/sdf
        snapshot_id          : snap-0d2288b4f30e3f9e3
        snapshot_name        : Barman_AWS:1:/dev/sdf-20240813t200506
        Mount point          : /opt/postgres
        Mount options        : rw,noatime,seclabel

      Base backup information:
        Backup Method        : snapshot-concurrent
        Backup Size          : 1.0 KiB (16.0 MiB with WALs)
        WAL Size             : 16.0 MiB
        Timeline             : 1
        Begin WAL            : 00000001000000000000001A
        End WAL              : 00000001000000000000001A
        WAL number           : 1
        Begin time           : 2024-08-14 16:21:50.820618+00:00
        End time             : 2024-08-14 16:22:38.264726+00:00
        Copy time            : 47 seconds
        Estimated throughput : 22 B/s
        Begin Offset         : 40
        End Offset           : 312
        Begin LSN            : 0/1A000028
        End LSN              : 0/1A000138

      WAL information:
        No of files          : 1
        Disk usage           : 16.0 MiB
        WAL rate             : 5048.32/hour
        Last available       : 00000001000000000000001B

      Catalog information:
        Retention Policy     : not enforced
        Previous Backup      : - (this is the oldest base backup)
        Next Backup          : - (this is the latest base backup)

The ``--format=json`` option can be used when integrating with external tooling.

.. code-block:: json

    {
      "snapshots_info": {
        "provider": "gcp",
        "provider_info": {
          "project": "project_id"
        },
        "snapshots": [
          {
            "mount": {
              "mount_options": "rw,noatime",
              "mount_point": "/opt/postgres"
            },
            "provider": {
              "device_name": "pgdata",
              "snapshot_name": "barman-av-ubuntu20-primary-pgdata-20230123t131430",
              "snapshot_project": "project_id"
            }
          },
          {
            "mount": {
              "mount_options": "rw,noatime",
              "mount_point": "/opt/postgres/tablespaces/tbs1"
            },
            "provider": {
              "device_name": "tbs1",
              "snapshot_name": "barman-av-ubuntu20-primary-tbs1-20230123t131430",
              "snapshot_project": "project_id",
            }
          }
        ]
      }
    }

The metadata found in ``snapshots_info/provider_info`` and
``snapshots_info/snapshots/*/provider`` varies depending on the cloud provider, as
detailed in the following sections.

**GCP**

``snapshots_info/provider_info``

* ``project``: The GCP project ID of the project which owns the resources involved
  in backup and recovery.

``snapshots_info/snapshots/*/provider``

* ``device_name``: The short device name with which the source disk for the snapshot
  was attached to the backup VM at the time of the backup.
* ``snapshot_name``: The name of the snapshot.
* ``snapshot_project``: The GCP project ID which owns the snapshot.

**Azure**

``snapshots_info/provider_info``

* ``subscription_id``: The Azure subscription ID which owns the resources involved
  in backup and recovery.
* ``resource_group``: The Azure resource group to which the resources involved in
  the backup belong.

``snapshots_info/snapshots/*/provider``

* ``location``: The Azure location of the disk from which the snapshot was taken.
* ``lun``: The LUN identifying the disk from which the snapshot was taken at the
  time of the backup.
* ``snapshot_name``: The name of the snapshot.

**AWS**

``snapshots_info/provider_info``

* ``account_id``: The ID of the AWS account which owns the resources used to make
  the backup.
* ``region``: The AWS region in which the resources involved in backup are located.

``snapshots_info/snapshots/*/provider``

* ``device_name``: The device to which the source disk was mapped on the backup VM
  at the time of the backup.
* ``snapshot_id``: The ID of the snapshot as assigned by AWS.
* ``snapshot_name``: The name of the snapshot.
