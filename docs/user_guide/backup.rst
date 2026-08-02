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

The most critical requirement for a Barman server is the amount of disk space available.
You are recommended to plan the required disk space based on the size of the clusters
to backup, number of WAL files generated per day, frequency of backups, and retention
policies.

Barman developers regularly test Barman with XFS and ext4 filesystems. Like PostgreSQL,
Barman does nothing special for NFS mountpoints used for storing backups and WALs.
The following points are required for safely using Barman with NFS:

  * The ``barman_lock_directory`` should be on a local filesystem.
  * Use at least NFS protocol version 4.
  * The file system must be mounted using the hard and synchronous options
    (``hard``, ``sync``).

.. important::
  For Postgres 15 and higher, ``exclusive`` backups are no longer supported. The only
  method for taking backups is through ``concurrent`` backup. If ``backup_options`` is
  unset, Barman will automatically set it to ``concurrent_backup``.

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


.. _backup-streaming-backup-block-level-incremental:

Block-level Incremental Backup
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. important::
    If you enable ``data_checksums`` between block-level incremental backups, it's
    advisable to take a new full backup. Divergent checksum configurations can
    potentially cause issues during recovery.


.. _backup-streaming-backup-cloud:

Streaming backups to the cloud
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When configured with ``backup_method = postgres`` as well as ``basebackups_directory``
and ``wals_directory`` to cloud storage URLs, Barman supports streaming backups
directly to a cloud storage. In this mode, backups are streamed from the Postgres
server via ``pg_basebackup`` to the Barman server and dynamically pushed to the cloud
in chunks, without ever storing the complete backup locally. WAL files are streamed
from the Postgres server to the Barman server and uploaded to the cloud whenever
archiving is triggered, which happens periodically or when manually running the
``barman archive-wal`` command.

This is currently the only method that enables block-level incremental backups in a
cloud environment.

To configure this feature, set ``backup_method = postgres`` as well as
``basebackups_directory`` and ``wals_directory`` to cloud storage URLs. For example,
if using an S3-compatible storage:

.. code-block:: text

    backup_method = postgres
    basebackups_directory = s3://mybucket/barman/
    wals_directory = s3://mybucket/barman/

Once configured, you can leverage all the capabilities of a Barman server, which
includes incremental backups, retention policies, and more, while having your data
safely stored in the cloud.

.. note::

  The destination URL can specify an S3 access point ARN in place of a bucket
  name, including an S3 on Outposts access point. Refer to
  :ref:`AWS Outposts <aws-outposts-s3>` for the AWS-side prerequisites this
  requires.

.. note::

  Although it is possible to set different URLs for ``basebackups_directory`` and
  ``wals_directory``, it is highly recommended to use the same URL, and possibly the
  same path, for both options. This ensures a consistent storage structure in the cloud
  as Barman organizes data of each server in dedicated subdirectories.

.. warning::
  Changing the value of ``basebackups_directory`` and/or ``wals_directory`` on an
  active server is discouraged. Even if done with care, by deactivating the server,
  moving existing data to the new location, and reactivating it, there are potential
  risks of data loss or misconfiguration.

  Instead, the safe option is to create a new server configuration with the new value
  for ``basebackups_directory`` or ``wals_directory``, perform a new full backup to the
  new server, and then decommission the old server once the new one is fully
  operational.

When using this feature, Barman requires a local staging space to process chunks before
uploading them to the cloud. The amount of staging space allowed to be used as well as
its location can be configured with the options ``cloud_staging_max_size`` and
``cloud_staging_directory``, or overridden at runtime with the
``--cloud-staging-max-size`` and ``--cloud-staging-directory`` CLI options.

This is an experimental feature. For this reason, a few limitations apply:

1. Currently, only S3-compatible storages are supported as destination;
2. Encryption of backups and WALs is not supported;
3. Compression of backups is not supported. WAL compression is supported except when
   using ``pigz`` or ``custom`` as compression methods;
4. Barman subcommands which require access to the backup or WAL files, such as
   ``verify-backup``, ``generate-manifest``, ``rebuild-xlogdb`` and ``get-wal``, are
   not supported and will fail if executed;
5. The Barman :ref:`geographical-redundancy` feature and its related commands are not
   supported.
6. For now this feature works only on Linux-based distributions and is not
   supported on BSD-derived systems (such as FreeBSD or OpenBSD).


.. _backup-rsync-backup:

Rsync Backups
-------------

Barman can perform a backup of a Postgres server using Rsync, which uses SSH as a
transport mechanism.

To configure a backup using rsync, include the following parameters in the Barman
configuration file for the specific Postgres server:

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

File-Level Incremental Backups with Rsync
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

Local Rsync Backups
-------------------

Under special circumstances, Barman can be installed on the same server where the
Postgres instance resides, with backed up data stored on a separate volume from
``PGDATA`` and, where applicable, tablespaces. Usually, these backup volumes reside on
network storage appliances, with filesystems like NFS.

For these cases, Barman supports a local backup method called ``local-rsync``. This
method uses the same rsync-based backup approach as the remote ``rsync`` method, but
without relying on SSH for the transport layer. Instead, it performs file copy
operations locally on the same server where Postgres is running (for this reason it is
required that Barman runs with the same user as Postgres).

The only requirement for local backups is that Barman runs with the same user as the
Postgres server, which is normally ``postgres``. Given that the community packages by
default install Barman under the ``barman`` user, this use case requires manual
installation procedures that include:

* cron configurations
* log configurations, including logrotate
* Barman home configuration.

The recommended way to set up Barman for local backups is to follow these steps
after installing Barman:

1. Set ``barman_user = postgres`` (or the operating system user running the server if
   using a different one) in the global configuration file to ensure Barman has access
   to read the data files.
2. Configure the backup directory and other relevant directories to be accessible by the
   same user. This can be done by setting the appropriate options in the configuration file
   (e.g., ``backup_directory``, ``barman_home``, ``log_filename``, etc.) to point to paths
   with the correct permissions. Don't use the default paths provided by the community
   packages, as they are owned by the ``barman`` user, and permissions and ownership of those
   directories will be restored to the original state during package updates, which can
   cause issues.

Refer to :ref:`Configuration options <configuration-options-general>` for more details.

In order to use local backup for a given server in Barman, you need to set
``backup_method`` to ``local-rsync``.


.. _backup-local-to-cloud-backup:

Local-to-Cloud Backups
----------------------

Barman can perform backups directly to cloud object storage using the ``local-to-cloud``
backup method. This method reads data directly from the Postgres PGDATA directory on
the local filesystem and uploads it directly to cloud storage without requiring
intermediate local storage for the backup data.

.. note::
    Currently, only **Amazon S3** and S3-compatible storage are supported. Support for
    Azure Blob Storage and Google Cloud Storage will be added in future versions of
    Barman.

Like other backup methods, this approach uses the Postgres's low-level backup API
for coordination with the database server, but uploads the backup data directly to the cloud
as it is read from disk.

.. important::
    The ``local-to-cloud`` backup method requires:

    * Postgres 9.6 or later (for concurrent backup support).
    * Barman must have direct filesystem access to the PGDATA directory.
    * The user executing Barman must be the owner of the database cluster.
    * AWS credentials properly configured (or credentials for S3-compatible storage).
    * Network access to a properly configured S3 or S3-compatible object storage
      bucket.

To configure local-to-cloud backups, set the ``backup_method`` to ``local-to-cloud`` and
configure cloud storage path URLs:

.. code-block:: text

    backup_method = local-to-cloud
    basebackups_directory = s3://bucket-name
    wals_directory = s3://bucket-name
    conninfo = host=localhost dbname=postgres user=postgres

The ``basebackups_directory`` must be an S3 storage URL (e.g., ``s3://bucket/path``).
Similarly, ``wals_directory`` should point to an S3 storage location for WAL archiving.

.. note::
    The destination URL can specify an S3 access point ARN in place of a
    bucket name, including an S3 on Outposts access point. Refer to
    :ref:`AWS Outposts <aws-outposts-s3>` for the AWS-side prerequisites this
    requires.

.. note::
    It's recommended to use the same bucket and path for both ``basebackups_directory``
    and ``wals_directory`` to maintain a consistent storage structure in the cloud, as
    Barman organizes data of each server in dedicated subdirectories.

.. warning::
  Changing the value of ``basebackups_directory`` and/or ``wals_directory`` on an
  active server is discouraged. Even if done with care, by deactivating the server,
  moving existing data to the new location, and reactivating it, there are potential
  risks of data loss or misconfiguration.

  Instead, the safe option is to create a new server configuration with the new value
  for ``basebackups_directory`` or ``wals_directory``, perform a new full backup to the
  new server, and then decommission the old server once the new one is fully
  operational.


AWS S3 Configuration Options
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``local-to-cloud`` backup method supports various AWS S3 configuration options:

**Bandwidth Control:**

* ``bandwidth_limit``: Maximum transfer rate in kilobytes per second for cloud uploads
  (default: ``0``, meaning no limit).

**AWS S3 Options:**

* ``aws_region``: AWS region for the S3 bucket.
* ``aws_profile``: AWS profile name for authentication.
* ``aws_encryption``: S3 encryption method (``AES256`` or ``aws:kms``).
* ``aws_sse_kms_key_id``: KMS key ID for server-side encryption.
* ``aws_read_timeout``: S3 read timeout in seconds.

**Cloud Options:**

* ``cloud_upload_max_archive_size``: Maximum size per cloud archive (default: ``100G``).
* ``cloud_upload_min_chunk_size``: Minimum chunk size for multipart uploads.
* ``cloud_delete_batch_size``: Number of files to delete in a single batch operation.

**Compression:**

* ``compression``: WAL compression algorithm (``gzip``, ``bzip2``, ``xz``, ``snappy``,
  ``zstd``, ``lz4``, or ``none``).
* ``compression_level``: Compression level - can be set to ``low``, ``medium``,
  ``high``, or a number according to the chosen algorithm.

You can find more information on compression settings in the :ref:`backup-backup-compression`
section below.

When to Use Local-to-Cloud
^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``local-to-cloud`` backup method is appropriate when:

* Postgres and Barman run on the same machine, or Barman has direct local filesystem
  access to the PGDATA directory.
* You want to back up directly to AWS S3 or S3-compatible storage without intermediate
  local storage for the backup data itself.
* You want a simple, unified cloud backup configuration through the main ``barman`` CLI.

Limitations
^^^^^^^^^^^

The ``local-to-cloud`` backup method has the following limitations:

* It **does not** support incremental backups (unlike the ``postgres`` method with
  Postgres 17+, which supports block-level incremental backups).
* It **does not** support file-level deduplication through hard links (unlike ``rsync``
  with ``reuse_backup``).
* **Only AWS S3 and S3-compatible storage are currently supported** - Azure Blob Storage
  and Google Cloud Storage support will be added in future versions.
* It requires local filesystem access to PGDATA (unlike ``rsync`` or ``postgres``
  methods for remote servers).

Example Configuration
^^^^^^^^^^^^^^^^^^^^^

Here is a complete example configuration for local-to-cloud backups with AWS S3:

.. code-block:: text

    [myserver]
    backup_method = local-to-cloud
    barman_user = postgres
    basebackups_directory = s3://my-backup-bucket
    wals_directory = s3://my-backup-bucket
    conninfo = host=localhost dbname=postgres user=postgres
    bandwidth_limit = 5120  ; 5 MB/s
    compression = gzip
    aws_region = us-west-2
    aws_profile = barman
    archiver = on

With this configuration, Barman will perform backups by reading data directly from the
local PGDATA directory and uploading it to the specified S3 bucket with bandwidth
throttling and gzip compression for WAL files.

WAL archival with local-to-cloud
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using the ``local-to-cloud`` backup method, WAL files can be archived to the cloud
using the `barman cloud-wal-archive` command, which uploads WAL files via the
``archive_command`` to the cloud storage specified in the configuration.

.. code-block:: text

    archive_command = 'barman cloud-wal-archive myserver %p'

To reduce WAL archival backlog during periods of high WAL generation, the ``--parallel``
flag (or the ``cloud_wal_archive_parallel`` configuration option) can be used to upload
additional WAL files concurrently:

.. code-block:: text

    archive_command = 'barman cloud-wal-archive --parallel 2 myserver %p'

When ``--parallel N`` is set (N > 1), up to ``N - 1`` extra WAL files that are ready
in ``pg_wal/archive_status`` are uploaded in background processes after the primary WAL
has been successfully archived.


General Backup Settings
-----------------------

.. _backup-incremental-backups:

Incremental Backups
^^^^^^^^^^^^^^^^^^^

Incremental backups involve using an existing backup as a reference to copy only the
data changes that have occurred since the last backup on the Postgres server.

The primary objectives of incremental backups in Barman are:

* Shorten the duration of the full backup process.
* Reduce disk space usage by eliminating redundant data across periodic backups (data
  deduplication).

Barman supports two types of incremental backups:

* File-level incremental backups (using ``rsync``).
* Block-level incremental backups (using ``pg_basebackup`` with Postgres 17).

.. note::
    Incremental backups of different types are not compatible with each other. For
    example, you cannot take a block-level incremental backup on top of an rsync backup,
    nor can you take a file-level incremental backup on top of a streaming backup created
    with ``pg_basebackup``.

.. _backup-concurrent-backup-of-a-standby:

Backup from a Standby Server
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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


.. _backup-managing-bandwidth-usage:

Managing Bandwidth Usage
^^^^^^^^^^^^^^^^^^^^^^^^

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
    The ``bandwidth_limit`` option is available with ``rsync``, ``postgres`` and
    ``local-to-cloud`` backup methods, but the ``tablespace_bandwidth_limit`` option is
    only applicable when using ``rsync``.

.. _backup-network-compression:

Network Compression
^^^^^^^^^^^^^^^^^^^

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
^^^^^^^^^^^^^^^^^^

Barman supports backup compression using the ``pg_basebackup`` tool. This feature can be
enabled with the ``backup_compression`` option (global or per server).

.. important::
    The ``backup_compression`` option, along with other options discussed here, is only
    available with the ``postgres`` backup method.

Compression Algorithms
~~~~~~~~~~~~~~~~~~~~~~

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
    If using ``backup_compression``, you must also set ``staging_path`` and
    ``staging_location`` to enable recovery of compressed backups. Refer to the
    :ref:`Recovering Compressed backups <recovery-recovering-compressed-backups>`
    section for details.

Compression Workers
~~~~~~~~~~~~~~~~~~~

You can use multiple threads to speed up compression by setting the
``backup_compression_workers`` option (default is ``0``):

.. code-block:: text

    backup_compression_workers = 2

.. note::
    This option is available only with ``zstd`` compression. ``zstd`` version must be
    1.5.0 or higher, or 1.4.4 or higher with multithreading enabled.

Compression Level
~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~

For Postgres 15 or higher, you can choose where compression occurs: on the ``server``
or the ``client``. Set the ``backup_compression_location`` option:

.. code-block:: text

    backup_compression_location = server | client

* ``server``: Compression occurs on the Postgres server, reducing network bandwidth
  but increasing server workload.
* ``client``: Compression is handled by ``pg_basebackup`` on the client side.

You can also specify the backup format using ``backup_compression_format``:

.. code-block:: text

    backup_compression_format = plain | tar

* ``plain``: ``pg_basebackup`` decompresses data before writing to disk.
* ``tar``: Backups are written as compressed tarballs (default).

.. note::
  If setting ``backup_compression_location = server`` and
  ``backup_compression_format = plain``, you can reduce network usage given the files
  are compressed on the server side and decompressed on the client side. This can be
  useful when the network bandwidth is limited but CPU is not, and backups need to be
  stored uncompressed.

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


.. _backup-encryption:

Backup Encryption
^^^^^^^^^^^^^^^^^

Barman supports encryption of both backups and WAL files. This feature can be enabled
with the ``encryption`` option (global or per server).

Requirements
~~~~~~~~~~~~

The current encryption implementation for backups relies on the ``pg_basebackup``
ability to take backups in tar format. To achieve that, you need to set your
configuration as follows:

* ``backup_method = postgres``
* ``backup_compression = <compression_method>`` (``none`` for no compression)
* ``backup_compression_format = tar``

The backed up tar files are encrypted immediately after ``pg_basebackup`` finishes
writing them on the Barman server disk.

Encryption Methods
~~~~~~~~~~~~~~~~~~

Setting the ``encryption`` option dictates the encryption method used for base backups
and WALs. Currently, only ``gpg`` and ``none`` (no encryption) are accepted values.

.. note::
  For details about WAL encryption, refer to :ref:`wal_archiving-WAL-encryption`.

.. note::
  For details about decryption, refer to :ref:`recovery-recovering-encrypted-backups`.

GPG
~~~

This method is enabled by setting ``encryption = gpg`` in the configuration file.

To use :term:`GPG` for encryption, you need ``gpg`` version 2.1 or higher installed on
the server. You must also generate a GPG key pair in advance and configure the
``encryption_key_id`` option with the ID or recipient's email of the generated public
key. The corresponding private key must be present in GPG's keyring and secured with a
strong passphrase.


.. _backup-backups-on-immutable-storage:

Using an immutable storage for backups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Barman can be configured to store backups on immutable storage to protect against
malicious actors or accidental deletions. Such storage may also be referred to as
:term:`WORM` (Write Once, Read Many) storage.

The main use case for this type of storage is to protect the backups from ransomware
attacks. By using immutable storage, the backups cannot be deleted or modified for a
specific period of time.

In order for Barman to provide immutable backups, only the backups and WAL files
should be located in the immutable storage, leaving non-restorable data in regular
storage. This way Barman will be able to maintain transient information about metadata
of backups and WAL files as that information needs regular updates.

Given the above, to configure Barman to store backups on an immutable storage, you need
to follow these suggestions:

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
* Paths used for restoring incremental or compressed or encrypted backups, defined by
  the ``staging_path`` and ``staging_location`` options (see :ref:`restore configuration <configuration-options-restore>`
  for details), should also live in regular storage.
* Retention policies should cover at least the full period in which the backed up files
  are immutable. This can be accomplished by setting the ``retention_policy`` option in
  the server section to a value that is greater than the immutable storage's period of
  immutability. This is to ensure that the backups are not deleted before the
  immutability period expires.

To configure immutability of backups there's a :ref:`worm_mode <configuration-options-backups-worm-mode>`
option that needs to be enabled. This will let Barman skip processes which are
problematic when backups and WAL files are stored in a :term:`WORM` environment.

.. note::
    The option for relocating the ``xlogdb`` file was included in Barman 3.12. Refer
    to its :ref:`configuration section <configuration-options-wals-xlogdb-directory>`
    for more information.

Current limitations
~~~~~~~~~~~~~~~~~~~

The current implementation of immutable backup support in Barman has the following 
limitation:

* The WORM environment must have a grace period. A grace period provides a predefined
  window during which data can be modified or deleted before WORM restrictions take
  effect. This requirement exists because Barman makes use of renaming to safely copy
  WALs to external partitions, which would fail if the file has already entered a WORM
  state.

In general, a grace period of at least 15 minutes is recommended, as this provides
enough time for Barman to complete any necessary operations.

.. note::
  If backup encryption is also enabled, then the grace period must be long enough
  to cover the time required to perform the encryption (especially when the backup
  also includes tablespaces, which results in multiple tarballs).


  This is because encryption only happens at the end of the backup process, i.e.
  after ``pg_basebackup`` is finished.  As encryption can not be performed in-place,
  each tar file is encrypted individually, having its unencrypted version deleted once
  it is complete.

Given these constraints, users should evaluate whether the current implementation meets
their requirements before enabling immutable backup support.


.. _backup-managing-external-configuration-files:

Managing external configuration files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. _backup-immediate-checkpoint:

Immediate Checkpoint
^^^^^^^^^^^^^^^^^^^^

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
