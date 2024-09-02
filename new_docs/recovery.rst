.. _recovery:

Recovery
========

The recover command is used to restore an entire PostgreSQL server from a backup created
with the backup command. To use it, run:

``barman recover [OPTIONS] SERVER_NAME BACKUP_ID DESTINATION_PATH``

.. note::
  * Do not run the recover command on a directory where a Postgres instance is currently
    running. Ensure Postgres is stopped before initiating recovery, including recovery
    of tablespace directories.
  * The backup is restored to the specified directory, which will be ready to start a
    Postgres instance.
  * Use the ``list-backups`` command to find the specific backup ID you need.
  * Barman does not track symbolic links inside PGDATA (except for tablespaces).
    Ensure you manage symbolic links and include them in your disaster recovery plans.

.. _recovery-remote-recovery:

Remote Recovery
---------------

Use ``--remote-ssh-command COMMAND`` to perform recovery on a remote server via SSH.
It's recommended to use the postgres user on the target node for remote recovery.

**Known Limitations**

* Requires at least 4GB of free space in the system's temporary directory unless
  ``get-wal`` is specified.
* SSH must use public key authentication.
* The remote user must be able to create the necessary directory structure.
* Ensure there is enough free space on the remote server for the base backup and WAL
  files.

.. _recovery-tablespace-remapping:

Tablespace Remapping
--------------------

Use ``--tablespace NAME:DIRECTORY`` to remap tablespaces to a new location. Barman will
attempt to create the destination directory if it doesn't exist.

.. important::
  By default, tablespaces are restored to the same path they had on the source server.
  Be cautious when restoring a backup without any remapping to a destination where a
  Postgres instance already exists, as it can end up overriding existing tablespace
  directories.


.. _recovery-point-in-time-recovery:

Point-in-Time Recovery
----------------------

Specify a recovery ``target`` with one of the options:

* ``--target-time``: Recover to a specific timestamp.
* ``--target-xid``: Recover to a specific transaction ID.
* ``--target-lsn``: Recover to a Log Sequence Number (Postgres 10+).
* ``--target-name``: Recover to a named restore point.
* ``--target-immediate``: End recovery when a consistent state is reached (default).

.. note::
  * Recovery targets must be a value after the end of the backup. To recover to a
    point in time within a backup, use the previous backup.
  * Timezone defaults to the Barman host if not specified in ``--target-time``'s
    timestamp.
  * Use ``--exclusive`` to control whether to stop right before the target or including
    the target.
  * ``--target-tli`` sets the target timeline. Use numeric IDs or shortcut values
    (latest or current).

The previous targets can be used with a ``--target-action`` which can take these values:

* ``shutdown``: Shut down Postgres when the target is reached.
* ``pause``: Pause Postgres for inspection when the target is reached.
* ``promote``: Promote Postgres to master when the target is reached.

Also you can configure the instance as a standby by calling ``--standby-mode``. After
recovery, ensure you modify the configuration to connect to the intended upstream node
server.

.. _recovery-fetching-wals-from-barman:

Fetching WALs from Barman
-------------------------

Use ``--get-wal`` to configure Postgres to fetch WALs from Barman during recovery. If not
set, Barman will copy the WALs required for recovery.

.. note:: 
  When using ``--no-get-wal`` with targets like ``--target-xid``, ``--target-name``, or 
  ``--target-time``, Barman will copy the entire WAL archive to ensure availability.

.. _recovery-recovering-compressed-backups:

Recovering Compressed Backups
-----------------------------

If a backup is compressed using the ``backup_compression`` option, Barman can decompress
it during recovery. 

The process involves a few steps:

1. The compressed backup files are copied to a staging directory on either the local or
   remote server using Rsync. 
2. These files are then decompressed to the recovery destination directory.
3. For remote recovery, configuration files requiring special handling are copied from the
   recovery destination directory to a local temporary directory in the barman node,
   edited and mangled as needed, and then returned to the recovery directory using
   Rsync. For local recovery, the local temporary directory is the recovery destination
   itself, so editing and mangling operations are done in place. This intermediate step
   is necessary because Barman can only access individual files in the recovery
   directory, as the backup directory contains only a compressed tarball file.
4. The staging directory is removed after recovery is complete.

Since Barman does not have knowledge of the deployment environment, it depends on the
``recovery_staging_path`` option to determine an appropriate location for the staging
directory. Set the option in the global/server configuration or use the
``--recovery-staging-path`` option with the barman recover command. Failing to do so
will result in an error, as Barman cannot guess a suitable location on its own.

.. _recovery-recovering-block-level-incremental-backups:

Recovering block-level incremental Backups
------------------------------------------

If you are recovering from a block-level incremental backup, Barman combines the backup
chain using ``pg_combinebackup``. This chain consists of the root backup and all
subsequent incremental backups up to the one being recovered. 

To successfully recover from a block-level incremental backup, you must specify the
``local_staging_path`` in the global/server configuration or use the
``--local-staging-path`` option with the barman recover command. Failing to do so will
result in an error, as Barman cannot automatically determine a suitable staging
location.

The process involves the following steps:

1. Barman creates a synthetic backup by combining the chain of backups. This is done in
   a staging directory on the Barman server using ``pg_combinebackup``. Barman will
   create a subfolder inside the staging directory with the ID of the backup.
2. If the recovery is local, the synthetic backup is moved directly to the target
   location. If it is a remote recovery, the synthetic backup is transferred to the
   target location using Rsync.
3. After the recovery is complete, the temporary subfolder in the local staging
   directory used for combining backups is removed. The local staging directory itself
   is kept.

.. important::
  If any backups in the chain were taken with checksums disabled, but the final backup
  has checksums enabled, the resulting syntethic backup may contain pages with invalid
  checksums. Please refer to the limitations in the `pg_combinebackup documentation <https://www.postgresql.org/docs/17/app-pgcombinebackup.html>`_
  for more details.

.. _recovery-limitation-of-partial-wal-files:

Limitations of .partial WAL files
---------------------------------

When using ``streaming_archiver``, Barman relies on ``pg_receivewal`` to continuously
receive transaction logs from a PostgreSQL server (either master or standby) through the
native streaming replication protocol. By default, ``pg_receivewal`` writes these logs
to files with a ``.partial`` suffix, indicating they are not yet complete. Barman looks
for these ``.partial`` files in the ``streaming_wals_directory``. Once ``pg_receivewal``
completes the file, it removes the ``.partial`` suffix and hands it over to Barman's
``archive-wal`` command for permanent storage and compression.

If the master PostgreSQL server suddenly fails and cannot be recovered, the ``.partial``
file that was streamed to Barman may contain crucial data that might not have been delivered
to the archiving process.

Starting with Barman version 2.10, the ``get-wal`` command can retrieve the content of
the current ``.partial`` WAL file using the ``--partial`` or ``-P`` option. This is
useful for recovery, whether performing a full restore or a point-in-time recovery. When
you initiate a recovery command with ``get-wal`` and without ``--standby-mode``, Barman
will automatically include the ``-P`` option in the ``barman-wal-restore`` command to
handle the ``.partial`` file.

Moreover, ``get-wal`` will check the ``incoming`` directory for any WAL files that have
been sent to Barman but not yet archived.

Recovering from Snapshot Backups
--------------------------------

Barman currently does not support fully automated recovery from snapshot backups. This
limitation arises because snapshot recovery requires provisioning and managing new
infrastructure, a task best handled by dedicated :term:`IAC` solutions like Terraform.

However, you can still use the barman recover command to validate the snapshot recovery
instance and perform post-recovery tasks, such as checking the Postgres configuration for
unsafe settings and configuring any necessary PITR options. The command will also copy
the ``backup_label`` file into place, as this file is not included in the volume
snapshots, and will transfer any required WAL files--unless the ``--get-wal`` recovery
option is specified, in which case it configures the Postgres ``restore_command`` to fetch
the WALs.

If restoring from a backup created with ``barman-cloud-backup``, you should use the
``barman-cloud-restore`` command instead of ``barman recover``.

.. note::
  The same requirements and configurations apply for restore when working with a cloud
  provider. See the ``Requirements and Configuration`` section and the specific cloud
  provider you are working with in the :ref:`Cloud Snapshot Backups <cloud_snapshot_backups>` section.

Recovery Steps
""""""""""""""

1. Provision a new disk for each snapshot taken during the backup.
2. Provision a compute instance to which each disk from step 1 is attached and mounted according to the backup metadata.
3. Use the ``barman recover`` or ``barman-cloud-restore`` command to validate and finalize the recovery.

Steps 1 and 2 are ideally managed by an existing IAC system, but they can also be
performed manually or via a custom script.

Helpful Resources
"""""""""""""""""

`Example recovery script for GCP <https://github.com/EnterpriseDB/barman/blob/master/scripts/prepare_snapshot_recovery.py>`_.

`Example runbook for Azure <https://github.com/EnterpriseDB/barman/blob/master/doc/runbooks/snapshot_recovery_azure.md>`_.

These resources make assumptions about your backup and recovery environment and should be
customized before use in production.

Running the Recovery Command
""""""""""""""""""""""""""""

Once the recovery instance is provisioned and the disks cloned from the backup snapshots
are attached and mounted, execute the barman recover command with the following
additional arguments:

* ``--remote-ssh-command``: The SSH command required to log into the recovery instance.
* ``--snapshot-recovery-instance``: The name of the recovery instance as specified by
  your cloud provider.
* Any additional arguments specific to the snapshot provider.

Example Command
^^^^^^^^^^^^^^^

.. code:: bash
  
  barman recover SERVER_NAME BACKUP_ID REMOTE_RECOVERY_DIRECTORY \
    --remote-ssh-command 'ssh USER@HOST' \
    --snapshot-recovery-instance INSTANCE_NAME

Barman will automatically recognize the backup as a snapshot and verify that the
attached disks were cloned from the corresponding snapshots. It will then prepare
Postgres for recovery by copying the backup label and WALs into place and adjusting the
Postgres configuration with the necessary recovery options.

Provider-Specific Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^^

For GCP:

* ``--gcp-zone``: The availability zone where the recovery instance is located. If
  omitted, Barman will use the ``gcp_zone`` value set in the server config.

For Azure:

* ``--azure-resource-group``: The resource group for the recovery instance. If not
  provided, Barman will refer to the ``azure_resource_group`` value in the server config.

For AWS:

* ``--aws-region``: The AWS region of the recovery instance. If not specified, Barman
  will default to the ``aws_region`` value set in the server config.