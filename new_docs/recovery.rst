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

Tablespace Remapping
--------------------

Use ``--tablespace NAME:DIRECTORY`` to remap tablespaces to a new location. Barman will
attempt to create the destination directory if it doesn't exist.

.. important::
  If you do not use the ``--tablespace`` option during recovery and your backup
  includes tablespaces, Barman will restore them to their original paths. 
  
  For local recovery, this can be problematic because the original path is possibly
  still occupied by the backed up server files, so it is crucial to specify the
  ``--tablespace`` option to avoid using potentially compromised paths. 
  
  For remote recovery, tablespaces will be restored to paths that match those specified
  when the tablespaces were originally created.
  
  Ensure proper remapping as needed to prevent issues related to path conflicts or data
  corruption.

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

Fetching WALs from Barman
-------------------------

Use ``--get-wal`` to configure Postgres to fetch WALs from Barman during recovery. If not
set, Barman will copy the WALs required for recovery.

.. note:: 
  When using ``--no-get-wal`` with targets like ``--target-xid``, ``--target-name``, or 
  ``--target-time``, Barman will copy the entire WAL archive to ensure availability.

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