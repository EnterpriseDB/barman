.. _recovery:

Recovery
========

The restore command is used to restore an entire Postgres server from a backup created
with the backup command. When creating a backup, a ``backup_id`` is assigned, which
uniquely identifies the backup. To use the restore command, run:

``barman restore [OPTIONS] SERVER_NAME BACKUP_ID DESTINATION_PATH``

It is possible to restore a backup by specifying the ``backup_id`` as ``auto``. In this
case, Barman will automatically choose the most suitable backup from the catalog. If no
recovery target is provided, it will select the most recent available backup by default.
If a recovery target is specified, Barman will use it to identify the appropriate backup
based on the recovery criteria:

* ``target_time``: Barman will retrieve the most recent backup available up to
  ``target_time``, e.g., ``2025-01-21 10:00:00``.

* ``target_lsn``: Barman will retrieve the most recent backup available up to
  ``target_lsn``, e.g., ``3/64000000``.

* ``target_tli``: Barman will retrieve the most recent backup available from timeline
  ``target_tli``, e.g., ``2``.

* ``target_tli`` combined with one of the other two targets: Barman will retrieve the
  most recent backup available up to the ``target_lsn`` or ``target_time`` which belongs
  to the ``target_tli``, e.g., ``2025-01-21 10:00:00`` from timeline ``2``.

.. note::
  * Refer to :ref:`concepts-barman-concepts-restore-and-recover` for a clearer
    understanding of the recovery concept in Barman.
  * The backup is restored to the specified directory, which will be ready to start a
    Postgres instance.
  * Use the ``list-backups`` command to find the specific backup ID you need.
  * Barman does not track symbolic links inside PGDATA (except for tablespaces).
    Ensure you manage symbolic links and include them in your disaster recovery plans.
  * When specifying the ``backup_id`` as ``auto``, Barman does not consider
    the :ref:`backup_method <configuration-options-backups-backup-method>`. If the
    selected backup requires specific arguments or doesn't support certain options, the
    restoration may fail. In such cases, you will receive an error message that can help
    adjust the restore command accordingly. For example,
    ``--snapshot-recovery-instance`` is required when restoring a snapshot backup.

.. danger::
  Do not run the restore command with the destination directory set to a location where
  a Postgres instance is actively running. If you intend to reuse that directory, ensure
  that Postgres is fully stopped before initiating recovery, including recovery of
  tablespace directories.

.. _recovery-local-recovery:

Local recovery
--------------

In the absence of ``--remote-ssh-command``, Barman restores the backup locally to
the path specified in ``DESTINATION_PATH``.

All files and directories will be owned by the ``barman`` user. Therefore, there are two
options when starting Postgres to perform the recovery:

1. Start Postgres as the ``barman`` user. In this case, if using ``--get-wal``, the
   ``restore_command`` can invoke ``get-wal`` without requiring ``sudo``.
2. Start Postgres as the ``postgres`` user. In this case, you must change the ownership
   of all files and directories restored (this includes PGDATA and any tablespaces) to
   the ``postgres`` user before starting the server. If using ``--get-wal``, the
   ``restore_command`` must use ``sudo`` to run ``get-wal`` as the ``barman`` user.

Check the :ref:`Fetching WALs <recovery-fetching-wals-from-barman>` for more information
on settings for WAL restore.

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

.. danger::
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
* ``--target-lsn``: Recover to a Log Sequence Number.
* ``--target-name``: Recover to a named restore point.
* ``--target-tli``: Recover to a specific timeline.
* ``--target-immediate``: End recovery when a consistent state is reached.

.. note::
  * Recovery targets must be a value after the end of the backup. To recover to a
    point in time within a backup, use the previous backup.
  * Timezone defaults to the Barman host if not specified in ``--target-time``'s
    timestamp.
  * Use ``--exclusive`` to control whether to stop right before the target or including
    the target.
  * ``--target-tli`` sets the target timeline. Use numeric IDs or shortcut values
    (``latest`` or ``current``).
  * When at least one `--target-*` option is specified, a ``recovery.signal`` file is
    created by Barman when restoring the backup, which signals the server to start a
    targeted recovery.
  * When specifying the ``backup_id`` as ``auto``, the only recovery targets
    allowed are: ``--target-time``, ``--target-lsn`` and ``--target-tli``. Note that
    ``--target-time`` and ``--target-lsn`` are mutually exclusive, while
    ``--target-tli`` can be used independently or together with ``--target-time`` or
    ``--target-lsn``.

The previous targets can be used with a ``--target-action`` which can take these values:

* ``shutdown``: Shut down Postgres when the target is reached.
* ``pause``: Pause Postgres for inspection when the target is reached.
* ``promote``: Promote Postgres to primary when the target is reached.

You can also configure the instance as a standby by calling ``--standby-mode``. After
the backup is restored, ensure you modify the configuration to connect to the intended
upstream node before starting the restored node in recovery mode.

.. note::
  * When ``--standby-mode`` is specified, a ``standby.signal`` file is created instead
    of a ``recovery.signal`` file.
  * When using ``--standby-mode``, although possible, you are not expected to set any of
    the ``--target-*`` options.

.. seealso::
  For more information regarding Postgres recovery behavior, refer to
  `Archive Recovery <https://www.postgresql.org/docs/current/runtime-config-wal.html#RUNTIME-CONFIG-WAL-ARCHIVE-RECOVERY>`_
  and `Recovery Target <https://www.postgresql.org/docs/current/runtime-config-wal.html#RUNTIME-CONFIG-WAL-RECOVERY-TARGET>`_

.. _recovery-fetching-wals-from-barman:

Fetching WALs from Barman
-------------------------

Use ``--get-wal`` to configure Postgres to fetch WALs from Barman during recovery. If not
set, Barman will copy all the WALs required for Postgres recovery as part of the restore
command.

.. note:: 
  When using ``--no-get-wal`` with targets like ``--target-xid``, ``--target-name``, or 
  ``--target-time``, Barman will copy the entire WAL archive to ensure availability.

Another option is to include ``get-wal`` inside the ``recovery_options`` configuration
at the global/server level prior to a recovery operation to retrieve WAL files during
the recovery process without the need to specifying the ``--get-wal``, effectively
turning the Barman server into a WAL hub for your servers.

.. code-block:: text

  recovery_options = 'get-wal'

If ``get-wal`` is included during restore, Barman will set up the ``restore_command``
to use either ``barman get-wal`` or ``barman-wal-restore`` to retrieve the required WAL
files, depending on whether the recovery is local or remote.

If ``get-wal`` is specified in ``recovery_options`` but not needed during a specific
recovery, you can disable it using the ``--no-get-wal`` option with the ``barman
restore`` command.

Using ``get-wal`` for local recovery
""""""""""""""""""""""""""""""""""""

Here's an example of a ``restore_command`` for local recovery:

.. code-block:: text

  restore_command = 'sudo -u barman barman get-wal SERVER %f > %p'

Remember that the :ref:`barman get-wal <commands-barman-get-wal>` command should always
be executed as the ``barman`` user, with the necessary permissions to access WAL files
from the catalog, which is why ``sudo -u barman`` is used in this example.

To allow the ``postgres`` user to run the ``get-wal`` command as the ``barman`` user, 
you can add the following line to the ``/etc/sudoers`` file (replace SERVER with the
actual server name):

.. code-block:: text

  postgres ALL=(barman) NOPASSWD: /usr/bin/barman get-wal SERVER *

Using ``get-wal`` for remote recovery
"""""""""""""""""""""""""""""""""""""

For remote recovery, setting ``recovery_options`` to ``get-wal`` will create a
``restore_command`` using the :ref:`commands-barman-cli-barman-wal-restore` script,
which is designed to handle SSH connection errors more robustly.

This script offers useful features like automatic compression and decompression of WAL
files and the ``peek`` feature, allowing you to retrieve upcoming WAL files while
Postgres is processing earlier ones, optimizing bandwidth between Postgres and Barman.

``barman-wal-restore`` is included in the ``barman-cli`` package. Here's an example of
a ``restore_command`` for **remote recovery**:

.. code-block:: text

  restore_command = 'barman-wal-restore -U barman backup SERVER_NAME %f %p'

Here, ``backup`` refers to the host where Barman is installed. Since it communicates via
SSH, SSH key authentication is required for the ``postgres`` user to log in as
``barman`` on the backup server. If you need to use a non-default SSH port, you can
specify it with the ``--port`` option.

To verify that ``barman-wal-restore`` can connect to the Barman server and that the
required Postgres server is set up to send WAL files, use the following command:

.. code-block:: text

  barman-wal-restore --test backup pg DUMMY DUMMY

Here, ``backup`` refers to the host where Barman is installed, ``pg`` is the name of the
Postgres server configured in Barman, and ``DUMMY`` acts as a placeholder (the script
needs two arguments for the WAL file name and destination directory, which will be
ignored).

If everything is set up correctly, you should see:

.. code-block:: text

  Ready to retrieve WAL files from the server pg

For further details on the ``barman-wal-restore`` command, type
``man barman-wal-restore`` on the host where ``barman-cli`` was installed or refer to
the :ref:`commands-barman-cli-barman-wal-restore` command reference.

.. tip:: 
  When both the ``pg_wal`` directory and the ``spool`` directory are located on the same
  filesystem, serving WAL files will be faster because the files are renamed rather than
  copied. However, if these directories are on different filesystems, there will be no 
  performance improvement, as the operation will involve both copying the file and then
  removing the original. Be mindful of the filesystem locations to optimize WAL file
  management efficiency.

.. _recovery-recovering-encrypted-backups:

Recovering Encrypted Backups
-----------------------------

Encrypted backups and WALs are decrypted during the restore phase, before they are
copied to the final destination. During the restore, a command to fetch the private key's
passphrase must be present in ``encryption_passphrase_command``. This command must
output the passphrase to standard output and can be used to retrieve it from a secure
location such as a password vault, an external key management service, or a file.

These are some examples of how to set the passphrase command:

* Example reading from an environment variable:

  .. code-block:: ini

      encryption_passphrase_command="echo $BARMAN_PASSPHRASE"

* Example reading from a file:

  .. code-block:: ini

      encryption_passphrase_command="cat /path/to/barman_passphrase"

* Example reading from HashiCorp Vault:

  .. code-block:: ini  

      encryption_passphrase_command="vault kv get -field=<FIELD> <KEY>"

* Example reading from AWS Secret Manager:

  .. code-block:: ini

      encryption_passphrase_command="aws secretsmanager get-secret-value --secret-id
      <SECRET_NAME>  --profile <AWS_PROFILE> --output text --query SecretString | jq -r
      '.<SECRET_KEY>'"

The decryption of backups happens as follows:

1. The backup is decrypted into a staging directory on the Barman server. This location
   is defined by the ``staging_path`` and ``staging_location`` options.
2. If any additional operations are required — such as decompression or combination
   (in the case of incremental backups) — they are performed using the staging
   directory's content as source. Otherwise, the decrypted files are copied directly
   to the final destination.
3. The staging directory is removed after the restore is complete.

Decryption of WAL files depends on how they are retrieved during recovery:

1. If using the ``--no-get-wal`` option (default), all required WAL files are
   decrypted into the staging directory and then copied to the final destination. That
   means the ``encryption_passphrase_command`` is invoked once and the output is reused
   for all WAL files. Also, the command is only required during the execution of the
   ``barman restore`` command, and not during the Postgres recovery process.
2. Using the ``--get-wal`` option, WAL files are served to the Postgres server
   when needed during recovery process. In this scenario, Barman decrypts each WAL
   file locally before sending it to the Postgres server. This also means that
   ``encrytion_passphrase_command`` is invoked once for each WAL file being fetched
   through the ``restore_command``.

.. _recovery-recovering-compressed-backups:

Recovering Compressed Backups
-----------------------------

If a backup is compressed using the ``backup_compression`` option, Barman can decompress
it during restore. 

The process involves a few steps:

1. The compressed backup files are copied to a staging directory on either the local or
   remote server using Rsync. 
2. These files are then decompressed to the restore destination directory.
3. For remote recovery, configuration files requiring special handling are copied from the
   restore destination directory to a local temporary directory in the barman node,
   edited and mangled as needed, and then returned to the restore directory using
   Rsync. For local recovery, the local temporary directory is the restore destination
   itself, so editing and mangling operations are done in place. This intermediate step
   is necessary because Barman can only access individual files in the restore
   directory, as the backup directory contains only a compressed tarball file.
4. The staging directory is removed after the restore is complete.

Since Barman does not have knowledge of the deployment environment, it depends on the
``staging_path`` and ``staging_location`` options to determine an appropriate location
for the staging directory. Set the option in the global/server configuration or use the
``--staging-path`` and ``staging-location`` options with the ``barman restore`` command.
Failing to do so will result in an error, as Barman cannot guess a suitable location on
its own.

.. _recovery-recovering-block-level-incremental-backups:

Recovering block-level incremental Backups
------------------------------------------

If you are recovering from a block-level incremental backup, Barman combines the backup
chain using ``pg_combinebackup``. This chain consists of the root backup and all
subsequent incremental backups up to the one being recovered. 

To successfully recover from a block-level incremental backup, you must specify the
``staging_path`` and ``staging_location`` options in the global/server configuration or
use the equivalent ``--staging-path`` and ``--staging-location`` options with the
``barman restore`` command. Failing to do so will result in an error, as Barman cannot
automatically determine a suitable staging location.

The process involves the following steps:

1. Barman creates a synthetic backup by combining the chain of backups. This is done in
   a staging directory on the Barman server using ``pg_combinebackup``. Barman will
   create a subfolder inside the staging directory with the ID of the backup.
2. If the recovery is local, the synthetic backup is moved directly to the target
   location. If it is a remote recovery, the synthetic backup is transferred to the
   target location using Rsync.
3. After the restore is complete, the temporary subfolder in the local staging
   directory used for combining backups is removed. The local staging directory itself
   is kept.

.. important::
  If any backups in the chain were taken with checksums disabled, but the final backup
  has checksums enabled, the resulting syntethic backup may contain pages with invalid
  checksums. Please refer to the limitations in the
  `pg_combinebackup documentation <https://www.postgresql.org/docs/current/app-pgcombinebackup.html>`_
  for more details.

.. _recovery-limitation-of-partial-wal-files:

Limitations of .partial WAL files
---------------------------------

When using ``streaming_archiver``, Barman relies on ``pg_receivewal`` to continuously
receive transaction logs from a Postgres server (either master or standby) through the
native streaming replication protocol. By default, ``pg_receivewal`` writes these logs
to files with a ``.partial`` suffix, indicating they are not yet complete. Barman looks
for these ``.partial`` files in the ``streaming_wals_directory``. Once ``pg_receivewal``
completes the file, it removes the ``.partial`` suffix and hands it over to Barman's
``archive-wal`` command for permanent storage and compression.

If the master Postgres server suddenly fails and cannot be recovered, the ``.partial``
file that was streamed to Barman may contain crucial data that might not have been
delivered to the archiving process.

Starting with Barman version 2.10, the ``get-wal`` command can retrieve the content of
the current ``.partial`` WAL file using the ``--partial`` or ``-P`` option. This is
useful for recovery, whether performing a full restore or a point-in-time recovery. When
you initiate a restore command with ``get-wal`` and without ``--standby-mode``, Barman
will automatically include the ``-P`` option in the ``barman-wal-restore`` command to
handle the ``.partial`` file.

Moreover, ``get-wal`` will check the ``incoming`` directory for any WAL files that have
been sent to Barman but not yet archived.

.. _recovery-managing-external-configuration-files:

Managing external configuration files
-------------------------------------

Barman restores external configuration files differently depending on how the backup was
originally taken. When restoring a ``rsync`` backup, external files are restored into
the :term:`PGDATA` directory via rsync, and not in the original location. A warning is issued regarding potentially
risky settings, including the ones related to configuration files. In contrast, when
restoring a ``postgres`` backup, external files are not restored as they were not backed up. A warning is
provided to inform the user about the files that were not restored.

Refer to the :ref:`Managing external configuration files <backup-managing-external-configuration-files>`
section in the backup chapter to understand how external files are handled when
creating a backup.

.. _recovery-recovering-from-snapshot-backups:

Recovering from Snapshot Backups
--------------------------------

Barman currently does not support fully automated recovery from snapshot backups. This
limitation arises because snapshot recovery requires provisioning and managing new
infrastructure, a task best handled by dedicated :term:`IAC` solutions like Terraform
or OpenTofu.

However, you can still use the barman restore command to validate the snapshot recovery
instance and perform post-recovery tasks, such as checking the Postgres configuration for
unsafe settings and configuring any necessary PITR options. The command will also copy
the ``backup_label`` file into place, as this file is not included in the volume
snapshots, and will transfer any required WAL files--unless the ``--get-wal`` recovery
option is specified, in which case it configures the Postgres ``restore_command`` to fetch
the WALs.

If restoring from a backup created with ``barman-cloud-backup``, you should use the
``barman-cloud-restore`` command instead of ``barman restore``.

.. note::
  The same requirements and configurations apply for restore when working with a cloud
  provider. See the ``Requirements and Configuration`` section and the specific cloud
  provider you are working with in the 
  :ref:`Cloud Snapshot Backups <backup-cloud-snapshot-backups>` section.

Recovery Steps
""""""""""""""

1. Provision a new disk for each snapshot taken during the backup.
2. Provision a compute instance to which each disk from step 1 is attached and mounted
   according to the backup metadata.
3. Use the ``barman restore`` or ``barman-cloud-restore`` command to validate and
   finalize the recovery.

Steps 1 and 2 are ideally managed by an existing IAC system, but they can also be
performed manually or via a custom script.

Helpful Resources
"""""""""""""""""

`Example recovery script for GCP <https://github.com/EnterpriseDB/barman/blob/master/scripts/prepare_snapshot_recovery.py>`_.

`Example runbook for Azure <https://github.com/EnterpriseDB/barman/blob/master/scripts/runbooks/snapshot_recovery_azure.md>`_.

These resources make assumptions about your backup and recovery environment and should be
customized before use in production.

Running the restore command
""""""""""""""""""""""""""""

Once the recovery instance is provisioned and the disks cloned from the backup snapshots
are attached and mounted, execute the barman restore command with the following
additional arguments:

* ``--remote-ssh-command``: The SSH command required to log into the recovery instance.
* ``--snapshot-recovery-instance``: The name of the recovery instance as specified by
  your cloud provider.
* Any additional arguments specific to the snapshot provider.

Example Command
^^^^^^^^^^^^^^^

.. code:: bash
  
  barman restore SERVER_NAME BACKUP_ID REMOTE_RECOVERY_DIRECTORY \
    --remote-ssh-command 'ssh USER@HOST' \
    --snapshot-recovery-instance INSTANCE_NAME

Barman will automatically recognize the backup as a snapshot and verify that the
attached disks were cloned from the corresponding snapshots. It will then prepare
Postgres for recovery by copying the backup label and WALs into place and adjusting the
Postgres configuration with the necessary recovery options.

Provider-Specific Arguments
^^^^^^^^^^^^^^^^^^^^^^^^^^^

For GCP:

* ``--gcp-zone``: The availability zone where the recovery instance is located. If
  omitted, Barman will use the ``gcp_zone`` value set in the server config.

For Azure:

* ``--azure-resource-group``: The resource group for the recovery instance. If not
  provided, Barman will refer to the ``azure_resource_group`` value in the server config.

For AWS:

* ``--aws-region``: The AWS region of the recovery instance. If not specified, Barman
  will default to the ``aws_region`` value set in the server config.