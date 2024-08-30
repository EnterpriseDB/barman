.. _barman_check:

Barman check
============

The ``check`` command verifies the connection to a specified server and ensures the server
configuration is coherent. This command performs a series of checks to validate the
proper setup of SSH and Postgres connections are functioning, the existence of backup directories,
and the integrity of the WAL archive. It also verifies Postgres configurations, retention
policies, backup validity, and WAL archiving processes. It identifies and reports any
detected issues, making it one of the most critical features provided by Barman.

Running this command will:

* Ensure that WAL archiving is correctly configured.
* Validate the Postgres connection and necessary privileges.
* Verify that all required directories exist.
* Check that retention policies are properly set.
* Confirm the validity of backups and WAL files.
* Detect configuration errors and archiver issues.

Usage
-----

To use the ``check`` command, run:

.. code-block:: bash

  barman check <server_name>

To run a check on all configured servers, use:

.. code-block:: bash

  barman check all

.. tip::
  Integrate the ``check`` command with your alerting and monitoring infrastructure to
  ensure continuous oversight of your backup environment. The ``--nagios`` option is
  useful for creating plugins for Nagios/Icinga, allowing you to monitor the status of
  your servers seamlessly. For example, you can run:

.. code-block:: text

    barman check --nagios <server_name>

`This will output results in a format compatible with Nagios, facilitating integration
into your existing monitoring setup.`

.. tip::
  For other backup and server-specific checks, you can use the following commands:
   
  * ``barman check-backup``: Checks that all necessary WAL files for verifying the
    consistency of a physical backup are properly archived. This command is used by the
    ``cron`` job and is automatically executed after each backup operation. See
    :ref:`barman_check_backup` for more details.
  * ``barman check-wal-archive``: Checks that the WAL archive destination for a server
    is suitable for use with a new PostgreSQL cluster. See
    :ref:`barman_check_wal_archive` for more details.

Understanding the output
------------------------

Running ``barman check`` will display an output similar to the following:

.. code-block:: text

  $ barman check example
  Server example:
      PostgreSQL: OK
      superuser or standard user with backup privileges: OK
      PostgreSQL streaming: OK
      wal_level: OK
      replication slot: OK
      directories: OK
      retention policy settings: OK
      backup maximum age: OK (no last_backup_maximum_age provided)
      compression settings: OK
      configuration files: OK
      pg_basebackup: OK
      pg_basebackup compatible: OK
      received WAL files: OK
      archiving: OK
      archive_mode: OK
      archive_command: OK
      continuous archiving: OK
      archive_timeout: OK

The output label can vary depending on the status of each check. For example, if a
check fails, it will be marked as ``FAILED`` and may include a hint to help troubleshoot
the issue. If a check passes but with warnings, it will be marked as ``WARNING`` with
additional context.

The ``barman check`` command performs the checks for each of the following aspects of
Barman functioning:

**WAL Archive**

* Ensures that WAL archiving is set up correctly.
* Checks the number of WAL files in the incoming and streaming directories.

.. note::
  If ``archiver = off`` in the Barman configuration and there are WALs in the incoming
  directory, the check will fail. This happens because WALs in the incoming directory
  suggest PostgreSQL is still using ``archive_command`` or switched from ``archiver`` to
  ``streaming_archiver``, leaving WALs unsaved in Barman's archive.

  To resolve this, you must determine if the WALs in incoming are necessary or can be
  safely deleted. This can be done by:

  1. Checking if the WALs are newer than the ``begin_wal`` of the oldest backup.
  2. Verifying if these WALs are already in Barman's archive (usually ``true`` if
     ``streaming_archiver`` is configured).

  The same issue applies if ``streaming_archiver = off`` and WALs are found in the
  streaming directory.

**Postgres Connection**

* Validates the Postgres connection.
* Ensures that the server version is supported.
* Checks for necessary privileges and streaming support.

**Local Tools Validity**

* Ensures that local tools for taking backups and receiving WALs will work correctly
  with the version of the database server, such as ``pg_basebackup`` for taking backups,
  and ``pg_receivewal`` for streaming WAL files.

**Directory**

* Ensures that all necessary backup directories exist.
* Creates directories if they do not exist.

**Retention Policy**

* Validates the retention policy settings.

**Backup Validity**

* Ensures that the backup validity requirements are satisfied.
* Checks the maximum age and minimum size of backups.

**WAL Validity**

* Ensures that WAL archiving requirements are met.
* Checks the maximum age and size of WAL files.

**Configuration**

* Inspects the server's message list for error messages.
* Outputs any errors found.

**Identity**

* Verifies that the system ID retrieved from the streaming connection matches the one
  from the standard connection and the one stored on disk.

**Archiver Errors**

* Inspects the errors directory for the presence of archiving errors.
