.. _commands:

Commands Reference
==================

Barman has a command-line interface named ``barman``, which is used basically to
interact with Barman's backend.

Before jumping into each of the sub-commands of ``barman``, be aware that ``barman``
has global options available for all of the sub-commands. These options can modify the
behavior of the sub-commands and can be used as follows:

.. include:: commands/barman/barman.rst

Shortcuts
---------

For some commands, you can use the following shortcuts or aliases to identify a backup
for a given server. Specifically, the ``all`` shortcut can be used to identify all
servers:

.. list-table::
    :widths: 25 100
    :header-rows: 1

    * - **Shortcut**
      - **Description**
    * - **all**
      - All available servers
    * - **first/oldest**   
      - Oldest available backup for the server, in chronological order.
    * - **last/latest**
      - Most recent available backup for the server, in chronological order.
    * - **last-full/latest-full**
      - Most recent full backup eligible for a block-level incremental backup using the
        ``--incremental`` option.
    * - **last-failed**
      - Most recent backup that failed, in chronological order.

Exit Statuses
-------------

Status code **0** means **success**, while status code **Non-Zero** means **failure**.

.. _commands-sub-commands:

Sub-Commands
------------

``barman`` exposes several handy operations. This section is intended to describe each
of them.

In the following sections you can find a description of each command implemented by
``barman``. Some of these commands may have more detailed information in another main
section in this documentation. If that is the case, a reference is provided to help you
quickly navigate to it.

.. include:: commands/barman/archive_wal.rst
.. include:: commands/barman/backup.rst
.. include:: commands/barman/check_backup.rst
.. include:: commands/barman/check.rst
.. include:: commands/barman/config_switch.rst
.. include:: commands/barman/config_update.rst
.. include:: commands/barman/cron.rst
.. include:: commands/barman/delete.rst
.. include:: commands/barman/diagnose.rst
.. include:: commands/barman/generate_manifest.rst
.. include:: commands/barman/get_wal.rst
.. include:: commands/barman/keep.rst
.. include:: commands/barman/list_backups.rst
.. include:: commands/barman/list_files.rst
.. include:: commands/barman/list_servers.rst
.. include:: commands/barman/lock_directory_cleanup.rst
.. include:: commands/barman/put_wal.rst
.. include:: commands/barman/rebuild_xlogdb.rst
.. include:: commands/barman/receive_wal.rst
.. include:: commands/barman/recover.rst
.. include:: commands/barman/replication_status.rst
.. include:: commands/barman/show_backup.rst
.. include:: commands/barman/show_servers.rst
.. include:: commands/barman/status.rst
.. include:: commands/barman/switch_wal.rst
.. include:: commands/barman/switch_xlog.rst
.. include:: commands/barman/sync_backup.rst
.. include:: commands/barman/sync_info.rst
.. include:: commands/barman/sync_wals.rst
.. include:: commands/barman/verify_backup.rst
.. include:: commands/barman/verify.rst

``barman-cli`` commands
-----------------------

The ``barman-cli`` package includes a collection of recommended client utilities that 
should be installed alongside the PostgreSQL server. Here are the command references for
both utilities.

.. include:: commands/barman_cli/wal_archive.rst
.. include:: commands/barman_cli/wal_restore.rst