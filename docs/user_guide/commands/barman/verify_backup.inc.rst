.. _commands-barman-verify-backup:

``barman verify-backup``
""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    verify-backup
        [ { -h | --help } ]
        SERVER_NAME BACKUP_ID

Description
^^^^^^^^^^^

Runs ``pg_verifybackup`` on a backup manifest file (available since Postgres version 13).
For rsync backups, it can be used after creating a manifest file using the
``generate-manifest`` command. Requires ``pg_verifybackup`` to be installed on the
backup server. You can use a shortcut instead of ``BACKUP_ID``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``BACKUP_ID``
    Id of the backup in barman catalog.

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

.. only:: man

    Shortcuts
    ^^^^^^^^^
    
    For some commands, instead of using the timestamp backup ID, you can use the following
    shortcuts or aliases to identify a backup for a given server:
    
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