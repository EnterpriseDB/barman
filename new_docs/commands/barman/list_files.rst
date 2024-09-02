.. _commands-barman-list-files:

``barman list-files``
""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    list-files
        [ --target { data | full | standalone | wal } ]
        SERVER_NAME BACKUP_ID

Description
^^^^^^^^^^^

List all files in a specific backup. You can use a shortcut instead of ``BACKUP_ID``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``BACKUP_ID``
    Id of the backup in barman catalog.

``--target``
    Define specific files to be listed. The possible values are:

    * ``standalone`` (default): List the base backup files, including required WAL files.
    * ``data``: List just the data files.
    * ``wal``: List all the WAL files between the start of the base backup and the end of
      the log or the start of the following base backup (depending on whether the
      specified base backup is the most recent one available).
    * ``full``: same as ``data`` + ``wal``.

.. only:: man

    Shortcuts
    ^^^^^^^^^

    Use shortcuts instead of ``BACKUP_ID``.
    
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