.. _commands-barman-sync-backup:

``barman sync-backup``
""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    sync-backup SERVER_NAME BACKUP_ID

Description
^^^^^^^^^^^

This command synchronizes a passive node with its primary by copying all files from a
backup present on the server node. It is available only for passive nodes and uses
the ``primary_ssh_command`` option to establish a secure connection with the primary
node. You can use a shortcut instead of ``BACKUP_ID``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``BACKUP_ID``
    Id of the backup in barman catalog.

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