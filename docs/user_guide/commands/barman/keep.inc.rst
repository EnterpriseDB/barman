.. _commands-barman-keep:

``barman keep``
"""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    keep
        { --release | --status | --target { full | standalone } }
        SERVER_NAME BACKUP_ID
        

Description
^^^^^^^^^^^

Mark the specified backup with a ``target`` as an archival backup to be retained
indefinitely, overriding any active retention policies. You can also check the keep
``status`` of a backup and ``release`` the keep mark from a backup. You can use a
shortcut instead of ``BACKUP_ID``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``BACKUP_ID``
    Id of the backup in barman catalog.

``--release``
    Release the keep mark from this backup. This will remove its archival status and
    make it available for deletion, either directly or by retention policy.

``--status``
    Report the archival status of the backup. The status will be either ``full`` or
    ``standalone`` for archival backups, or ``nokeep`` for backups that have not been
    designated as archival.

``--target``
    Define the recovery target for the archival backup. The possible values are:

    * ``full``: The backup can be used to recover to the most recent point in time. To
      support this, Barman will keep all necessary WALs to maintain the backup's
      consistency as well as any subsequent WALs.
    * ``standalone``: The backup can only be used to restore the server to its state at the
      time of the backup. Barman will retain only the WALs required to ensure the
      backup's consistency.

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