.. _commands-barman-check-backup:

``barman check-backup``
"""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    check-backup
        [ { -h | --help } ]
        SERVER_NAME BACKUP_ID

Description
^^^^^^^^^^^

Check that all necessary WAL files for verifying the consistency of a physical backup are
properly archived. This command is automatically executed by the cron job and at the end
of each backup operation. You can use a shortcut instead of ``BACKUP_ID``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``BACKUP_ID``
    Id of the backup in barman catalog.

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

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
          - Most recent full backup taken with methods ``rsync`` or ``postgres``.
        * - **last-failed**
          - Most recent backup that failed, in chronological order.