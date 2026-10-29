.. _commands-barman-generate-manifest:

``barman generate-manifest``
""""""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    generate-manifest
        [ { -h | --help } ]
        SERVER_NAME BACKUP_ID

Description
^^^^^^^^^^^

Generates a ``backup_manifest`` file for a backup. You can use a backup name or a
shortcut instead of ``BACKUP_ID``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``BACKUP_ID``
    ID or name of the backup in the barman catalog. If more than one backup has the
    same name, the command fails and lists the matching backup IDs.

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