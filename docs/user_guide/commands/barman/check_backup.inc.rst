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
of each backup operation. You can use a backup name or a
shortcut instead of ``BACKUP_ID``.

The exit status reflects the status of the backup after the check:

.. list-table::
    :widths: 15 100
    :header-rows: 1

    * - **Exit code**
      - **Meaning**
    * - ``0``
      - The backup is ``DONE``: all the required WAL files are archived.
    * - ``1``
      - The backup is not ``DONE``, or the check could not be performed. This happens
        when:

        * The backup is ``FAILED``, for example because a required WAL file is missing.
        * The backup is ``WAITING_FOR_WALS``: not all the required WAL files have been
          archived yet, so the backup consistency cannot be verified.
        * The check could not be performed, for example because another process holds
          the backup lock.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

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