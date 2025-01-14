.. _commands-barman-list-backups:

``barman list-backups``
"""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    list-backups
        [ { -h | --help } ]
        [ --minimal ]
        SERVER_NAME

Description
^^^^^^^^^^^

Display the available backups for a server. This command is useful for retrieving both
the backup ID and the backup type. You can find details about this command in
:ref:`Catalog usage <catalog-usage-list-backups>`.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--minimal``
    Machine readable output.

.. only:: man

    Shortcuts
    ^^^^^^^^^

    Use shortcuts instead of ``SERVER_NAME``.

    .. list-table::
        :widths: 25 100
        :header-rows: 1
    
        * - **Shortcut**
          - **Description**
        * - **all**
          - All available servers
