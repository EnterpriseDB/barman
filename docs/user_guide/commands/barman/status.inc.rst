.. _commands-barman-status:

``barman status``
"""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    status
        [ { -h | --help } ]
        SERVER_NAME [ SERVER_NAME ... ]

Description
^^^^^^^^^^^

Display information about a server's status, including details such as the state,
Postgres version, WAL information, available backups and more.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

.. only:: man

    Shortcuts
    ^^^^^^^^^

    .. list-table::
        :widths: 25 100
        :header-rows: 1
    
        * - **Shortcut**
          - **Description**
        * - **all**
          - All available servers
