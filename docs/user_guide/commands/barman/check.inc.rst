.. _commands-barman-check:

``barman check``
""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    check
        [ { -h | --help } ]
        [ --nagios ]
        SERVER_NAME

Description
^^^^^^^^^^^

Display status information about a server, such as SSH connection, Postgres version,
configuration and backup directories, archiving and streaming processes, replication
slots, and more. Use ``all`` as shortcut to show diagnostic information for all
configured servers.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--nagios``
    Nagios plugin compatible output.

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
