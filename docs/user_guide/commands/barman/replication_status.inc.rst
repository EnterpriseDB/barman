.. _commands-barman-replication-status:

``barman replication-status``
"""""""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    replication-status
        [ { -h | --help } ]
        [ --minimal ]
        [ --source { backup-host | wal-host } ]
        [ --target { hot-standby | wal-streamer | all } ]
    SERVER_NAME

Description
^^^^^^^^^^^

Display real-time information and status of any streaming clients connected to the
specified server. Specify ``all`` shortcut to diplay information for all configured
servers.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--minimal``
    Machine readable output.

``--source``
    The possible values are:

    * ``backup-host`` (default): List clients using the backup connection information
      for a server.
    * ``wal-host``: List clients using the WAL streaming connection information for a
      server.

``--target``
    The possible values are:

    * ``hot-standby``: List only hot standby servers.
    * ``wal-streamer``: List only WAL streaming clients, such as ``pg_receivewal``.
    * ``all`` (default): List all streaming clients.

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
