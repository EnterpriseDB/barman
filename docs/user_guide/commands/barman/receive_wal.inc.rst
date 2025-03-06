.. _commands-barman-receive-wal:

``barman receive-wal``
""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    receive-wal
        [ --create-slot ]
        [ --if-not-exists ]
        [ --drop-slot ]
        [ { -h | --help } ]
        [ --reset ]
        [ --stop ]
        SERVER_NAME

Description
^^^^^^^^^^^

Initiate the streaming of transaction logs for a server. This process uses
``pg_receivewal`` or ``pg_receivexlog`` to receive WAL files from Postgres servers via
the streaming protocol.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``--create-slot``
    Create the physical replication slot configured with the ``slot_name`` configuration
    parameter.

``--if-not-exists``
    Do not error out when --create-slot is specified and a slot with the specified name
    already exists.

``--drop-slot``
    Drop the physical replication slot configured with the ``slot_name`` configuration
    parameter.

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``--reset``
    Reset the status of ``receive-wal``, restarting the streaming from the current WAL file
    of the server.

``--stop``
    Stop the process for the server.
