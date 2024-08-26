.. _barman_receive_wal:

``barman receive-wal``
""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    receive-wal
        [ --create-slot ]
        [ --drop-slot ]
        [ --reset ]
        [ --stop ]
        SERVER_NAME

Description
^^^^^^^^^^^

Initiate the streaming of transaction logs for a server. This process uses
``pg_receivewal`` or ``pg_receivexlog`` to receive WAL files from PostgreSQL servers via
the streaming protocol.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``--create-slot``
    Create the physical replication slot configured with the ``slot_name`` configuration
    parameter.

``--drop-slot``
    Drop the physical replication slot configured with the ``slot_name`` configuration
    parameter.

``--reset``
    Reset the status of ``receive-wal``, restarting the streaming from the current WAL file
    of the server.

``--stop``
    Stop the process for the server.
