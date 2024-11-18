receive-wal *SERVER_NAME*
:   Start the stream of transaction logs for a server.
    The process relies on `pg_receivewal`/`pg_receivexlog` to receive
    WAL files from the PostgreSQL servers through the streaming protocol.

    --stop
    :   stop the receive-wal process for the server

    --reset
    :   reset the status of receive-wal, restarting the streaming
        from the current WAL file of the server

    --create-slot
    :   create the physical replication slot configured with the
        `slot_name` configuration parameter

    --drop-slot
    :   drop the physical replication slot configured with the
        `slot_name` configuration parameter
