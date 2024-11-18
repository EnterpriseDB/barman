wal_streaming_conninfo
:   A connection string which, if set, will be used by Barman to connect to
    the Postgres server when receiving WAL segments via the streaming
    replication protocol. If left unset then Barman will use the connection
    string defined by `streaming_conninfo` for receiving WAL segments.

    Scope: Server/Model.
