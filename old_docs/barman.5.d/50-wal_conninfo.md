wal_conninfo
:   A connection string which, if set, will be used by Barman to connect to the
    Postgres server when checking the status of the replication slot used for
    receiving WALs. If left unset then Barman will use the connection string
    defined by `wal_streaming_conninfo`. If `wal_conninfo` is set but
    `wal_streaming_conninfo` is unset then `wal_conninfo` will be ignored.

    Scope: Server/Model.
