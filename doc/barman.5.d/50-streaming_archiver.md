streaming_archiver
:   This option allows you to use the PostgreSQL's streaming protocol to
    receive transaction logs from a server. If set to `on`, Barman expects
    to find `pg_receivewal` (known as `pg_receivexlog` prior to
    PostgreSQL 10) in the PATH (see `path_prefix` option) and that
    streaming connection for the server is working. This activates connection
    checks as well as management (including compression) of WAL files.
    If set to `off` (default) barman will rely only on continuous archiving
    for a server WAL archive operations, eventually terminating any running
    `pg_receivexlog` for the server. Global/Server.
