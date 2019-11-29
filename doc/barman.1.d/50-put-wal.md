put-wal *\[OPTIONS\]* *SERVER_NAME*
:   Receive a WAL file from a remote server and securely store it into
    the `SERVER_NAME` incoming directory.
    The WAL file is retrieved from the `STDIN`, and must be encapsulated
    in a tar stream together with a `MD5SUMS` file to validate it.
    This command is meant to be invoked through SSH from a remote
    `barman-wal-archive` utility (part of `barman-cli` package).
    Do not use this command directly unless you take full responsibility
    of the content of files.

    -t, --test
    :   test both the connection and the configuration of the
        requested PostgreSQL server in Barman to make sure it is ready to
        receive WAL files.
