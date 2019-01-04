put-wal *SERVER_NAME*
:   Receive a WAL file from a remote server and securely store it into
    the `SERVER_NAME` incoming directory.
    The WAL file is retrieved from the `STDIN`, and must be encapsulated
    in a tar stream together with a `MD5SUMS` file to validate it.
    This command is meant to be invoked through SSH from a remote
    `barman-wal-archive` utility (part of barman-cli).
    Do not use this command directly unless you take full responsibility
    of the content of files.
