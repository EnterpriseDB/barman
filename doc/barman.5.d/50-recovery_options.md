recovery_options
:   Options for recovery operations. Currently only supports `get-wal`.
    `get-wal` activates generation of a basic `restore_command` in
    the resulting `recovery.conf` file that uses the `barman get-wal`
    command to fetch WAL files directly from Barman's archive of WALs.
    Comma separated list of values, default empty. Global/Server.
