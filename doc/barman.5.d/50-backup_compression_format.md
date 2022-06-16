backup_compression_format
:   The format pg_basebackup should use when writing compressed backups to
    disk. Can be set to either `plain` or `tar`. If unset then a default of
    `tar` is assumed. The value `plain` can only be used if the server is
    running PostgreSQL 15 or later *and* if `backup_compression_location` is
    `server`. Only supported when `backup_method = postgres`. Global/Server.
