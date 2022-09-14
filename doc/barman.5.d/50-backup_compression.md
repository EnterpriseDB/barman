backup_compression
:   The compression to be used during the backup process. Only supported when
    `backup_method = postgres`. Can either be unset or `gzip`,`lz4` or `zstd`. If unset then
    no compression will be used during the backup. Use of this option requires that the
    CLI application for the specified compression algorithm is available on the Barman
    server (at backup time) and the PostgreSQL server (at recovery time). Note that
    the `lz4` and `zstd` algorithms require PostgreSQL 15 (beta) or later. Global/Server.
