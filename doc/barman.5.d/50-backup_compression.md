backup_compression
:   The compression to be used during the backup process. Only supported when
    `backup_method = postgres`. Can either be unset or `gzip`. If unset then
    no compression will be used during the backup. Global/Server.
