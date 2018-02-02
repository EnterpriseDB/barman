get-wal *\[OPTIONS\]* *SERVER_NAME* *WAL_ID*
:   Retrieve a WAL file from the `xlog` archive of a given server.
    By default, the requested WAL file, if found, is returned as
    uncompressed content to `STDOUT`. The following options allow
    users to change this behaviour:

    -o *OUTPUT_DIRECTORY*
    :   destination directory where the `get-wal` will deposit the requested WAL

    -z
    :   output will be compressed using gzip

    -j
    :   output will be compressed using bzip2

    -p *SIZE*
    :   peek from the WAL archive up to *SIZE* WAL files, starting
        from the requested one. 'SIZE' must be an integer >= 1.
        When invoked with this option, get-wal returns a
        list of zero to 'SIZE' WAL segment names, one per row.
