sync-info *SERVER_NAME* \[*LAST_WAL* \[*LAST_POSITION*\]\]
:   Collect information regarding the current status of a Barman server, to be
    used for synchronisation purposes. Returns a JSON output representing
    `SERVER_NAME`, that contains: all the successfully finished backup,
    all the archived WAL files, the configuration, last WAL file been read
    from the `xlog.db` and the position in the file.

    LAST_WAL
    :   tells sync-info to skip any WAL file previous to that
        (incremental synchronisation)

    LAST_POSITION
    :   hint for quickly positioning in the `xlog.db` file
        (incremental synchronisation)
