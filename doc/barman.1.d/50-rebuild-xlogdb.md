rebuild-xlogdb *SERVER_NAME*
:   Perform a rebuild of the WAL file metadata for `SERVER_NAME`
    (or every server, using the `all` shortcut) guessing it from
    the disk content. The metadata of the WAL archive is contained
    in the `xlog.db` file, and every Barman server has its own copy.
