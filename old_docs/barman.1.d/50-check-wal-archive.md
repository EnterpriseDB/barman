check-wal-archive *SERVER_NAME*
:   Check that the WAL archive destination for *SERVER_NAME*
    is safe to use for a new PostgreSQL cluster. With no
    optional args (the default) this will pass if the WAL
    archive is empty and fail otherwise.

    --timeline [TIMELINE]
    :    A positive integer specifying the earliest timeline for which
         associated WALs should cause the check to fail.
         The check will pass if all WAL content in the archive relates
         to earlier timelines. If any WAL files are on this timeline or
         greater then the check will fail.
