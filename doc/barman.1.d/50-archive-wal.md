archive-wal *SERVER_NAME*
:   Get any incoming xlog file (both through standard `archive_command`
    and streaming replication, where applicable) and moves them in the
    WAL archive for that server. If necessary, apply compression when
    requested by the user.
