switch-wal *SERVER_NAME*
:   Execute pg_switch_wal() on the target server (from PostgreSQL 10),
    or pg_switch_xlog (for PostgreSQL 8.3 to 9.6).

    --force
    :   Forces the switch by executing CHECKPOINT before pg_switch_xlog().
        *IMPORTANT:* executing a CHECKPOINT might increase I/O load on
        a PostgreSQL server. Use this option with care.

    --archive
    :   Wait for one xlog file to be archived.
        If after a defined amount of time (default: 30 seconds) no xlog
        file is archived, Barman will teminate with failure exit code.

    --archive-timeout *TIMEOUT*
    :   Specifies the amount of time in seconds (default: 30 seconds)
        the archiver will wait for a new xlog file to be archived
        before timing out.
