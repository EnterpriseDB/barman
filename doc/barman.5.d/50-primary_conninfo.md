primary_conninfo
:   The connection string used by Barman to connect to the primary Postgres
    server during backup of a standby Postgres server. Barman will use this
    connection to carry out any required WAL switches on the primary during
    the backup of the standby. This allows backups to complete even when
    `archive_mode = always` is set on the standby and write traffic to the
    primary is not sufficient to trigger a natural WAL switch.

    If primary_conninfo is set then it *must* be pointing to a primary
    Postgres instance and conninfo *must* be pointing to a standby Postgres
    instance. Furthermore both instances must share the same systemid. If
    these conditions are not met then `barman check` will fail.

    The primary_conninfo value must be a libpq connection string; consult the
    [PostgreSQL manual][conninfo] for more information. Commonly used
    keys are: host, hostaddr, port, dbname, user, password. Server.

[conninfo]: https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING
