## Streaming backup

Barman can backup a PostgreSQL server using the streaming connection,
relying on `pg_basebackup`, a utility that has been available from
PostgreSQL 9.1.

> **IMPORTANT:** Barman requires that `pg_basebackup` is installed in
> the same server. For PostgreSQL 9.2 servers, you need the
> `pg_basebackup` of version 9.2 installed alongside with Barman.  For
> PostgreSQL 9.3 and above, it is recommented to install the last
> available version of `pg_basebackup`, as it is back compatible.  You
> can even install multiple versions of `pg_basebackup` on the Barman
> server and properly point to the specific version for a server,
> using the `path_prefix` option in the configuration file.

To successfully backup your server with the streaming connection, you
need to use `postgres` as your backup method:

``` ini
backup_method = postgres
```

> **IMPORTANT:** keep in mind that if the WAL archiving is not
> currently configured, you will not be able to start a backup.

To check if the server configuration is valid you can use the `barman
check` command:

``` bash
barman@backup$ barman check pg
```

To start a backup you can use the `barman backup` command:

``` bash
barman@backup$ barman backup pg
```

> **IMPORTANT:** `pg_basebackup` 9.4 or higher is required for
> tablespace support if you use the `postgres` backup method.
