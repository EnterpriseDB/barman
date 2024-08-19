## Streaming backup

Barman can backup a PostgreSQL server using the streaming connection,
relying on `pg_basebackup`. Since version 3.11, Barman also supports block-level
incremental backups using the streaming connection, for more information
consult the _"Features in detail"_ section.

> **IMPORTANT:** Barman requires that `pg_basebackup` is installed in
> the same server. It is recommended to install the last available 
> version of `pg_basebackup`, as it is backwards compatible.  You can 
> even install multiple versions of `pg_basebackup` on the Barman
> server and properly point to the specific version for a server,
> using the `path_prefix` option in the configuration file.

To successfully backup your server with the streaming connection, you
need to use `postgres` as your backup method:

``` ini
backup_method = postgres
```

> **IMPORTANT:** You will not be able to start a backup if WAL is not
> being correctly archived to Barman, either through the `archiver` or
> the `streaming_archiver`

To check if the server configuration is valid you can use the `barman
check` command:

``` bash
barman@backup$ barman check pg
```

To start a backup you can use the `barman backup` command:

``` bash
barman@backup$ barman backup pg
```
