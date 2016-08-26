## WAL archiving via `archive_command`

<!-- TODO: Cover requirements, Scenarios, etc. -->

In case you want to setup the traditional WAL file archiving process,
Barman requires that PostgreSQL's `archive_command` is properly
configured on the master.

> **Important:**
> PostgreSQL 9.5 introduces support for WAL file archiving using
> `archive_command` from a standby. This feature is not yet implemented
> in Barman.


Edit the `postgresql.conf` file of the PostgreSQL instance on the `pg`
database and activate the archive mode:

``` ini
archive_mode = on
wal_level = 'replica'
archive_command = 'rsync -a %p barman@backup:INCOMING_WALS_DIRECTORY/%f'
```

Make sure you change the `INCOMING_WALS_DIRECTORY` placeholder with
the value returned by the `barman show-server pg` command above.

For PostgreSQL versions older than 9.5, `wal_level` must be set to `hot_standby`.

Restart the PostgreSQL server.

In order to test that continuous archiving is on and properly working,
you need to check both the PostgreSQL server and the `backup` server
(in particular, that WAL files are correctly collected in the
destination directory).

In order to improve the verification of the WAL archiving process, the `switch-xlog` command has been developed:

``` bash
barman@backup$ barman switch-xlog --force pg
```


