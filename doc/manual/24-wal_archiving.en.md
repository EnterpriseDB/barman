## WAL archiving via `archive_command`

The `archive_command` is the traditional method to archive WAL files.

The value of this PostgreSQL configuration parameter must be a shell
command to be executed by the PostgreSQL server to copy the WAL files
to the Barman incoming directory.

You can retrieve the incoming WALs directory using the `show-server`
Barman command and looking for the `incoming_wals_directory` value:

``` bash
barman@backup$ barman show-server pg |grep incoming_wals_directory
        incoming_wals_directory: /var/lib/barman/pg/incoming
```

> **IMPORTANT:**
> PostgreSQL 9.5 introduced support for WAL file archiving using
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

Restart the PostgreSQL server.

In order to test that continuous archiving is on and properly working,
you need to check both the PostgreSQL server and the backup server. In
particular, you need to check that WAL files are correctly collected
in the destination directory.

In some cases, you might want to add stricter checks to the `archive_command`
process. For example, some users have suggested the following one:

``` ini
archive_command = 'test $(/bin/hostname --fqdn) = HOSTNAME \
  && rsync -a %p barman@backup:INCOMING_WALS_DIRECTORY/%f'
```

Where the `HOSTNAME` placeholder should be replaced with the value
returned by `hostname --fqdn`. This _trick_ is a safeguard in case
the server is cloned and avoids receiving WAL files from recovered
PostgreSQL instances.

## Verification of WAL archiving configuration

In order to improve the verification of the WAL archiving process, the
`switch-wal` command has been developed:

``` bash
barman@backup$ barman switch-wal --force --archive pg
```

The above command will force PostgreSQL to switch WAL file and
trigger the archiving process in Barman. Barman will wait for one
file to arrive within 30 seconds (you can change the timeout through
the `--archive-timeout` option). If no WAL file is received, an error
is returned.

You can verify if the WAL archiving has been correctly configured using
the `barman check` command.
