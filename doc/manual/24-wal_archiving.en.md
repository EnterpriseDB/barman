## WAL archiving via `archive_command`

The `archive_command` is the traditional method to archive WAL files.

The value of this PostgreSQL configuration parameter must be a shell
command to be executed by the PostgreSQL server to copy the WAL files
to the Barman incoming directory.

This can be done in two ways, both requiring a SSH connection:

- via `barman-wal-archive` utility (from Barman 2.6)
- via rsync/SSH (common approach before Barman 2.6)

See sections below for more details.

> **IMPORTANT:** PostgreSQL 9.5 introduced support for WAL file
> archiving using `archive_command` from a standby. Read the
> "Concurrent Backup and backup from a standby" section for more
> detailed information on how Barman supports this feature.


### WAL archiving via `barman-wal-archive`

From Barman 2.6, the **recommended way** to safely and reliably archive WAL
files to Barman via `archive_command` is to use the `barman-wal-archive`
command contained in the `barman-cli` package,
distributed via EnterpriseDB public repositories and available under
GNU GPL 3 licence. `barman-cli` must be installed on each PostgreSQL
server that is part of the Barman cluster.

Using `barman-wal-archive` instead of rsync/SSH reduces the risk
of data corruption of the shipped WAL file on the Barman server.
When using rsync/SSH as `archive_command` a WAL file, there is no
mechanism that guarantees that the content of the file is flushed
and fsync-ed to disk on destination.

For this reason, we have developed the `barman-wal-archive` utility
that natively communicates with Barman's `put-wal` command (introduced in 2.6),
which is responsible to receive the file, fsync its content and place
it in the proper `incoming` directory for that server. Therefore,
`barman-wal-archive` reduces the risk of copying a WAL file in the
wrong location/directory in Barman, as the only parameter to be used
in the `archive_command` is the server's ID.

For more information on the `barman-wal-archive` command, type `man barman-wal-archive`
on the PostgreSQL server.

You can check that `barman-wal-archive` can connect to the Barman server,
and that the required PostgreSQL server is configured in Barman to accept
incoming WAL files with the following command:

``` bash
barman-wal-archive --test backup pg DUMMY
```

Where `backup` is the host where Barman is installed, `pg` is the name
of the PostgreSQL server as configured in Barman and DUMMY is a placeholder
(`barman-wal-archive` requires an argument for the WAL file name,
which is ignored).

If everything is configured correctly you should see the following output:

``` bash
Ready to accept WAL files for the server pg
```

Since it uses SSH to communicate with the Barman server, SSH key authentication
is required for the `postgres` user to login as `barman` on the backup server.
If a port other than the SSH default of 22 should be used then the `--port`
option can be added to specify the port that should be used for the SSH
connection.

Edit the `postgresql.conf` file of the PostgreSQL instance on the `pg`
database, activate the archive mode and set `archive_command` to use
`barman-wal-archive`:

``` ini
archive_mode = on
wal_level = 'replica'
archive_command = 'barman-wal-archive backup pg %p'
```

Then restart the PostgreSQL server.


### WAL archiving via rsync/SSH

You can retrieve the incoming WALs directory using the `show-servers`
Barman command and looking for the `incoming_wals_directory` value:

``` bash
barman@backup$ barman show-servers pg |grep incoming_wals_directory
        incoming_wals_directory: /var/lib/barman/pg/incoming
```

Edit the `postgresql.conf` file of the PostgreSQL instance on the `pg`
database and activate the archive mode:

``` ini
archive_mode = on
wal_level = 'replica'
archive_command = 'rsync -a %p barman@backup:INCOMING_WALS_DIRECTORY/%f'
```

Make sure you change the `INCOMING_WALS_DIRECTORY` placeholder with
the value returned by the `barman show-servers pg` command above.

Restart the PostgreSQL server.

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

In order to test that continuous archiving is on and properly working,
you need to check both the PostgreSQL server and the backup server. In
particular, you need to check that WAL files are correctly collected
in the destination directory.

For this purpose and to facilitate the verification of the WAL archiving process,
the `switch-wal` command has been developed:

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
