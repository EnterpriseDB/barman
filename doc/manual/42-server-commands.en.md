\newpage

# Server commands

As we said in the previous section, server commands works directly on
a PostgreSQL server or on it's area in Barman, and are useful to check
its status, to do maintainance operations, to take backups, and to
manage WAL archiving.

## `archive_wal`

The `archive_wal` command execute maintainance operations on WAL files
for a given server. This operations include processing of the WAL
files received from the streaming connection or from the
`archive_command` or both.

> **Important:**
> The `archive_wal` command, even if it can be directly invoked, is
> designed to be started from the `cron` general command

## `backup`

The `backup` command take a full backup (base backup) of a given
server. It has several options, that let you override the
configuration parameter for the new backup. For more information
consult the manual page.

You can perform a full backup (base backup) for a given server with:

``` bash
barman backup [--immediate-checkpoint] <server_name>
```

If you specify the `--immediate-checkpoint` option, or if you have put
it in the configuration file, the server will complete a checkpoint
immediately at the start of backup.

> **Tip:**
> You can use `barman backup all` to sequentially backup all your
> configured servers.


## `check`

You can check the connection to a given server and the
configuration coherence with the `check` command:

``` bash
barman check <server_name>
```

> **Tip:**
> You can use `barman check all` to check all your configured servers.

You can automatically be notified if the latest backup of a given
server is older than, for example, _7 days_.[^SMELLY_BACKUP]

[^SMELLY_BACKUP]:
  This feature is commonly known among the development team members as
  _smelly backup check_.

Barman introduces the configuration parameter named
`last_backup_maximum_age` having the following syntax:

``` ini
last_backup_maximum_age = {value {DAYS | WEEKS | MONTHS}}
```

where `value` is a positive integer representing the number of days,
weeks or months of the time frame.

## `get-wal`

Barman allows users to request any _xlog_ file from its WAL archive
through the `get-wal` command:

``` bash
barman get-wal [-o OUTPUT_DIRECTORY] [-j|-x] <server_name> <wal_id>
```

If the requested WAL file is found in the server archive, the
uncompressed content will be returned to `STDOUT`, unless otherwise
specified.

The following options are available for the `get-wal` command:

- `-o` allows users to specify a destination directory where Barman
  will deposit the requested WAL file
- `-j` will compress the output using `bzip2` algorithm
- `-x` will compress the output using `gzip` algorithm
- `-p SIZE` peeks from the archive up to WAL files, starting from
  the requested one.

It is possible to use `get-wal` during a recovery operation,
transforming the Barman server in a _WAL hub_ for your servers. This
can be automatically achieved by adding the `get-wal` value to the
`recovery_options` global/server configuration option:

``` ini
recovery_options = 'get-wal'
```

`recovery_options` is a global/server option that accepts a list of
comma separated values. If the keyword `get-wal` is present, during a
recovery operation Barman will prepare the `recovery.conf` file by
setting the `restore_command` so that `barman get-wal` is used to
fetch the required WAL files.

This is an example of a `restore_command` for a local recovery:

``` ini
restore_command = 'sudo -u barman barman get-wal SERVER %f > %p'
```

Please note that the `get-wal` command should always be invoked as
`barman` user, and that it requires the correct permission to
read the WAL files from the catalog. This is the reason why we are
using `sudo -u barman` in the example.

Setting `recovery_options` to `get-wal` for a remote recovery will instead
generate a `restore_command` using the `barman-wal-restore` script.
`barman-wal-restore` is a more resilient shell script which manages SSH
connection errors.

This script has many useful options such as the automatic compression and
decompression of the WAL file and the *peek* feature, which allows you
to retrieve the next WAL files while PostgreSQL is applying one of them. It is
an excellent way to optimise the bandwidth usage between the PostgreSQL and the
Barman server.

`barman-wal-restore` is available in the `barman-cli` project or package.

This is an example of a `restore_command` for a remote recovery:

``` ini
restore_command = 'barman-wal-restore -U barman backup SERVER %f %p'
```

Since it uses SSH to communicate with the Barman server, SSH key authentication
is required for the `postgres` user to login as `barman` on the backup server.

> **Important:**
> Even though `recovery_options` aims to automate the process, using
> the `get-wal` facility requires manual intervention and proper
> testing.

## `list-backup`

You can list the catalogue of available backups for a given server
with:

``` bash
barman list-backup <server_name>
```

> **Tip:** You can request a full list of the backups of all servers
> using `all` as the server name.

To have a machine-readable output you can use the `--minimal` option.

## `rebuild-xlogdb`

At any time, you can regenerate the content of the WAL archive for a
specific server (or every server, using the `all` shortcut). The WAL
archive is contained in the `xlog.db` file and every server managed by
Barman server has its own copy.

The `xlog.db`file can be rebuilt with the `rebuild-xlogdb`
command. This will scan all the archived WAL files and regenerate the
metadata for the archive.

For example:

``` bash
barman rebuild-xlogdb <server_name>
```

## `receive-wal`

This command manages the `receive-wal` process, which uses the
streaming protocol to receive WAL files from the PostgreSQL streaming
connection.

### receive-wal process management

If the command is run without options, a `receive-wal` process will
be started. This command is based on the `pg_receivexlog` PostgreSQL
command.

``` bash
barman receive-wal <server_name>
```

If the command is run with the `--stop` option, the currently running
`receive-wal` process will be stopped.

The `receive-wal` process uses a status file to track last written
record of the transaction log. When the status file needs to be
cleaned, the `--reset` option can be used.

### Replication slot management

The `receive-wal` process is also useful to create or drop the
replication slot needed by Barman for its WAL archiving procedure.

With the `--create-slot` option, the replication slot named after the
`slot_name` configuration option will be created on the PostgreSQL
server.

With the `--drop-slot`, the previous replication slot will be deleted.

## `show-server`

You can show the configuration parameters for a given server with:

``` bash
barman show-server <server_name>
```

> **Tip:** you can request a full configuration report using `all` as
> the server name.


## `status`

The `status` command shows live information and status of a PostgreSQL
server or of all server if you use `all` as server name.

``` bash
barman show-server <server_name>
```

## `switch-xlog`

This command makes the PostgreSQL server switch to another transaction
log file, allowing the current log file to be received and then
archived.

``` bash
barman switch-xlog <server_name>
```

If there has been no transaction activity since the last transaction
log file switch, the switch needs to be forced. You can use the
`--force` option for that.
