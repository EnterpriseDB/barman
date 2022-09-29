## WAL streaming

Barman can reduce the Recovery Point Objective (RPO) by allowing users
to add continuous WAL streaming from a PostgreSQL server, on top of
the standard `archive_command` strategy.

Barman relies on [`pg_receivewal`][25], it exploits the native streaming
replication protocol and continuously receives transaction logs from a
PostgreSQL server (master or standby).
Prior to PostgreSQL 10, `pg_receivewal` was named `pg_receivexlog`.

> **IMPORTANT:**
> Barman requires that `pg_receivewal` is installed on the same
> server. It is recommended to install the latest available version of
> `pg_receivewal`, as it is back compatible.  Otherwise, users can
> install multiple versions of `pg_receivewal` on the Barman server
> and properly point to the specific version for a server, using the
> `path_prefix` option in the configuration file.

In order to enable streaming of transaction logs, you need to:

1. setup a streaming connection as previously described
2. set the `streaming_archiver` option to `on`

The `cron` command, if the aforementioned requirements are met,
transparently manages log streaming through the execution of the
`receive-wal` command. This is the recommended scenario.

However, users can manually execute the `receive-wal` command:

``` bash
barman receive-wal <server_name>
```

> **NOTE:**
> The `receive-wal` command is a foreground process.

Transaction logs are streamed directly in the directory specified by the
`streaming_wals_directory` configuration option and are then archived
by the `archive-wal` command.

Unless otherwise specified in the `streaming_archiver_name` parameter,
Barman will set `application_name` of the WAL streamer process to 
`barman_receive_wal`, allowing you to monitor its status in the 
`pg_stat_replication` system view of the PostgreSQL server.


### Replication slots

Replication slots are an automated way to ensure that the PostgreSQL
server will not remove WAL files until they were received by all
archivers. Barman uses this mechanism to receive the transaction logs
from PostgreSQL.

You can find more information about replication slots in the
[PostgreSQL manual][replication-slots].

You can even base your backup architecture on streaming connection
only. This scenario is useful to configure Docker-based PostgreSQL
servers and even to work with PostgreSQL servers running on Windows.

> **IMPORTANT:**
> At this moment, the Windows support is still experimental, as it is
> not yet part of our continuous integration system.


### How to configure the WAL streaming

First, the PostgreSQL server must be configured to stream the
transaction log files to the Barman server.

To configure the streaming connection from Barman to the PostgreSQL
server you need to enable the `streaming_archiver`, as already said,
including this line in the server configuration file:

``` ini
streaming_archiver = on
```

If you plan to use replication slots (recommended),
another essential option for the setup of the streaming-based
transaction log archiving is the `slot_name` option:

``` ini
slot_name = barman
```

This option defines the name of the replication slot that will be
used by Barman. It is mandatory if you want to use replication slots.

When you configure the replication slot name, you can manually create a
replication slot for Barman with this command:

``` bash
barman@backup$ barman receive-wal --create-slot pg
Creating physical replication slot 'barman' on server 'pg'
Replication slot 'barman' created
```

Starting with Barman 2.10, you can configure Barman to automatically
create the replication slot by setting:

``` ini
create_slot = auto
```

### Limitations of partial WAL files with recovery

The standard behaviour of `pg_receivewal` is to write transactional
information in a file with `.partial` suffix after the WAL segment name.

Barman expects a partial file to be in the `streaming_wals_directory` of
a server. When completed, `pg_receivewal` removes the `.partial` suffix
and opens the following one, delivering the file to the `archive-wal` command
of Barman for permanent storage and compression.

In case of a sudden and unrecoverable failure of the master PostgreSQL server,
the `.partial` file that has been streamed to Barman contains very important
information that the standard archiver (through PostgreSQL's `archive_command`)
has not been able to deliver to Barman.

As of Barman 2.10, the `get-wal` command is able to return the content of
the current `.partial` WAL file through the `--partial/-P` option.
This is particularly useful in the case of recovery, both full or to a point
in time. Therefore, in case you run a `recover` command with `get-wal` enabled,
and without `--standby-mode`, Barman will automatically add the `-P` option
to `barman-wal-restore` (which will then relay that to the remote `get-wal`
command) in the `restore_command` recovery option.

`get-wal` will also search in the `incoming` directory, in case a WAL file
has already been shipped to Barman, but not yet archived.
