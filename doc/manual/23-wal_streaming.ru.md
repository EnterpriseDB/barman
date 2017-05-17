## WAL streaming

Barman can reduce the Recovery Point Objective (RPO) by allowing users
to add continuous WAL streaming from a PostgreSQL server, on top of
the standard `archive_command` strategy

Barman relies on [`pg_receivexlog`] [25], a utility that has been
available from PostgreSQL 9.2 which exploits the native streaming
replication protocol and continuously receives transaction logs from a
PostgreSQL server (master or standby).

> **IMPORTANT:**
> Barman requires that `pg_receivexlog` is installed on the same
> server.  For PostgreSQL 9.2 servers, you need `pg_receivexlog` of
> version 9.2 installed alongside Barman. For PostgreSQL 9.3 and
> above, it is recommended to install the latest available version of
> `pg_receivexlog`, as it is back compatible.  Otherwise, users can
> install multiple versions of `pg_receivexlog` on the Barman server
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
and only for PostgreSQL 9.3 or above, Barman will set `application_name`
of the WAL streamer process to `barman_receive_wal`, allowing you to
monitor its status in the `pg_stat_replication` system view of the
PostgreSQL server.


### Replication slots

> **IMPORTANT:** replication slots are available since PostgreSQL 9.4

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
> In this moment, the Windows support is still experimental, as it is
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

When you configure the replication slot name, you can create a
replication slot for Barman with this command:

``` bash
barman@backup$ barman receive-wal --create-slot pg
Creating physical replication slot 'barman' on server 'pg'
Replication slot 'barman' created
```

