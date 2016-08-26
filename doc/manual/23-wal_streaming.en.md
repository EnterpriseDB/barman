## WAL streaming

Barman can reduces Recovery Point Objective (RPO) by allowing users to add, on top of the standard `archive_command` strategy, continuous WAL streaming from a PostgreSQL server.

Barman relies on [`pg_receivexlog`] [25], a utility that is available
from PostgreSQL 9.2 which exploits the native streaming replication protocol
and continuously receives transaction logs from a PostgreSQL
server (be it a master or a standby).

> **Important:**
> Barman requires that `pg_receivexlog` is installed in the same server.
> For PostgreSQL 9.2 servers, you need `pg_receivexlog` of version 9.2
> installed alongside with Barman. For PostgreSQL 9.3 and above, it is
> recommended to install the latest available version of `pg_receivexlog`,
> as it is back compatible.
> Otherwise, users can install multiple versions of `pg_receivexlog` in the
> Barman server and properly point to the specific version for a server,
> using the `path` option in the configuration file.

In order to enable streaming of transaction logs, you need to:

1. setup a streaming connection, as previously described;
2. set the `streaming_archiver` option to `on`.

The `cron` command, if the aforementioned requirements are met,
transparently manages log streaming through the execution of the
`receive-wal` command. This is the recommended scenario.

However, users can manually execute the `receive-wal` command:

``` bash
barman receive-wal <server_name>
```

> **Note:**
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

**TODO:**

- Explain how to configure replication slots, how to create them, etc.
- Mention streaming-only scenarios

