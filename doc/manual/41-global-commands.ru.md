\newpage

# General commands

Barman has many commands and, for the sake of exposition, we can
organize them by scope.

The scope of the **general commands** is the entire Barman server,
that can backup many PostgreSQL servers. **Server commands**, instead,
act only on a specified server. **Backup commands** work on a backup,
which is taken from a certain server.

The following list includes the general commands.


## `cron`

`barman` doesn't include a long-running daemon or service file (there's
nothing to `systemctl start`, `service start`, etc.).  Instead, the `barman
cron` subcommand is provided to perform `barman`'s background
"steady-state" backup operations.

You can perform maintenance operations, on both WAL files and backups,
using the `cron` command:

``` bash
barman cron
```

> **NOTE:**
> This command should be executed in a _cron script_. Our
> recommendation is to schedule `barman cron` to run every minute.  If
> you installed Barman using the rpm or debian package, a cron entry
> running on every minute will be created for you.

`barman cron` executes WAL archiving operations concurrently on a
server basis, and this also enforces retention policies on those
servers that have:

- `retention_policy` not empty and valid;
- `retention_policy_mode` set to `auto`.

The `cron` command ensures that WAL streaming is started for those
servers that have requested it, by transparently executing the
`receive-wal` command.

In order to stop the operations started by the `cron` command, comment out
the cron entry and execute:

```bash
barman receive-wal --stop SERVER_NAME
```

You might want to check `barman list-server` to make sure you get all of
your servers.

## `diagnose`

The `diagnose` command creates a JSON report useful for diagnostic and
support purposes. This report contains information for all configured
servers.

> **IMPORTANT:**
> Even if the diagnose is written in JSON and that format is thought
> to be machine readable, its structure is not to be considered part
> of the interface. Format can change between different Barman versions.


## `list-server`

You can display the list of active servers that have been configured
for your backup system with:

``` bash
barman list-server
```

A machine readble output can be obtained with the `--minimal` option:

``` bash
barman list-server --minimal
```
