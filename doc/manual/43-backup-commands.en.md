\newpage

# Backup commands

Backup commands are those that works directly on backups already existing in
Barman's backup catalog.

> **NOTE:**
> Remember a backup ID can be retrieved with `barman list-backup
> <server_name>`

## Backup ID shortcuts

Barman allows you to use special keywords to identify a specific backup:

* `last/latest`: identifies the newest backup in the catalog
* `first/oldest`: identifies the oldest backup in the catalog

Using those keywords with Barman commands allows you to execute actions
without knowing the exact ID of a backup for a server.
For example we can issue:

``` bash
barman delete <server_name> oldest
```

to remove the oldest backup available in the catalog and reclaim disk space.

## `delete`

You can delete a given backup with:

``` bash
barman delete <server_name> <backup_id>
```

The `delete` command accepts any [shortcut](#shortcuts) to identify backups.

## `list-files`

You can list the files (base backup and required WAL files) for a
given backup with:

``` bash
barman list-files [--target TARGET_TYPE] <server_name> <backup_id>
```

With the `--target TARGET_TYPE` option, it is possible to choose the
content of the list for a given backup.

Possible values for `TARGET_TYPE` are:

- `data`: lists the data files
- `standalone`: lists the base backup files, including required WAL
  files
- `wal`: lists all WAL files from the beginning of the base backup to
  the start of the following one (or until the end of the log)
- `full`: same as `data` + `wal`

The default value for `TARGET_TYPE` is `standalone`.

> **IMPORTANT:**
> The `list-files` command facilitates interaction with external
> tools, and can therefore be extremely useful to integrate
> Barman into your archiving procedures.

## `recover`

The `recover` command is used to recover a whole server after
a backup is executed using the `backup` command.

This is achieved issuing a command like the following:

```bash
barman@backup$ barman recover <server_name> <backup_id> /path/to/recover/dir
```

> **IMPORTANT:**
> Do not issue a `recover` command using a target data directory where
> a PostgreSQL instance is running. In that case, remember to stop it
> before issuing the recovery. This applies also to tablespace directories.

At the end of the execution of the recovery, the selected backup is recovered
locally and the destination path contains a data directory ready to be used
to start a PostgreSQL instance.

> **IMPORTANT:**
> Running this command as user `barman`, it will become the database superuser.

The specific ID of a backup can be retrieved using the [list-backup](#list-backup)
command.

> **IMPORTANT:**
> Barman does not currently keep track of symbolic links inside PGDATA
> (except for tablespaces inside pg_tblspc). We encourage
> system administrators to keep track of symbolic links and to add them
> to the disaster recovery plans/procedures in case they need to be restored
> in their original location.

The recovery command has several options that modify the command behavior.

### Remote recovery

Add the `--remote-ssh-command <COMMAND>` option to the invocation
of the recovery command. Doing this will allow Barman to execute
the copy on a remote server, using the provided command to connect
to the remote host.

> **NOTE:**
> It is advisable to use the `postgres` user to perform
> the recovery on the remote host.

> **IMPORTANT:**
> Do not issue a `recover` command using a target data directory where
> a PostgreSQL instance is running. In that case, remember to stop it
> before issuing the recovery. This applies also to tablespace directories.

Known limitations of the remote recovery are:

* Barman requires at least 4GB of free space in the system temporary directory
  unless the [`get-wal`](#get-wal) command is specified
  in the `recovery_option` parameter in the Barman configuration.
* The SSH connection between Barman and the remote host **must** use the
  public key exchange authentication method
* The remote user **must** be able to create the directory structure
  of the backup in the destination directory.
* There must be enough free space on the remote server
  to contain the base backup and the WAL files needed for recovery.

### Tablespace remapping

Barman is able to automatically remap one or more tablespaces using
the recover command with the --tablespace option.
The option accepts a pair of values as arguments using the
`NAME:DIRECTORY` format:

* `NAME` is the identifier of the tablespace
* `DIRECTORY` is the new destination path for the tablespace

If the destination directory does not exists,
Barman will try to create it (assuming you have the required permissions).

### Point in time recovery

Barman wraps PostgreSQL's Point-in-Time Recovery (PITR),
allowing you to specify a recovery target, either as a timestamp,
as a restore label, or as a transaction ID.

> **IMPORTANT:**
> The earliest PITR for a given backup is the end of the base
> backup itself. If you want to recover at any point in time
> between the start and the end of a backup, you must use
> the previous backup. From Barman 2.3 you can exit recovery
> when consistency is reached by using `--target-immediate` option
> (available only for PostgreSQL 9.4 and newer).

The recovery target can be specified using one of
four mutually exclusive options:

* `--target-time TARGET_TIME`: to specify a timestamp
* `--target-xid TARGET_XID`: to specify a transaction ID
* `--target-name TARGET_NAME`: to specify a named restore point
  previously created with the pg_create_restore_point(name)
  function[^TARGET_NAME]
* `--target-immediate`: recovery ends when a consistent state is reached
                 (that is the end of the base backup process)
                 [^RECOVERY_TARGET_IMMEDIATE]

> **IMPORTANT:**
> Recovery target via _time_ and _xid_  **must be** subsequent to the
> end of the backup. If you want to recover to a point in time between
> the start and the end of a backup, you must recover from the
> previous backup in the catalogue.

[^TARGET_NAME]:
  Only available on PostgreSQL 9.1 and above

[^RECOVERY_TARGET_IMMEDIATE]:
  Only available on PostgreSQL 9.4 and above

You can use the `--exclusive` option to specify whether to stop immediately
before or immediately after the recovery target.

Barman allows you to specify a target timeline for recovery,
using the `target-tli` option. The notion of timeline goes beyond the scope of
this document; you can find more details in the PostgreSQL documentation,
as mentioned in the _"Before you start"_ section.

Barman 2.4 introduces support for `--target-action` option, accepting
the following values:

* `shutdown`: once recovery target is reached, PostgreSQL is shut down [^TARGET_SHUTDOWN]
* `pause`: once recovery target is reached, PostgreSQL is started in pause
   state, allowing users to inspect the instance [^TARGET_PAUSE]
* `promote`: once recovery target is reached, PostgreSQL will exit recovery
   and is promoted as a master [^TARGET_PROMOTE]

> **IMPORTANT:**
> By default, no target action is defined (for back compatibility).
> The `--target-action` option requires a Point In Time Recovery target
> to be specified.

[^TARGET_SHUTDOWN]:
  Only available on PostgreSQL 9.5 and above

[^TARGET_PAUSE]:
  Only available on PostgreSQL 9.1 and above

[^TARGET_PROMOTE]:
  Only available on PostgreSQL 9.5 and above

For more detailed information on the above settings, please consult
the [PostgreSQL documentation on recovery target settings][target].

Barman 2.4 also adds the `--standby-mode` option for the `recover`
command which, if specified, adds `standby_mode = on` to the generated
`recovery.conf` file, causing the recovered PostgreSQL instance to start
as a standby. Further information on _standby mode_ is available in
the [PostgreSQL documentation on standby server settings][standby].

## `show-backup`

You can retrieve all the available information for a particular backup of
a given server with:

``` bash
barman show-backup <server_name> <backup_id>
```

The `show-backup` command accepts any [shortcut](#shortcuts) to identify backups.
