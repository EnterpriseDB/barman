\newpage

# Backup commands

**TODO:**

- Define backup commands
- do an updated inventory of server commands
- order commands alphabetically
- Remove all 'From version ...' paragraph. Assume that we start from scratch with 2.0

> **Note:**
> Remember: a backup ID can be retrieved with `barman list-backup
> <server_name>`

TODO: Shortcuts

## `delete`

You can delete a given backup with:

``` bash
barman delete <server_name> <backup_id>
```

From version 1.1.2, in order to delete the oldest backup, you can
issue:

``` bash
barman delete <server_name> oldest
```

## `list-files`

You can list the files (base backup and required WAL files) for a
given backup with:

``` bash
barman list-files [--target TARGET_TYPE] <server_name> <backup_id>
```

With the `--target TARGET_TYPE` option, it is possible to choose the
content of the list for a given backup.

Possible values for `TARGET_TYPE` are:

- `data`: lists just the data files;
- `standalone`: lists the base backup files, including required WAL
  files;
- `wal`: lists all WAL files from the beginning of the base backup to
  the start of the following one (or until the end of the log);
- `full`: same as `data` + `wal`.

The default value for `TARGET_TYPE` is `standalone`.

> **Important:**
> The `list-files` command facilitates interaction with external
> tools, and therefore can be extremely useful to integrate > Barman
> into your archiving procedures.

## `recover`

TODO

## `show-backup`

You can show all the available information for a particular backup of
a given server with:

``` bash
barman show-backup <server_name> <backup_id>
```

From version 1.1.2, in order to show the latest backup, you can issue:

``` bash
barman show-backup <server_name> latest
```
