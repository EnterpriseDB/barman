list-backups *SERVER_NAME*
:   Show available backups for `SERVER_NAME`. This command is useful to
    retrieve a backup ID and the backup type. For example:

```
servername 20111104T102647 - F - Fri Nov  4 10:26:48 2011 - Size: 17.0 MiB - WAL Size: 100 B
```

In this case, *20111104T102647* is the backup ID, and `F` is the backup type label for a full backup taken with `pg_basebackup`. The backup type label displayed by this command takes one of the following values:
    - `F`: for full backups taken with `pg_basebackup`
    - `I`: for incremental backups taken with `pg_basebackup`
    - `R`: for backups taken with `rsync`
    - `S`: for cloud snapshot backups