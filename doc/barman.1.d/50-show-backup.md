show-backup *SERVER_NAME* *BACKUP_ID*
:   Show detailed information about a particular backup, identified by
    the server name and the backup ID. See the [Backup ID shortcuts](#shortcuts)
    section below for available shortcuts. The following example is from
    a block-level incremental backup (which requires Postgres version >= 17):

```
Backup 20240814T017504:
  Server Name            : quagmire
  Status                 : DONE
  PostgreSQL Version     : 90402
  PGDATA directory       : /srv/postgresql/9.4/main/data
  Estimated Cluster Size : 22.4 MiB

  Server information:
    Checksums            : on
    WAL summarizer       : on

  Base backup information:
    Backup Method        : postgres
    Backup Type          : incremental
    Backup Size          : 22.3 MiB (54.3 MiB with WALs)
    WAL Size             : 32.0 MiB
    Resource savings     : 19.5 MiB (86.80%)
    Timeline             : 1
    Begin WAL            : 0000000100000CFD000000AD
    End WAL              : 0000000100000D0D00000008
    WAL number           : 3932
    WAL compression ratio: 79.51%
    Begin time           : 2015-08-28 13:00:01.633925+00:00
    End time             : 2015-08-29 10:27:06.522846+00:00
    Copy time            : 1 second
    Estimated throughput : 2.0 MiB/s
    Begin Offset         : 1575048
    End Offset           : 13853016
    Begin XLOG           : CFD/AD180888
    End XLOG             : D0D/8D36158

  WAL information:
    No of files          : 35039
    Disk usage           : 121.5 GiB
    WAL rate             : 275.50/hour
    Compression ratio    : 77.81%
    Last available       : 0000000100000D95000000E7

  Catalog information:
    Retention Policy     : not enforced
    Previous Backup      : 20150821T130001
    Next Backup          : - (this is the latest base backup)
    Root Backup          : 20240814T015504
    Parent Backup        : 20240814T016504
    Backup chain size    : 3
    Children Backup(s)   : 20240814T018515
```

> **NOTE:**
> Depending on the version of your Postgres Server and/or the type
> of the backup, the output of `barman show-backup` command may
> be different. For example, fields like "Root Backup", "Parent Backup",
> "Backup chain size", and "Children Backup(s)" only make sense when
> showing information about a block-level incremental backup taken
> with `backup_method = postgres` and using Postgres 17 or newer,
> thus those fields are omitted for other kind of backups or older versions
> of Postgres.
>
> Also note that `show-backup` relies on the backup metadata so if a backup
> was created with Barman version 3.10 or earlier, the backup will not 
> contain the fields added in version 3.11 (which are those added after
> the introduction of "incremental" backups in PostgreSQL 17).
>
> These are the possible values for the field "Backup Type":
>
> * `rsync`: for a backup taken with `rsync`;
> * `full`: for a full backup taken with `pg_basebackup`;
> * `incremental`: for an incremental backup taken with `pg_basebackup`;
> * `snapshot`: for a snapshot-based backup taken in the cloud.
>
> Below you can find a list of fields that may be shown or omitted depending
> on the type of the backup:
>
> * `Resource savings`: available for "rsync" and "incremental" backups;
> * `Root Backup`, `Parent Backup`, `Backup chain size`: available for 
> "incremental" backups only;
> * `Children Backup(s)`: available for "full" and "incremental" backups;
> * `Snapshot information`: available for "snapshot" backups only.