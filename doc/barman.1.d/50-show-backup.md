show-backup *SERVER_NAME* *BACKUP_ID*
:   Show detailed information about a particular backup, identified by
    the server name and the backup ID. See the [Backup ID shortcuts](#shortcuts)
    section below for available shortcuts. For example:

```
Backup 20150828T130001:
  Server Name            : quagmire
  Status                 : DONE
  PostgreSQL Version     : 90402
  PGDATA directory       : /srv/postgresql/9.4/main/data

  Base backup information:
    Disk usage           : 12.4 TiB (12.4 TiB with WALs)
    Incremental size     : 4.9 TiB (-60.02%)
    Timeline             : 1
    Begin WAL            : 0000000100000CFD000000AD
    End WAL              : 0000000100000D0D00000008
    WAL number           : 3932
    WAL compression ratio: 79.51%
    Begin time           : 2015-08-28 13:00:01.633925+00:00
    End time             : 2015-08-29 10:27:06.522846+00:00
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
```
