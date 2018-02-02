status *SERVER_NAME*
:   Show information about the status of a server, including: number of
    available backups, `archive_command`, `archive_status` and many more.
    For example:

```
Server quagmire:
  Description: The Giggity database
  Passive node: False
  PostgreSQL version: 9.3.9
  pgespresso extension: Not available
  PostgreSQL Data directory: /srv/postgresql/9.3/data
  PostgreSQL 'archive_command' setting: rsync -a %p barman@backup:/var/lib/barman/quagmire/incoming
  Last archived WAL: 0000000100003103000000AD
  Current WAL segment: 0000000100003103000000AE
  Retention policies: enforced (mode: auto, retention: REDUNDANCY 2, WAL retention: MAIN)
  No. of available backups: 2
  First available backup: 20150908T003001
  Last available backup: 20150909T003001
  Minimum redundancy requirements: satisfied (2/1)
```
