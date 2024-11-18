# EXAMPLE

Here is an example of configuration file:

```
[barman]
; Main directory
barman_home = /var/lib/barman

; System user
barman_user = barman

; Log location
log_file = /var/log/barman/barman.log

; Default compression level
;compression = gzip

; Incremental backup
reuse_backup = link

; 'main' PostgreSQL Server configuration
[main]
; Human readable description
description =  "Main PostgreSQL Database"

; SSH options
ssh_command = ssh postgres@pg

; PostgreSQL connection string
conninfo = host=pg user=postgres

; PostgreSQL streaming connection string
streaming_conninfo = host=pg user=postgres

; Minimum number of required backups (redundancy)
minimum_redundancy = 1

; Retention policy (based on redundancy)
retention_policy = REDUNDANCY 2
```
