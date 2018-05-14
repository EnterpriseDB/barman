\newpage

\appendix

# Feature matrix

Below you will find a matrix of PostgreSQL versions and Barman features for backup and archiving:

| **Version** | **Backup with rsync/SSH** | **Backup with pg_basebackup** | **Standard WAL archiving** | **WAL Streaming** | **RPO=0** |
|:---------:|:---------------------:|:-------------------------:|:----------------------:|:----------------------:|:-------:|
| **10** | Yes | Yes | Yes | Yes | Yes |
| **9.6** | Yes | Yes | Yes | Yes | Yes |
| **9.5** | Yes | Yes | Yes | Yes | Yes ~(d)~ |
| **9.4** | Yes | Yes | Yes | Yes | Yes ~(d)~ |
| **9.3** | Yes | Yes ~(c)~ | Yes | Yes ~(b)~ | No |
| **9.2** | Yes | Yes ~(a)~~(c)~ | Yes | Yes ~(a)~~(b)~ | No |
| _9.1_ | Yes | No | Yes | No | No |
| _9.0_ | Yes | No | Yes | No | No |
| _8.4_ | Yes | No | Yes | No | No |
| _8.3_ | Yes | No | Yes | No | No |


**NOTE:**

a) `pg_basebackup` and `pg_receivexlog` 9.2 required
b) WAL streaming-only not supported (standard archiving required)
c) Backup of tablespaces not supported
d) When using `pg_receivexlog` 9.5, minor version 9.5.5 or higher required [^commitsync]

  [^commitsync]: The commit ["Fix pg_receivexlog --synchronous"][49340627f9821e447f135455d942f7d5e96cae6d] is required (included in version 9.5.5)

It is required by Barman that `pg_basebackup` and `pg_receivewal`/`pg_receivexlog` of the same version of the PostgreSQL server (or higher) are installed on the same server where Barman resides. The only exception is that PostgreSQL 9.2 users are required to install version 9.2 of `pg_basebackup` and `pg_receivexlog` alongside with Barman.

>> **TIP:** We recommend that the last major, stable version of the PostgreSQL clients (e.g. 10) is installed on the Barman server if you plan to use backup and WAL archiving over streaming replication through `pg_basebackup` and `pg_receivewal`, for PostgreSQL 9.3 or higher servers.

>> **TIP:** For "RPO=0" architectures, it is recommended to have at least one synchronous standby server.

