\newpage

# Before you start

Before you start using Barman, it is fundamental that you get familiar
with PostgreSQL and the concepts around physical backups, Point-In-Time-Recovery and replication, such as base backups, WAL archiving, etc.

Below you can find a non exhaustive list of resources that we recommend for you to read:

- _PostgreSQL documentation_:
    - [SQL Dump][sqldump][^pgdump]
    - [File System Level Backup][physicalbackup]
    - [Continuous Archiving and Point-in-Time Recovery (PITR)][pitr]
    - [Recovery Configuration][recoveryconfig]
    - [Reliability and the Write-Ahead Log][wal]
- _Book_: [PostgreSQL 9 Administration Cookbook - 2nd edition][adminbook]

  [^pgdump]: It is important that you know the difference between logical and physical backup, therefore between `pg_dump` and a tool like Barman.

Professional training on these topics is another effective way of
learning these concepts. At any time of the year you can find many
courses available all over the world, delivered by PostgreSQL
companies such as 2ndQuadrant.
