.. _glossary:

Glossary
========

.. glossary::

    AWS
        Amazon Web Services

    Barman
        Backup and Recovery Manager.

    DBA
        Database Administrator.

    DEB
        Debian Package.

    External Configuration Files
        Files that are stored outside of PGDATA. For example, when you create a new
        PostgreSQL cluster using ``pg_createcluster`` (e.g.,
        ``sudo pg_createcluster 14 main``) in Ubuntu, it sets up a new data directory,
        typically under ``/var/lib/postgresql/``, but the configuration files, like
        ``postgresql.conf`` and ``pg_hba.conf``, are usually stored under
        ``/etc/postgresql/<version>/main/`` to separate them from the actual data.

    GCP
        Google Cloud Platform

    IAC
        Infrastructure As Code

    ICT
        Information and Communication Technology.

    libpq
        The C application programmer's interface to Postgres. libpq is a set
        of library functions that allow client programs to pass queries to the
        Postgres backend server and to receive the results of these queries.

    PGDATA
        PostgreSQL data directory.

    PGDG
        Postgres Global Development Group.

    PITR
        Point-in-time Recovery.

    RHEL
        Red Hat Enterprise Linux.

    RPM
        Red Hat Package Manager.

    RPO
        Recovery Point Objective. The maximum targeted period in which data might be
        lost from an IT service due to a major incident. In summary, it represents the
        maximum amount of data you can afford to lose.

    SLES
        SUSE Linux Enterprise Server

    SPOF
        Single Point of Failure

    VLDB
        Very Large DataBase
