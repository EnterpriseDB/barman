.. _diagnose-and-troubleshooting:

Diagnose and troubleshooting
============================

One of the features that tools require is the ability to troubleshoot problems
in an efficient way. Barman provides multiple ways to diagnose and troubleshoot
problems.

Usually problems arise from errors or warning messages returned by
:ref:`barman check <barman-check>`, but they may come up from other sources as well.

Barman status
-------------

You can check the status of a specific Postgres server using the
:ref:`barman status <commands-barman-status>` command. This command provides a detailed
view of the status of the server, such as the PostgreSQL version, the current data size,
the PostgreSQL data directory, the current WAL, the archive command, the last archived
WAL, the number of available backups, and more:

.. code:: bash

    $ barman status pg16
    Server pg16:
        Description: PostgreSQL 16 Database (via SSH)
        Active: True
        Disabled: False
        PostgreSQL version: 16.4
        Cluster state: in production
        Current data size: 31.9 MiB
        PostgreSQL Data directory: /var/lib/pgsql/16/data
        Current WAL segment: 000000010000003B00000010
        PostgreSQL 'archive_command' setting: barman-wal-archive barman_server pg16 %p
        Last archived WAL: 000000010000003B0000000F, at Mon Nov 18 07:54:54 2024
        Failures of WAL archiver: 0
        Passive node: False
        Retention policies: not enforced
        No. of available backups: 2
        First available backup: 20240911T085206
        Last available backup: 20240911T085736
        Minimum redundancy requirements: satisfied (2/0)

This is a good tool to collaborate that the server's configuration aligns with the
configuration of the barman server.

Barman show servers
-------------------

You can use the :ref:`barman show-servers <commands-barman-show-servers>` command to
list all the configuration options and values for a specific Postgres server configured
for backups.

.. code:: bash

    $ barman show-servers pg16
    Server local16:
        active: True
        archive_command: barman-wal-archive barman-server pg16 %p
        archive_mode: on
        archive_timeout: 0
        archived_count: 1
        ...


Barman replication status
-------------------------

You can do a quick check of the replication status for a specific Postgres server
by using the :ref:`barman replication-status <commands-barman-replication-status>`
command. This command provides a detailed view of the status of the replication.

.. code:: bash

    $ barman replication-status pg16
    Status of streaming clients for server 'pg16':
    Current LSN on master: 3B/10000148
    Number of streaming clients: 1

    1. Async WAL streamer
        Application name: barman_receive_wal
        Sync stage      : 3/3 Remote write
        Communication   : Unix domain socket
        User name       : barman
        Current state   : streaming (async)
        Replication slot: standby
        WAL sender PID  : 165959
        Started at      : 2024-11-18 07:49:01.837787+00:00
        Sent LSN   : 3B/10000148 (diff: 0 B)
        Write LSN  : 3B/10000148 (diff: 0 B)
        Flush LSN  : 3B/10000000 (diff: -328 B)


Barman diagnose
---------------

The :ref:`barman diagnose <commands-barman-diagnose>` command gathers important
information about the status of all the configured servers. It's an overall view of
the configured Postgres servers that are being backed up by Barman.

.. code:: bash

    barman diagnose


The ``diagnose`` command output is a full snapshot of the barman server, providing useful information, such as global configuration, SSH version,
Python version, ``rsync`` version, PostgreSQL clients version,
as well as current configuration and status of all servers.

The ``diagnose`` command is extremely useful for troubleshooting problems,
as it gives a global view on the status of your Barman installation.
