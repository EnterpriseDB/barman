.. _catalog:

Catalog information
===================


The backup catalog  is a comprehensive record that keeps track of all servers and
backups on the Barman node. Note that servers are the PostgreSQL database systems that
are being tracked and backed up by Barman. Each server is configured within the Barman
node to ensure that its data is regularly backed up and can be restored if needed. The
Barman node is another server that hosts the Barman software, which manages the backup
and recovery processes for these PostgreSQL servers. It communicates with the configured
servers to perform backups, store them securely, and maintain a detailed catalog of all
backup activities.

It is essential for effective backup management and recovery operations, offering an
unified interface that makes it a key component of the Barman tool. It plays a central
role in organizing, monitoring, and executing backup and recovery tasks by providing a
comprehensive view and precise control over all backup activities. This streamlined
approach enhances efficiency, simplifies management, and ensures seamless recovery
processes.

The global configuration option ``barman_home`` specifies the directory where Barman
stores and manages backups for multiple servers. By default, this directory is set to
``/var/lib/barman``, but it can be customized by modifying the global configuration file
found at ``/etc/barman.conf``. Within this directory, backups from each server are
organized into separate subdirectories, with each server's backups located under
``<barman_home>/<SERVER_NAME>/base``. This structure helps keep backups organized and
easily accessible.

Purpose
-------

Serve as a centralized repository that keeps track of all PostgreSQL server and
backup-related information. 

Here are some key roles it plays:

* **Backup Metadata Storage**: It stores metadata about each backup, such as the
  backup's start and end times, the status, the specific PostgreSQL instance it was taken
  from and many other metrics. This metadata helps in tracking and managing the backup
  lifecycle.

* **Backup Management**: The catalog provides a way to organize and manage backups
  efficiently. By keeping detailed records, it simplifies operations such as listing
  available backups, checking the status of backups, identifying the latest or
  specific backups, and applying retention policies.

* **Restore Operations**: During a restore operation, the catalog helps in quickly
  identifying which backups are available and their details. This facilitates efficient
  restoration by providing necessary information about the backups and their associated
  archive logs.

* **Ease of Use**: It simplifies the process of managing multiple backups and their
  associated metadata, making it easier for administrators to handle large numbers of
  backups and complex backup strategies.

Usage
-----

Barman offers a straightforward terminal interface for managing PostgreSQL backups and
interacting with the backup catalog. This interface provides a range of sub-commands for
both server management and backup operations. All Barman sub-commands can be found in
the :ref:`sub commands <commands-sub-commands>` section, including two important ones
which are ``list-backups`` and ``show-backup``. These commands can be found below with
an example.

.. _catalog-usage-list-backups:

``list-backups``
""""""""""""""""

Show available backups for a server. This command is useful to retrieve a list o backups
with minimal yet important informations, such as backup ID and the backup type.

For example:

.. code-block:: text
        
    svr_pg17 20240901T103000 - F - Mon Nov  2 15:12:03 2024 - Size: 820.0 MiB - WAL Size: 22 MiB

* ``svr_pg17`` is the server name.
* ``20240901T103000`` is the backup ID.
* ``F`` is the backup type label.
* ``Nov  1 11:12:03 2024`` is the date and time the backup operation ended.
* ``Size: 820.0 MiB`` is the size of the backup.
* ``WAL Size: 22 MiB`` is the size of all WAL files related to this backup.


.. note::
  The backup type label can be ``F`` for full backups and ``I`` for block-level
  incremental backups. ``R`` for rsync backups and ``S`` for cloud snapshot backups.

.. _catalog-usage-show-backup:

``show-backup``
"""""""""""""""

Show detailed information about a specific backup. For example, a block-level incremental
backup:

.. code-block:: text
    
    Backup 20240902T130000:
      Server Name            : prod_pg17
      Status                 : DONE
      PostgreSQL Version     : 170000
      PGDATA directory       : /var/lib/pgsql/17/data
      Estimated Cluster Size : 244.7 GiB

      Server information:
        Checksums            : on
        WAL summarizer       : on

      Base backup information:
        Backup Method        : postgres
        Backup Type          : incremental
        Backup Size          : 3.4 GiB (36.7 GiB with WALs)
        WAL Size             : 32.3 GiB
        Resources saved      : 241.3 GiB (98.61%)
        Timeline             : 1
        Begin WAL            : 0000000100000CFD000000AD
        End WAL              : 0000000100000D0D00000008
        WAL number           : 3932
        WAL compression ratio: 79.51%
        Begin time           : 2024-09-02 13:00:01.633925+00:00
        End time             : 2024-09-03 10:27:06.522846+00:00
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
        Previous Backup      : 20240902T120001
        Next Backup          : - (this is the latest base backup)
        Root Backup          : 20240801T015504
        Parent Backup        : 20240831T016504
        Backup chain size    : 3
        Children Backup(s)   : 20240903T018515,20240903T019515

.. note::
    The output of the ``show-backup`` command can vary depending on the version of
    your PostgreSQL server and the type of backup.

    * The fields ``Root Backup``, ``Parent Backup``, ``Backup chain size`` and
      ``Children Backup(s)`` are relevant only for block-level incremental backups taken
      with ``backup_method=postgres`` on PostgreSQL 17 or newer. These fields will not be
      shown for other types of backups or older PostgreSQL versions.
    * The ``show-backup`` command relies on backup metadata. If a backup was created
      with Barman version 3.10 or earlier, it will not include fields introduced in
      version 3.11, such as those related to block-level incremental backups in
      PostgreSQL 17.
    * The field ``Resource Saved`` is available for rsync and incremental
      backups, and ``Snapshot Information`` is only available for snapshot backups.
    * The possible values for the field ``Backup Type`` are:

      * ``rsync``: for a backup taken with rsync;
      * ``full``: for a full backup taken with pg_basebackup;
      * ``incremental``: for an incremental backup taken with pg_basebackup;
      * ``snapshot``: for a snapshot-based backup taken in the cloud.
