.. _concepts:

Concepts
========

Creating a disaster recovery plan can be challenging, especially for those unfamiliar
with the various concepts involved in backup management. There are many different
methods for taking backups, each with its own advantages, disadvantages, and technical
requirements. The choice of the right approach will depend on your resources,
environment and technical knowledge. Knowing that not everyone might be well-grounded
in this context, this section is dedicated to explaining the most fundamental concepts
regarding database backups, particularly in the context of Postgres and Barman.

If you are already familiar with the concepts of backups, logical and physical backups
in Postgres, feel free to skip to the :ref:`Barman concepts and terminology <concepts-barman-concepts-and-terminology>`
section.

.. _concepts-introduction:

Introduction
------------

In a perfect world, backups wouldn't be necessary. However, it is important,
especially in critical business environments, to be prepared for when the unexpected
happens. In a database scenario, the "unexpected" could take any of the following
forms:

* Data corruption.
* System failure (including hardware failure).
* Human error.
* Natural disaster.

In such cases, any :term:`ICT` manager or :term:`DBA` should be able to fix the
incident and recover the database in the shortest time possible. We normally refer to
this discipline as disaster recovery, and more broadly as business continuity.

Within business continuity, it is important to familiarize yourself with two
fundamental metrics, as defined by Wikipedia:

* Recovery Point Objective (RPO): the maximum targeted period in which data might be
  lost from an IT service due to a major incident.
* Recovery Time Objective (RTO): the targeted duration of time and a service level
  within which a business process must be restored after a disaster (or disruption) in
  order to avoid unacceptable consequences associated with a breakage in business
  continuity.

In a few words, RPO represents the maximum amount of data you can afford to lose, while
RTO represents the maximum down-time you can afford for your service.

Understandably, we all want RPO=0 (zero data loss) and RTO=0 (zero down-time, utopia),
even if it is our grandmother's recipe website. In reality, a careful cost analysis
phase is required to determine your business continuity requirements.

Fortunately, with an open source stack composed of Barman and Postgres, you can achieve
RPO=0 thanks to synchronous streaming replication. RTO is more the focus of a High
Availability solution, like ``Patroni`` or ``repmgr``. Therefore, by integrating Barman
with any of these tools, you can dramatically reduce RTO to nearly zero.

In any case, it is important for us to emphasize more on cultural aspects related to
disaster recovery, rather than the actual tools. Tools without human beings are
useless. Our mission with Barman is to promote a culture of disaster recovery that:

* Focuses on backup procedures.
* Focuses even more on recovery procedures.
* Relies on education and training on strong theoretical and practical concepts of
  Postgres crash recovery, backup, Point-In-Time-Recovery, and replication for your
  team members.
* Promotes testing your backups (only a backup that is tested can be considered to be
  valid), either manually or automatically (be creative with Barman's hook scripts!).
* Fosters regular practice of recovery procedures, by all members of your devops team
  (yes, developers too, not just system administrators and :term:`DBAs <DBA>`).
* Solicits regularly scheduled drills and disaster recovery simulations with the
  team every 3-6 months.
* Relies on continuous monitoring of Postgres and Barman, and that is able to promptly
  identify any anomalies.

Moreover, do everything you can to prepare yourself and your team for when the disaster
happens, because when it happens:

* It is going to be a Friday evening, most likely right when you are about to leave the
  office.
* It is going to be when you are on holiday (right in the middle of your cruise around
  the world) and somebody else has to deal with it.
* It is certainly going to be stressful.
* You will regret not being sure that the last available backup is valid.
* Unless you know how long it approximately takes to recover, every second will seem
  like forever.

In 2011, with these goals in mind, 2ndQuadrant started the development of Barman, now
one of the most used backup tools for Postgres. Barman is an acronym for "Backup and
Recovery Manager".

Be prepared, don't be scared.


.. _concepts-general-backup-concepts:

General backup concepts
-----------------------

While each database system may have its own terminology, there are fundamental backup
principles that are consistent across all relational databases. This section provides
an overview of the core concepts necessary to understand how backups work.


.. _concepts-general-backup-concepts-physical-and-logical-backups:

Physical and logical backups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In the context of relational databases, a logical backup is nothing more than a series
of operations that, when executed, recreate your data in the exact state as when the
backup was taken. To put it simple, it is a sequence of SQL statements to reconstruct
the database structure and data. This method is not dependent on specific environment
specifications such as database version or system architecture, making it a suitable
choice for migrating data across incompatible systems. It also offers more flexibility
by allowing the restoration of specific database objects or tables. 

However, logical backups can be time-consuming, potentially taking several hours or
days, depending on the size of the database. Also, because the backup reflects the
database's state at the start of the backup process, any changes made during the backup
are not captured, introducing potential windows for data loss. As a result, this method
is typically recommended for smaller and less complex databases. In Postgres, logical
backups are implemented via the ``pg_dump`` and ``pg_dumpall`` utilities.

A physical backup, on the other hand, works by copying the database files directly from
the file system. Therefore, this method is usually tied to environment specifications.
There are different approaches to taking physical backups, ranging from using basic
Unix tools like ``cp`` to more sophisticated solutions such as using backup managers,
like Barman. Backup management tools can play a vital role in physical backups, as
ensuring the files represent a consistent state of the database, while also keeping the
server running normally, can be challenging if done manually.

Physical backups can be much faster than logical backups, not only during the backup
process but especially during recovery, since they do not require a complete replay of
operations in order to recreate the database. It also enables the possibility of
incremental backups, which significantlly reduces time and storage usage, allowing for
more frequent backups. Finally, one of the greatest advantages of this approach is the
ability to perform point-in-time recovery (PITR), which allows you to restore your
database to any specific point in time between the current time and the time of the
backup. This feature is only possible when transaction logs are archived alongside your
physical backups.

As noticed, physical backups are more robust but also more complex. For this reason,
auxiliary backup management tools, like Barman for Postgres, play an important role in
ensuring this process is handled effectively and reliably in your disaster recovery
plan.


.. _concepts-general-backup-concepts-backup-types:

Backup types
^^^^^^^^^^^^

Regarding physical backups, they can essentially be divided into three different types:
full, incremental and differential.

A full backup, often also called base backup, captures all your data at a specific
point in time, essentially creating a complete snapshot of your entire database. This
type of backup contains every piece of information needed to restore the system to its
exact state as when the backup was taken. In this sense, a recovery from a consistent
full physical backup is the fastest possible, as it is inherently complete by nature.

Incremental backups, on the other hand, are designed to capture only the changes that
have occurred since a previous backup. A previous backup could be either a full backup
or another incremental backup. Incremental backups significantly reduce the time and
storage usage, allowing for more frequent backups, consequently reducing the risks of
data loss. Generally, Incremental backups are only possible with physical backups as
they rely on low-level data structures, such as files and internal data blocks, to
determine what has actually changed. A recovery from an incremental backup requires a
chain of all backups, from the base to the most recent incremental.

Lastly, differential backups are similar to incremental backups in that they capture
only the changes made since a previous backup. However, the key difference is that a
differential backup always records changes relative to a full backup, never being
relative to an incremental or another differential backup. In fact, every differential
backup is an incremental backup, but not every incremental backup is a differential
backup. A recovery in this case only requires the most recent differential backup and
its related base backup.


.. _concepts-general-backup-concepts-transaction-logs:

Transaction logs
^^^^^^^^^^^^^^^^

Transaction logs are a fundamental piece of most relational databases. It consists of a
series of contiguous files that record every operation in the database before they are
executed. As a result, they possess every change that happened in the database during a
period of time.  It primarily ensures that databases can effectively recover from
crashes by being able to replay any operations that were not yet flushed to disk before
the crash, thus preventing data loss. It is also a key component of many
implementations of database replication.

Transaction logs are recycled after all its operations are persisted. However, if we
are able to archive these logs in advance, we essentially retain a complete record of
all changes made to the database that can be replayed at any time. Having a base backup
along with transaction logs archival enables for continuous backups. This is
particularly valuable for large databases where it is not possible to take full backups
regularly. You might notice that it achieves a similar goal as differential backups,
but with even more capabilities as it also enables more robust features such as
point-in-time recovery.


.. _concepts-general-backup-concepts-point-in-time-recovery:

Point-time Recovery
^^^^^^^^^^^^^^^^^^^

Point-in-time recovery enables you to restore your database to any specific moment
from the end-time of a base backup to the furthest point covered by your archived
transaction logs. By maintaining a continuous archive of transaction logs, you have the
ability to replay every change made to the database up to the present moment. This is
done by replaying all transaction logs on top of a base backup, also providing you with
the ability to stop the replay at any point you want based on a desired timestamp or
transaction ID, for example. PITR allows for a precision of less than seconds, a huge
advantage over standard full backups, which are usually executed daily.

This feature is especially valuable in situations where human error or unintended
changes occur, such as accidental deletions or modifications. By restoring the database
to the exact state it was just before the unwanted event, PITR significantly reduces
RPO. It provides a powerful safeguard, ensuring that critical data can be quickly and
accurately recovered without reverting the database to an earlier full backup and risk
losing all subsequent legitimate changes.

It does not mean, however, that PITR is the solution to all problems. Replaying
transaction logs can still take a long time depending on how far they go from the base
backup. Therefore, the optimal solution is actually a combination of all strategies:
full backups with frequent incremental backups along with transaction log archiving.
This way, restoring to the most recent state is a matter of restoring the most recent
backup followed by a replay of subsequent transaction logs. Similarly, restoring to a
specific point in time is a matter of restoring the previous backup closest to the
target point followed by a replay of subsequent transaction logs up to the desired
target.


.. _concepts-postgres-backup-concepts:

Postgres backup concepts and terminology
----------------------------------------

This section explores backup concepts in the context of Postgres, its implementations
and specific characteristics. The content is mainly based on the `Backup and Restore
section from the Postgres official documentation <https://www.postgresql.org/docs/current/backup.html>`_,
so we strongly recommend you read that if you want more detailed explanations on how
Postgres handles backups.


.. _concepts-postgres-backup-concepts-pgdump-vs-pgbasebackup:

pg_dump vs pg_basebackup
^^^^^^^^^^^^^^^^^^^^^^^^

There are essentially two main tools for taking backups in Postgres: ``pg_dump`` and
``pg_basebackup``. The difference between them is essentially the difference between
logical and physical backups. Namely, ``pg_dump`` (``pg_dumpall`` included) takes
logical backups while ``pg_basebackup`` takes physical backups.

.. note::

    Barman does not make use of ``pg_dump`` or ``pg_dumpall`` in any way as it does not
    operate with logical backups. ``pg_basebackup`` is used by Barman depending on the
    backup method configured.

``pg_basebackup`` essentially copies all files from your Postgres cluster to a
destination directory, including tablespaces, if any, using the `streaming replication
protocol <https://www.postgresql.org/docs/current/protocol-replication.html>`_.
It can only backup the entire cluster, not being able to backup specific databases or
objects. ``pg_basebackup`` is responsible for putting your database server in and out
of backup mode as well as making sure all required transaction logs for consistency are
stored along with the base backup. For that reason, unlike ``pg_dump``, a backup taken
with ``pg_basebackup`` also includes changes that happened while the backup was in
progress, which is a huge advantage for databases under frequent heavy load. You can
read more about ``pg_basebackup`` in its `dedicated section in the official
documentation <https://www.postgresql.org/docs/current/app-pgbasebackup.html>`_.

.. note::

    In reality, a physical backup in Postgres is only complete/self-contained if it
    also has at least the transaction logs (WALs in Postgres) that were generated
    during the backup process. Otherwise the backup itself is insufficient to restore
    and start a new Postgres instance.

It is also possible to accomplish a similar result as ``pg_basebackup`` using the
`Postgres low-level backup API <https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-LOWLEVEL-BASE-BACKUP>`_,
which is yet another way of taking physical backups in Postgres. The low-level API is
used in cases where you want to take physical backups manually using alternative
copying tools. In this scenario, you are responsible for putting the database server
in and out of backup mode manually as well as making sure all transaction logs required
for consistency are archived correctly.

.. note::

    Barman uses the Postgres low-level API depending on the backup method configured
    e.g. ``backup_method = rsync``.


.. _concepts-postgres-backup-concepts-wals:

Write-ahead logs
^^^^^^^^^^^^^^^^

Write-ahead logs (WAL) is how Postgres (and other databases) refer to transaction logs.
In Postgres, each WAL file supports 16 MB worth of changes (configurable). WAL files
are written sequentially, one after another, and are maintained simultaneously until a
checkpoint is performed. A checkpoint in Postgres is the act of persisting all changes
to disk so that WALs can be recycled afterwards. A checkpoint usually happens every
five minutes or after 1 GB of WAL files are generated, both options are configurable.
WAL not only helps with crash recovery, database replication and PITR, but it's also an
important component to ensure good performance, as otherwise changes would need to be
synced to disk after each transaction commit, resulting in huge I/Os. With WAL, changes
can be postponed to a checkpoint-time since it is sufficient to ensure database
consistency.


.. _concepts-postgres-backup-concepts-wal-archiving-and-wal-streaming:

WAL archiving and WAL streaming
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transaction log archiving is known as "continuous archiving" or "WAL archiving" in
Postgres. WAL archiving essentially means being able to store WAL files somewhere else
before they are recycled. In Postgres, the traditional way of doing that is via the
``archive_command`` parameter in the server configuration.

The ``archive_command`` accepts any shell command as a value, which will be executed
for each WAL file once completely filled. Such a command is responsible for making sure
each file is copied safely to the desired destination. This provides a lot of
flexibility in the sense that Postgres does not make any assumptions on how or where 
you want to store these files, thus allowing you to use any command or library you want.
This command must return a zero exit status, indicating success, otherwise Postgres
understands that the archiving has failed and will not recycle those files until they
can be successfully archived. While this is helpful for ensuring safety it can also
become a nightmare if your command starts failing for some reason as WAL files will
continue to pile up until it works again or you run out of disk space.
You can read more about `WAL archiving in the official documentation
<https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-ARCHIVING-WAL>`_.

An alternative way of archiving WALs is by using ``pg_receivewal``, a native Postgres
utility used to transfer WAL files to a desired location using the streaming
replication protocol. A huge advantage of this method, commonly known as WAL streaming,
compared to the traditional ``archive_command`` method is that files are transferred in
real time, meaning that it doesn't need to wait for a WAL segment to be completely
filled in order to start transferring it, significantly reducing the chances of data
loss.

Unlike the ``archive_command``, by default this method alone does not ensure that WAL
files are archived successfully before being recycled. This means that WAL files can be
recycled before being archived, essentially having its logs lost forever. For this
reason, the use of replication slots is extremely recommended in this scenario.
Replication slots are primarily used in the context of database replication to ensure
that the primary server will retain WAL files needed by its following replicas until
they are successfully received, providing an extra safety in case a replica goes
offline or gets disconnected. It achieves the same goal when used with
``pg_receivewal`` i.e. making sure WAL files are not recycled until successfully
transferred to the receiver.

.. _concepts-postgres-backup-concepts-recovery:

Recovery 
^^^^^^^^

The recovery process in Postgres depends on the backup type. With logical backups, this
process is as simple as running ``pg_restore`` or simply executing all SQL commands
from the backup file, depending on the backup file format. With physical backups,
however, the process is a bit more complex.

To successfully recover from a physical backup, you need both the cluster files and its
WAL archive. It is necessary to have at least the WAL files that were generated during
the backup process. If the backup was taken with ``pg_basebackup``, the required WAL
files will already be included in the output directory, unless specified otherwise. If
taken manually, however, using the Postgres low-level API, it is your responsibility to
make sure all required WAL files are available during recovery.

To prepare for recovery, you need to follow a few steps. This includes specifying
a few parameters in the configuration file of the backup cluster directory, such as a
command to get the WAL files from the WAL archive as well as a target point, in case
performing :term:`PITR`, among others. For a detailed explanation of this process,
refer to the `Postgres official documentation <https://www.postgresql.org/docs/current/continuous-archiving.html#BACKUP-PITR-RECOVERY>`_.
If everything is correct, you should then be able to start a new instance from the
backup and Postgres will make sure all required WALs are applied.

If the recovery involves Postgres incremental backups, you will then need to first
combine all the backups using ``pg_combinebackup``. It will generate a synthetic full
backup, which can be used for recovery in the same way as a standard full backup.

.. _concepts-barman-concepts-and-terminology:

Barman concepts and terminology
-------------------------------

This section offers an overview of important Barman concepts as well as demonstrates
how Barman utilizes some of the concepts explained in earlier sections.


.. _concepts-barman-concepts-server:

Server
^^^^^^

Barman can manage backups of multiple database servers simultaneously. For this reason,
a logical separation of your backup servers becomes necessary. In Barman, a backup
server, or simply server, represents the backup context of a specific database server.
It defines how Barman interacts with the database instance, how its backups are
managed, their retention policies, etc. Each server has its own dedicated directory
where all backups and WAL files are stored as well as a unique name which must be
supplied in most Barman commands to specify in which context it should run.


.. _concepts-barman-concepts-backup-methods:

Backup methods
^^^^^^^^^^^^^^

As outlined in
:ref:`Postgres backup concepts and terminology <concepts-postgres-backup-concepts>`,
there are multiple ways to back up a Postgres server. In the context of Barman, these
are referred to as backup methods. Barman supports various backup methods, each relying
on different Postgres features, with its own set of requirements, advantages, and
disadvantages. The desired backup method can be specified using the ``backup_method``
parameter in the server's configuration file.

.. note::
  It is highly recommended to use a single backup method when managing your Barman
  server. If you need to switch backup methods, it's advisable to set up a new Barman
  server.

.. _concepts-barman-concepts-rsync-backups:

Rsync backups
^^^^^^^^^^^^^

Backups taken with ``backup_method = rsync``. When using this backup method, Barman
uses the Postgres low-level API and Rsync to manually transfer cluster files
over an SSH connection. Rsync is a powerful copying tool which allows you to
synchronize files and directories between two locations, either on the same host or on
different hosts over a network. Barman utilizes the low-level API to put the server in
and out of backup mode while using Rsync to copy all relevant files to the server's
designated directory on Barman. At the end of this process, Barman forces a WAL switch
on the database server to ensure that all required WAL files are archived. Finally,
integrity checks are performed to verify that the backup is consistent.


.. _concepts-barman-concepts-streaming-backups:

Streaming Backups
^^^^^^^^^^^^^^^^^

Backups taken with ``backup_method = postgres``. When using this backup method, Barman
invokes ``pg_basebackup`` in order to back up your database server. Barman will map all
your tablespaces to the server's dedicated directory on Barman. At the end of this
process, Barman forces a WAL switch on the database server to ensure that all required
WAL files are archived. Finally, integrity checks are performed to verify that the
backup is consistent.


.. _concepts-barman-concepts-snapshot-backups:

Snapshot Backups
^^^^^^^^^^^^^^^^

Snapshot backups can be performed either by setting ``backup_method = snapshot`` or by
directly using the Barman's cloud CLI tools. These backups work by integrating Barman
with a cloud provider where your database server resides. A snapshot of the database's
storage volume is then taken as a physical backup. In this setup, Barman manages your
backups in the cloud, acting primarily as a storage server for WAL files and the
backups catalog.


.. _concepts-barman-concepts-file-level-incremental-backups:

File-level incremental backups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

File-level incremental backups are possible when using
:ref:`rsync <concepts-barman-concepts-rsync-backups>` backups. It uses Rsync native features of
deduplication, which relies on filesystem hard-links. When performing a file-level
incremental backup, Barman first creates hard-links to the latest server backup
available, essentially replicating its content in a different directory without
consuming extra disk space. Rsync is then used to synchronize its contents with the
the contents of the Postgres cluster, copying only the files that have changed.
You can also have file-level incremental backups without using hard-links, in which
case Barman will first copy the contents of the previous backup to the new backup
directory, essentially duplicating it and consuming extra disk space, but still copying
only changed files from the database server.


.. _concepts-barman-concepts-block-level-incremental-backups:

Block-level incremental backups
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Block-level incremental backups are possible when using
:ref:`streaming backups <concepts-barman-concepts-streaming-backups>`. It leverages the native
``pg_basebackup`` capabilities for incremental backups, introduced in Postgres 17. This
features requires a Postgres instance with version 17 or newer that is properly
configured for native incremental backups. With block-level incremental backups, any
backup with a valid ``backup_manifest`` file can be used as a reference for
deduplication. Block-level incremental backups are more efficient than file-level
incremental backups as deduplication happens at the block level (pages in Postgres).


.. _concepts-barman-concepts-wal-archiving:

WAL archiving via ``archive_command``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This is one of the two ways of transferring WAL files to Barman. Commonly used along
with :ref:`rsync <concepts-barman-concepts-rsync-backups>` backups, this approach
involves configuring the ``archive_command`` parameter in Postgres to archive WAL files
directly to the server's dedicated directory on Barman. The command can be either an
Rsync command, where you manually specify the server's WAL directory on the Barman
host, or the ``barman-wal-archive`` utility, which only requires the server name, with
Barman handling the rest. Additionally, ``barman-wal-archive`` provides added safety by
ensuring files are fsynced as soon as they are received.


.. _concepts-barman-concepts-wal-streaming:

WAL streaming
^^^^^^^^^^^^^

This is one of the two ways of transferring WAL files to Barman. Commonly used along
with :ref:`streaming backups <concepts-barman-concepts-streaming-backups>`, this
approach relies on the ``pg_receivewal`` utility to transfer WAL files.  It is much
simpler to configure, as no manual configuration is required on the database server.
As mentioned in :ref:`WAL archiving and WAL streaming <concepts-postgres-backup-concepts-wal-archiving-and-wal-streaming>`,
replication slots are recommended when using WAL streaming. You can create a slot
manually beforehand or let Barman create them for you by setting ``create_slot`` to
``auto`` in your backup server configurations.

.. _concepts-barman-concepts-hook-scripts:

Hook Scripts
^^^^^^^^^^^^

Barman enables developers to execute hook scripts along specific operations as
pre- and/or post-operations. This feature provides developers with the flexibility to
implement tailored and diverse behaviors. You can utilize a post-backup script to
generate a manifest for the backup when using rsync or you can create a hybrid
distributed architecture that allows you to copy backups to cloud storage as well by
combining post-backup :ref:`hook scripts with barman-cloud <hook-scripts-using-barman-cloud-scripts-as-hooks-in-barman>`
commands.

For a more in-depth exploration of this topic, please refer to the main section on
:ref:`hook scripts <hook-scripts>`.

.. _concepts-barman-concepts-restore-and-recover:

Restore and recover
^^^^^^^^^^^^^^^^^^^

In Barman, recovery is the process of restoring a backup along with all necessary WAL
files in a new location, effectively preparing a Postgres instance for recovery.

As outlined in :ref:`concepts-postgres-backup-concepts-recovery`, the recovery process
in Postgres consists of several steps, from preparing the base directory to starting the
server itself. Barman is able to perform all the steps required to prepare your backup
to be recovered, a process known as "restore" in Barman's terminology. In this case,
completing the recovery is usually just a matter of starting the server so that
Postgres can apply the required WALs and go live.
