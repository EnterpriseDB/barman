.. _concepts:

Concepts
========

Creating a disaster recovery plan can be challenging, especially for those unfamiliar
with the various backup concepts involved in backup management. There are many
different methods for taking backups, each with its own advantages, disadvantages, and
technical requirements. The choice of the right approach will depend on your resources,
environment and technical knowledge. Knowing that not everyone might be well-grounded
in this context, this section is dedicated to explaining the most fundamental concepts
regarding database backups, particularly in the context of Postgres and Barman.


.. _concepts-introduction:

Introduction
------------

In a perfect world, there would be no need for backups. However, it is important,
especially in critical business environments, to be prepared for when the unexpected
happens. In a database scenario, the "unexpected" could take any of the following
forms:

* Data corruption;
* System failure (including hardware failure);
* Human error;
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

Understandably, we all want RPO=0 (zero data loss) and RTO=0 (zero down-time, utopia) -
even if it is our grandmother's recipe website. In reality, a careful cost analysis
phase is required to determine your business continuity requirements.

Fortunately, with an open source stack composed of Barman and Postgres, you can achieve
RPO=0 thanks to synchronous streaming replication. RTO is more the focus of a High
Availability solution, like ``Patroni`` or ``repmgr``. Therefore, by integrating Barman any 
of these tools, you can dramatically reduce RTO to nearly zero. 

In any case, it is important for us to emphasize more on cultural aspects related to
disaster recovery, rather than the actual tools. Tools without human beings are
useless. Our mission with Barman is to promote a culture of disaster recovery that:

* Focuses on backup procedures;
* Focuses even more on recovery procedures;
* Relies on education and training on strong theoretical and practical concepts of
  Postgres crash recovery, backup, Point-In-Time-Recovery, and replication for your
  team members;
* Promotes testing your backups (only a backup that is tested can be considered to be
  valid), either manually or automatically (be creative with Barman's hook scripts!);
* Fosters regular practice of recovery procedures, by all members of your devops team
  (yes, developers too, not just system administrators and :term:`DBAs <DBA>`);
* Solicits regularly scheduled drills and disaster recovery simulations with the
  team every 3-6 months;
* Relies on continuous monitoring of Postgres and Barman, and that is able to promptly
  identify any anomalies.

Moreover, do everything you can to prepare yourself and your team for when the disaster
happens (yes, when), because when it happens:

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


.. _general-backup-concepts:

General backup concepts
-----------------------

While each database system may have its own terminology, there are fundamental backup
principles that are consistent across all relational databases. This section provides
an overview of the core concepts necessary to understand how backups work.


.. _physical-and-logical-backups:

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
like Barman. Backup management tools can play a vital role in physical backups as
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


.. _backup-types:

Backup types
^^^^^^^^^^^^

Regarding physical backups, they can essentially be divided into three different types:
full, incremental and differential.

A full backup, often also called base backup, captures all your data at a specific
point in time, essentially creating a complete snapshot of your entire database. This
type of backup contains every piece of information needed to restore the system to its
exact state as when the backup was taken. In this sense, a recovery from a consistent
full physical backup is the fastest possible as it is inherently complete by nature.

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


.. _transaction-logs:

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


.. _point-in-time-recovery:

Point-time Recovery
^^^^^^^^^^^^^^^^^^^

Point-in-time recovery enables you to restore your database to any specific moment
between the time of a base backup and the furthest point covered by your archived
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
full backups with frequent incremental backups along with transaction logs archiving.
This way, restoring to the most recent state is a matter of restoring the most recent
backup followed by a replay of subsequent transaction logs. Similarly, restoring to a
specific point in time is a matter of restoring the previous backup closest to the
target point followed by a replay of subsequent transaction logs up to the desired
target.

Postgres backup concepts and terminology
----------------------------------------

Barman concepts and terminology
-------------------------------

Outstanding features from Barman
--------------------------------
