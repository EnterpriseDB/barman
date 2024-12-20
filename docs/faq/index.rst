.. _faq:

FAQ
===

.. _faq-general:

General
-------

**Can Barman perform physical backups of Postgres instances?**

Yes. Barman is an application for physical backups of Postgres servers that manages
base backups and WAL archiving. It is a disaster recovery application. Barman does not
support logical backups (aka dumps).

**I am already using pg_dump. What is the difference between pg_dump and Barman
and why should I use Barman instead?**

If you already use ``pg_dump``, it is a good thing. However, if your business is based
on your database, logical backups (the ones performed by ``pg_dump``) are
not enough. These dumps are snapshots of your database at a particular point in time.
Usually, people perform these activities at night. If a crash occurs during the day,
all your transactions between when the dump started and the crash will be lost forever.
In this context, you need to put in place a more robust solution for disaster recovery,
based on physical backups, which allows for point in time recovery.

**I manage several Postgres instances. Can Barman manage multiple database servers?**

Yes. Barman has been designed to allow remote backups of Postgres servers. It allows
:term:`DBAs<DBA>` to manage backups of multiple servers from a centralized host. Barman
allows you to define a catalogue of backup servers for base backups and continuous
archiving of WAL segments.

**Does Barman manage replication and high availability as well? How does it compare
with repmgr, OmniPITR, walmgr and similar tools?**

No. Barman aims to be a pure disaster recovery solution. It is responsible for the sole
backup of a cluster of Postgres servers. If high availability is what you are looking
for, we encourage you to use a tool like Patroni or repmgr. Barman specifically targets
disaster recovery only, because it requires a simpler and less invasive design than the
one required to cover high availability. This is why Barman does not duplicate existing
HA tools like the ones mentioned above. You do not need to install anything on the
server. The only requirement is to configure access for the Barman client, which is
less invasive than what is required for high availability.

**Can I define retention policies for my backups?**

Yes. Barman 1.2.0 introduced support for retention policies both for base backups and
WAL segments, for every server you have. You can specify a retention policy in the
server configuration file. Retention policies can be based on time
(e.g. ``RECOVERY WINDOW OF 30 DAYS``) or number of base backups
(e.g. ``REDUNDANCY 3``).

**Can I specify different retention policies for base backups and WAL segments?**

Yes, definitely. It might happen for instance that you want to keep base backups of the
last 12 months and keep WALs for Point-In-Time-Recovery for the last month only.

**Does Barman guarantee data protection and security?**

Barman communicates with your remote/local Postgres server using SSH connections (for
process and file management) and the standard Postgres connection (for querying the
database). It is the system administrator's duty to make sure that SSH communications
occur in a secure way. Similarly, it is the database administrator's duty to make sure
communications with the database server occur in a secure way.

**Is the Barman interface complex or hard to understand?**

Barman has a simple console interface through which you can run its several
commands e.g. listing servers, listing backups, show detailed information of a backup,
etc. Launching a new base backup for a server is also trivial, as well as restoring it,
either locally or remotely.

**Can Barman manage backups in the cloud e.g. using AWS S3 or Azure Blob Storage?**

Yes. Barman can currently manage backups in Amazon, Azure and Google cloud providers.
This backup method is called snapshot backups in Barman terminology. In this scenario,
backups are taken via a snapshot of the storage volume where your database server
resides. In this case, Barman act mainly as a storage server for your WAL files and
backups catalog. You can check the :ref:`Barman Cloud <barman-cloud>` and
:ref:`Cloud Snapshot Backups <backup-cloud-snapshot-backups>` sections for further
details.

**Do you have packages for RedHat and Debian based distributions?**

Barman official packages are provided by :term:`PGDG`. Barman packages can be found in
alternative repositories, but we recommend using :term:`PGDG` repositories because it
ensures compatibility, stability and access to the latest updates. Refer to the
:ref:`installation <installation>` section for further details.

**Does Barman allow me to limit the bandwidth usage for backup and recovery?**

Yes. Barman 1.2.1 introduces support for bandwidth limitation at global, server and
tablespace level.

.. _faq-backup:

Backup
------

**Does Barman support incremental backups**

Yes. Barman currently supports file-level and block-level incremental backups,
depending on the backup method in use and the Postgres version to backup.

**How many backups can Barman handle for each Postgres instance?**

Barman allows the storage of multiple base backups per database server. The only
limit on the number of backups is the disk space on the Barman host.

**I have continuous archiving in place, but managing WAL files and understanding which
ones “belong” to a particular base backup is not obvious. Can Barman simplify this?**

Fortunately, yes. The way Barman works is by keeping separate the base backups from WAL
segments of a specific server. Even though they are much related, Barman sees a WAL
archive as a continuous stream of data from the first base backup to the last
available. A neat feature of Barman is to link every WAL file to a base backup so that
it is possible to determine the size of a backup in terms of two components: base
backup and WAL segments.

**Can Barman compress base backups?**

Currently, Barman can compress backups using ``backup_method = postgres``, thanks to
``pg_basebackup`` compression features. This can be enabled using the
``backup_compression`` config option. For Rsync-based backups, at the moment there is
no compression method, but it is feasible and the current design allows it.
You can check the :ref:`Backup Compression <backup-backup-compression>` section for
further details.

**Can Barman compress WAL segments?**

Yes. You can specify a compression filter for WAL segments, and significantly reduce
the size of your WAL files by 5/10 times. This is done by setting the ``compression``
option in the configurations. Refer to the :ref:`WAL configuration <configuration-options-wals>`
section for further details.

**Can Barman back up tablespaces?**

Yes. Tablespaces are handled transparently and automatically by Barman.

**Can I backup from a Postgres standby server?**

Yes, Barman natively supports backup from standby servers for both ``postgres`` and
``rsync`` backup methods.


**What's the difference between Full and Incremental backups when using the rsync backup
method in Barman?**

With the ``rsync`` backup method, the on-disk backup has no clear distinction between
a full and an incremental backup. In practice, all backups created with
``rsync`` are full backups, but they may share common files using hard-links, which
reduces storage space and speeds up backup creation.

When using ``rsync`` with ``reuse_backup = link``, files that are exactly the same
since the last backup are not copied again; instead, hard links to the existing files
are created. This makes the backups appear to full as all files are completely available
in the backup folder, yet the real size used on-disk and transfered is less because
unchanged files are linked rather than duplicated. For this reason, each rsync backup is
a full "snapshot", independent of previous backups, and deleting any of the backups
would not alter any of the others.

In contrast, with the ``backup_method = postgres`` method (Postgres 17+), incremental
backups depend on a chain of backups, and restoring an incremental backup requires
combining it with its full backup and any intermediate incremental backups. In This
case, deleting an incremental backup would invalidate the following incremental
backups.

To summarize, while rsync backups are file-level incremental in that they avoid
duplicating unchanged files, each backup remains a full "snapshot", independent of
previous ones.


.. _faq-installation-and-configuration:

Installation & Configuration
----------------------------

**Does Barman have to be installed on the same server as Postgres?**

No. Barman does not necessarily need to be on the same host where Postgres is
running. It is your choice to install it locally or on another server (usually
dedicated for backup purposes).  We strongly recommend having a dedicated server
for Barman.

**Can I have multiple configurations for different users in a Barman server?**

Yes. Barman needs a configuration file. You can have a system wide configuration
(``/etc/barman.conf``) or a user configuration (``~/.barman.conf``). For instance, you
could set up several users in your system that use Barman, each of those working on a
subset of your managed Postgres servers. This way you can protect your backups on a
user basis.

.. _faq-recovery:

Recovery
--------

**Does Barman manage recovery?**

Yes. With Barman, you can recover a Postgres instance on your backup server or a
remote node. Recovering remotely is just a matter of specifying an SSH command in the
``recover`` command, which Barman will use to connect to the destination host in order
to restore the backup.

**Does Barman manage recovery to a specific transaction or to a specific time?**

Yes. Barman allows you to perform point-in-time recovery by specifying a timestamp,
transaction ID, a Log Sequence Number (LSN) or a named restore point created
previously. It is just a matter of adding an extra option to your ``recover`` command.
You can refer to :ref:`Point-in-Time Recovery <recovery-point-in-time-recovery>` under
Recovery section for further details.

**Does Barman support timelines?**

Yes, Barman handles Postgres timelines for recovery.

**Does Barman handle tablespaces and their mapping during recovery operations?**

Yes. By default, tablespaces are restored to the same path they had on the source
server. You can remap them as you wish by specifying the ``--tablespace`` option in
your ``recover`` command.

**During recovery, does Barman allow me to relocate the PGDATA directory? What about
tablespaces?**

Yes. When recovering a server, you can specify different locations for your ``PGDATA``
directory and all your tablespaces, if any. This allows you to set up temporary sandbox
servers. This is particularly useful in cases where you want to recover a table that
you have unintentionally dropped from the master by dumping the table from the sandbox
server and then recreating it in your master server.

.. _faq-requirements:

Requirements
------------

**Does Barman work on Windows?**

Barman can take backups of your Postgres servers on Windows. The recovery part 
is not supported. Additionally, Barman will have to run on a UNIX box.



