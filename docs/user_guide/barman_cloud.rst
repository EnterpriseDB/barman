.. _barman-cloud:

Barman for the cloud
====================

Barman offers two primary methods for backing up Postgres servers to the cloud:

* **Creating disk volume snapshots as base backups.**

  This can be achieved through 2 different approaches:

  1. Setting up a Barman server to store the backup metadata and the WAL files,
     while your backups are created as disk volume snapshots in the cloud. This is an
     integrated feature of Barman. If you choose this approach, please consult the 
     :ref:`cloud snapshots backups <backup-cloud-snapshot-backups>` section for details.
  2. Interacting and managing backups directly with the command line utility provided by
     the barman cloud client package without the need of a Barman server. The backup
     metadata and the WAL files are stored in the cloud object storage, while your base
     backup is created as disk volume snapshots in the cloud.

* **Creating and transferring base backups to a cloud object storage.**

  This can also be achieved through 2 different approaches:

  1. Using the utility provided by the barman cloud client package in the Postgres host,
     without a Barman server. Both the base backup and the WALs are read from the local
     host (Postgres host), and they are stored along with the backup metadata in the
     cloud object storage.
  2. Setting up a Barman server to take base backups and store the backup metadata and
     the WAL files, then use the utility provided by the barman cloud client package as
     hook scripts to copy them to the cloud object storage. If you choose this approach,
     please consult the :ref:`hook-scripts-using-barman-cloud-scripts-as-hooks-in-barman`
     section for details.
  
This section of the documentation is focused in the ``barman-cloud-*`` commands that
can be used to manage and interact with backups without the need of a dedicated barman
server. To start working with it, you will need to install the barman cloud client
package on the same machine as your Postgres server.

Understanding these options will help you select the right approach for your cloud
backup and recovery needs, ensuring you leverage Barman's full potential.

.. _barman-cloud-barman-client-package:

Barman cloud client package
---------------------------

The barman cloud client package provides commands for managing cloud backups, both in
object storage and as disk volume snapshots, without requiring a Barman server.

With this utility, you can:

* Create and manage snapshot backups directly.
* Create and transfer backups to cloud object storage.

While it offers additional functionality for handling backups in cloud storage and disk
volumes independently, it does not fully extend Barman's native capabilities. It has
limitations compared to the integrated features of Barman and some operations may
differ.

.. note::
  Barman supports AWS S3 (and S3 compatible object stores), Azure Blob Storage
  and Google Cloud Storage.

.. _barman-cloud-installation:

Installation
------------

To back up Postgres servers directly to a cloud provider, you need to install the
`barman cloud client package` on those servers. Keep in mind that the installation
process varies based on the distribution you are using.

Refer to the :ref:`installation <installation>` section for the installation process,
and make sure to note the important information for each distribution.

.. _barman-cloud-commands-reference:

Commands Reference
------------------

You have several commands available to manage backup and recovery in the cloud using
this utility. The exit statuses for them are ``SUCCESS`` (0), ``FAILURE`` (1),
``FAILED CONNECTION`` (2) and ``INPUT_ERROR`` (3). Any other non-zero is ``FAILURE``.

.. include:: commands/barman_cloud/backup.inc.rst
.. include:: commands/barman_cloud/backup_delete.inc.rst
.. include:: commands/barman_cloud/backup_show.inc.rst
.. include:: commands/barman_cloud/backup_list.inc.rst
.. include:: commands/barman_cloud/backup_keep.inc.rst
.. include:: commands/barman_cloud/check_wal_archive.inc.rst
.. include:: commands/barman_cloud/restore.inc.rst
.. include:: commands/barman_cloud/wal_archive.inc.rst
.. include:: commands/barman_cloud/wal_restore.inc.rst