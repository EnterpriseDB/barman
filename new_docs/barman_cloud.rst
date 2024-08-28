.. _barman_cloud:

Barman for the cloud
====================

Barman offers two primary methods for backing up PostgreSQL servers to the cloud:

* *Creating disk volume snapshots as base backups.*

  You have two options to work with snapshots:

  1. You will need to setup a barman server to store the barman metadata and WAL files,
     while your backup will be created as disk volume snapshots in the cloud. This is an
     integrated feature of Barman. If you choose this approach, please consult the 
     :ref:`backup snapshots <backup>` section for details.
  2. Interact and manage backups directly with the command line utility provided by the
     ``barman-cli-cloud`` package without the need for a barman server. The barman
     metadata and WAL files will be stored in a cloud object storage, while your backup
     will be created as disk volume snapshots in the cloud.

* *Creating and transfering base backups to a cloud object storage.*

  This method is similar to the second option of snapshots, but the base backup is stored
  in an object storage alongside the WAL files and backup metadata.
  
This section of the documentation is focused in the ``barman-cloud-*`` commands that
can be used to manage and interact with backups without the need of a dedicated barman
server. To start working with it, you will need to install the ``barman-cli-cloud``
package on the same machine as your PostgreSQL server.

Understanding these options will help you select the right approach for your cloud
backup and recovery needs, ensuring you leverage Barman's full potential.

barman-cli-cloud
----------------

The ``barman-cli-cloud`` package provides commands for managing cloud backups, both in
object storage and as disk volume snapshots, without requiring a Barman server.

With this utility, you can:

* Create and manage snapshot backups directly.
* Create and transfer backups to cloud object storage.

``barman-cli-cloud`` extends beyond Barman's native capabilities, offering commands for
handling backups in cloud storage and disk volumes independently. Its operations may
differ from Barman's integrated features.

.. note::
  Barman supports AWS S3 (and S3 compatible object stores), Azure Blob Storage
  and Google Cloud Storage.

Installation
------------

To back up PostgreSQL servers directly to a cloud provider, you need to install the
Barman client utility for the cloud on those servers.

To install the package on a RedHat/CentOS system, run:

.. code:: bash

    yum install barman-cli-cloud

On Debian/Ubuntu systems, use:

.. code:: bash

    apt-get install barman-cli-cloud

The utility package requires the appropriate library and access control for the cloud
provider you wish to use:

* `boto3 <https://github.com/boto/boto3>`_ for **AWS**.
* `azure-storage-blob and azure-indentity (optional) <https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-azure-cli&pivots=blob-storage-quickstart-scratch#install-the-packages>`_ for  **Azure**.
* `google-cloud-storage <https://pypi.org/project/google-cloud-storage/>`_ for **GCP**.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` environment
  variable is supported at the moment.

Commands Reference
------------------

You have several commands available to manage backup and recovery in the cloud using
this utility. The exit statuses for them are ``SUCCESS`` (0), ``FAILURE`` (1),
``FAILED CONNECTION`` (2) and ``INPUT_ERROR`` (3). Any other non-zero is ``FAILURE``.

.. include:: commands/barman_cloud/backup.rst
.. include:: commands/barman_cloud/backup_delete.rst
.. include:: commands/barman_cloud/backup_show.rst
.. include:: commands/barman_cloud/backup_list.rst
.. include:: commands/barman_cloud/backup_keep.rst
.. include:: commands/barman_cloud/check_wal_archive.rst
.. include:: commands/barman_cloud/restore.rst
.. include:: commands/barman_cloud/wal_archive.rst
.. include:: commands/barman_cloud/wal_restore.rst