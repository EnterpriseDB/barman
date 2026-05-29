.. _barman-cloud:

Barman for the cloud
====================

Barman provides various ways for backing up Postgres clusters to the cloud:

* **Using a Barman server**

  Having a Barman server implies that you have installed and configured Barman via
  configuration files. In this case, all metadata about backups and WALs is stored 
  locally and maintenance operations are performed periodically.

  In this mode you can:

  1. Set up a standard Barman server which stores backups and WALs locally. Then use
     the Barman :ref:`hook-scripts` to copy the backups and WALs to a cloud object
     storage. If you choose this approach, please consult the
     :ref:`hook-scripts-using-barman-cloud-scripts-as-hooks-in-barman` section for
     further details.
  2. Set up a Barman server to store metadata and WAL files locally, while your
     backups are created as disk volume snapshots in the cloud. If you choose this
     approach, please consult the :ref:`cloud snapshots backups <cloud-snapshot-backups>`
     section for details.
  3. Set up a Barman server which streams backups from the Postgres server and sends
     them directly to a cloud object storage, without ever storing the full backup
     locally. WAL files are streamed from the Postgres server and stored temporarily
     in the Barman server until successfully archived to the cloud. This is currently
     the only method that allows you to have incremental backups (block-level) in the
     cloud. If you choose this approach, please consult the
     :ref:`backup-streaming-backup-cloud` section for details.
  4. Set up Barman locally on the Postgres server, and use the ``local-to-cloud`` backup
     method to create backups directly in the cloud, without ever storing them locally.
     WAL files can be archived directly to the object store using 
     ``barman cloud-wal-archive`` as the ``archive_command``. If you choose this
     approach, please consult the :ref:`backup-local-to-cloud` section for details. 

* **Using the cloud scripts, without the need of a Barman server**

  The Barman cloud scripts allow you to manage and interact with your backups directly
  via the command line utilities provided by the ``barman-cli-cloud`` package, without
  the need of a Barman server. In this case, data and metadata are always stored in
  the cloud object storage and Barman holds no local state.

  In this mode you can:

  1. Use the utility provided by the barman cloud client package in the Postgres host,
     without a Barman server. Both the base backup and the WALs are read from the local
     host (Postgres host), and they are stored along with the metadata in the cloud
     object storage. Check the :ref:`barman-cloud-commands-reference` section below
     for details about the commands available in this mode.
  2. Use the utility provided by the barman cloud client package in the Postgres host,
     without a Barman server. Metadata and the WAL files are stored in the cloud
     object storage, while your base backup is created as disk volume snapshots
     in the cloud. Check the :ref:`barman-cloud-commands-reference` section below
     for details about the commands available in this mode, particularly with the
     ``--snapshot-*`` options.

This section of the documentation is focused on the cloud script commands, which
can be used to manage and interact with backups without the need of a dedicated
Barman server. To start working with it, you will need to install the barman cloud
client package on the same machine as your Postgres server.

Understanding these options will help you select the right approach for your cloud
backup and recovery needs, ensuring you leverage Barman's full potential.

.. important::
   S3 Compatibility Statement

   Barman Cloud utilizes the Boto3 SDK to integrate with S3-compatible object stores.
   While this ensures Barman Cloud functions correctly with the AWS S3 reference
   implementation and supports Barman Cloud integration with S3-compatible object stores,
   Barman Cloud does not directly support the underlying storage infrastructure of
   third-party vendors. If operational inconsistencies occur exclusively on third-party
   object stores which cannot be reproduced on AWS S3, this is considered API
   incompatibilities. Such issues must be resolved by the user in conjunction with their
   specific storage vendor.

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

.. note::
   Some S3 third-party providers are not compatible with the new Data Integrity
   Protection checks provided by default with ``boto3>=1.36``. If your provider falls
   in this group, there is a workaround which may help you overcome the incompatibility
   with the AWS S3.

   For this, you will have to set ``when_required`` to the following two environment
   variables shown below in order for the ``barman-cloud`` tools to work (there is no
   guarantee it will work, but we've seen that setting these make ``barman-cloud`` work
   in such environments):

   .. code-block:: bash

      AWS_REQUEST_CHECKSUM_CALCULATION=when_required
      AWS_RESPONSE_CHECKSUM_VALIDATION=when_required

.. important::
   Starting with AWS boto3 1.36, the behavior of **Data Integrity Protection checks**
   has changed. Some methods used by Barman no longer require the ``Content-MD5``
   header.

   This means that **S3-compatible storage providers that have not updated their
   server-side code may fail** when used with newer boto3 versions. For example, MinIO
   addressed this change shortly after the boto3 1.36 announcement.

   If you are using MinIO, you **must upgrade** to the latest release (or at least
   ``RELEASE.2025-02-03T21-03-04Z`` or newer) to ensure compatibility and avoid failures
   when deleting backups or releasing a keep annotation.

.. _barman-cloud-installation:

Installation
------------

To back up Postgres servers directly to a cloud provider, you need to install the
`barman cloud client package` on those servers. Keep in mind that the installation
process varies based on the distribution you are using.

Refer to the :ref:`installation <installation>` section for the installation process,
and make sure to note the important information for each distribution.

.. _barman-cloud-object-lock:

Object Lock Support
-------------------

Barman cloud can work with object locking mechanisms that provide Write-Once-Read-Many
(WORM) protection for backups stored in cloud object storage. Object locking helps
ensure compliance with data retention requirements and prevents accidental or
malicious deletion of backup files, protecting critical backup data from ransomware
attacks or unauthorized deletions.

Object locking prevents objects from being deleted or overwritten for a fixed amount
of time (retention period) or indefinitely (legal hold). This is particularly useful
for regulatory compliance requirements.

.. note::
   Object Lock verification is currently only supported for AWS S3 and S3-compatible
   storage providers.

.. important::
   Barman does not create or manage object locks. It is the user's responsibility to
   configure the cloud storage bucket with the appropriate retention policies. Barman
   can verify and respect these locks during deletion operations to ensure compliance
   with the configured policies.

S3 Object Lock
""""""""""""""

Barman can work with AWS S3 Object Lock to help maintain backup immutability. To use
this feature, you must configure your S3 bucket with the necessary Object Lock settings:

* **Object versioning enabled**: Required for Object Lock to function.
* **Default retention policy**: Configure a default retention policy for new objects
  in your bucket. This policy is managed entirely within AWS S3, not by Barman.

When these settings are configured in your S3 bucket, AWS automatically applies the
retention policy to objects uploaded through ``barman-cloud-backup`` and
``barman-cloud-wal-archive``. The objects are protected according to the bucket's
retention configuration and cannot be deleted or modified until the retention period
expires.

When deleting backups and WALs with ``barman-cloud-backup-delete``, you can use the
``--check-object-lock`` option to verify whether objects are protected by Object Lock
before attempting deletion. This flag instructs Barman to check the lock status of
backup files and respect the configured retention policies, preventing deletion attempts
that would fail and ensuring compliance with your data retention requirements.

For more information about AWS S3 Object Lock, refer to the
`AWS S3 Object Lock documentation <https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html>`_.

See the :ref:`barman-cloud-barman-cloud-backup-delete` command reference for more
details on how the ``--check-object-lock`` option works, including information about the
deletion process and performance considerations.

.. _barman-cloud-permissions:

Cloud Provider Permissions
--------------------------

This section documents the minimum IAM/access permissions required for Barman to operate
with each supported cloud provider. This is useful if you want to follow the principle
of least privilege when configuring access for Barman.

AWS S3 Permissions
""""""""""""""""""

When using AWS S3 for object storage (backups and WAL files), the following IAM
permissions are required. These permissions apply to the ``barman-cloud-*`` scripts
(``barman-cloud-backup``, ``barman-cloud-restore``, ``barman-cloud-wal-archive``, etc.)
as well as to any Barman server that has S3 as the backup or WAL destination (i.e.
have an S3 URL in ``basebackups_directory`` or ``wals_directory``).

**Minimum Required Permissions**

These permissions are required for basic backup and restore operations:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Permission
     - Purpose
   * - ``s3:CreateBucket``
     - Create the bucket if it does not exist
   * - ``s3:ListBucket``
     - List objects in the bucket, verify bucket exists (``ListObjectsV2``, ``HeadBucket``)
   * - ``s3:GetObject``
     - Download backups and WAL files, check if objects exist (``GetObject``, ``HeadObject``)
   * - ``s3:PutObject``
     - Upload backups, WAL files, and metadata (includes multipart uploads)
   * - ``s3:AbortMultipartUpload``
     - Abort failed or interrupted multipart uploads
   * - ``s3:DeleteObject``
     - Delete backups and WAL files during retention enforcement

**Conditional Permissions**

These permissions are only required when using specific features:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Permission
     - When Required
   * - ``s3:PutObjectTagging``
     - When using the ``--tags`` option to tag objects
   * - ``s3:GetObjectRetention``
     - When using ``--check-object-lock`` option before deleting backups
   * - ``s3:GetObjectLegalHold``
     - When using ``--check-object-lock`` option before deleting backups

**KMS Permissions (Server-Side Encryption)**

When using server-side encryption with AWS KMS (``--sse-kms-key-id`` option):

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Permission
     - Purpose
   * - ``kms:GenerateDataKey``
     - Required for encrypting uploaded objects
   * - ``kms:Decrypt``
     - Required for decrypting objects during restore

**Sample IAM Policy**

The following IAM policy provides the minimum permissions for S3 object storage
operations:

.. code-block:: json

   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Sid": "BucketLevelPermissions",
         "Effect": "Allow",
         "Action": [
           "s3:CreateBucket",
           "s3:ListBucket"
         ],
         "Resource": "arn:aws:s3:::your-bucket-name"
       },
       {
         "Sid": "ObjectLevelPermissions",
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:AbortMultipartUpload",
           "s3:DeleteObject"
         ],
         "Resource": "arn:aws:s3:::your-bucket-name/*"
       }
     ]
   }

.. note::
   For EBS snapshot backups (using ``backup_method=snapshot``), additional EC2
   permissions are required. Refer to the
   :ref:`cloud snapshot backups <cloud-snapshot-backups>` section for details on
   EC2 permissions.

.. _barman-cloud-commands-reference:

Commands Reference
------------------

You have several commands available to manage backup and recovery in the cloud using
this utility. The exit statuses for them are ``SUCCESS`` (0), ``FAILURE`` (1),
``FAILED CONNECTION`` (2) and ``INPUT_ERROR`` (3). Any other non-zero is ``FAILURE``.

.. note::
  When running Barman cloud commands, it is possible to specify some backup
  :ref:`shortcuts <commands-shortcuts>` instead of a backup ID. The cloud commands
  support the following shortcuts: ``first``/``oldest``, ``last``/``latest`` and
  ``last-failed``.

.. include:: commands/barman_cloud/backup.inc.rst
.. include:: commands/barman_cloud/backup_delete.inc.rst
.. include:: commands/barman_cloud/backup_show.inc.rst
.. include:: commands/barman_cloud/backup_list.inc.rst
.. include:: commands/barman_cloud/backup_keep.inc.rst
.. include:: commands/barman_cloud/check_wal_archive.inc.rst
.. include:: commands/barman_cloud/restore.inc.rst
.. include:: commands/barman_cloud/wal_archive.inc.rst
.. include:: commands/barman_cloud/wal_restore.inc.rst
