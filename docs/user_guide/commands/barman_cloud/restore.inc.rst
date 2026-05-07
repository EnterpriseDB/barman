.. _barman-cloud-barman-cloud-restore:

``barman-cloud-restore``
""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-restore
                  [ { -V | --version } ]
                  [ --help ]
                  [ { { -v | --verbose } | { -q | --quiet } } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ --addressing-style { auto | virtual | path } ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ --sse-customer-key SSE_CUSTOMER_KEY ]
                  [ { --azure-credential | --credential } { azure-cli | managed-identity | default } ]
                  [ --snapshot-recovery-instance SNAPSHOT_RECOVERY_INSTANCE ]
                  [ --snapshot-recovery-zone GCP_ZONE ]
                  [ --aws-region AWS_REGION ]
                  [ --gcp-zone GCP_ZONE ]
                  [ --azure-resource-group AZURE_RESOURCE_GROUP ]
                  [ --tablespace NAME:LOCATION [ --tablespace NAME:LOCATION ... ] ]
                  [ --target-lsn LSN ]
                  [ --target-time TIMESTAMP ]
                  [ --target-tli TLI ]
                  SOURCE_URL SERVER_NAME BACKUP_ID RECOVERY_DESTINATION

**Description**

Use this script to restore a backup directly from cloud storage that was created with
the ``barman-cloud-backup`` command. Additionally, this script can prepare for recovery
from a snapshot backup by verifying that attached disks were cloned from the correct
snapshots and by downloading the backup label from object storage.

This command does not automatically prepare Postgres for recovery. You must manually
manage any :term:`PITR` options, custom ``restore_command`` values, signal files, or
required WAL files to ensure Postgres starts, either manually or using external tools.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.
  To use an alternative GCP universe (e.g. S3NS/T-Systems), set the
  ``GOOGLE_CLOUD_UNIVERSE_DOMAIN`` environment variable to the desired universe domain.

**Parameters**

``SERVER_NAME``
  Name of the server that holds the backup to be restored.

``SOURCE_URL``
  URL of the cloud source, such as a bucket in AWS S3. For example:
  ``s3://bucket/path/to/folder``.

``BACKUP_ID``
  The ID of the backup to be restored. You can use a shortcut instead of the backup ID.
  Besides that, you can use ``auto`` to have Barman automatically find the most suitable
  backup for the restore operation.

``RECOVERY_DESTINATION``
  The path to a directory for recovery.

``-V`` / ``--version``
  Show version and exit.

``--help``
  show this help message and exit.

``-v`` / ``--verbose``
  Increase output verbosity (e.g., ``-vv`` is more than ``-v``).

``-q`` / ``--quiet``
  Decrease output verbosity (e.g., ``-qq`` is less than ``-q``).

``-t`` / ``--test``
  Test cloud connectivity and exit.

``--cloud-provider``
  The cloud provider to use as a storage backend.
  
  Allowed options are:

  * ``aws-s3``.
  * ``azure-blob-storage``.
  * ``google-cloud-storage``.

``--snapshot-recovery-instance``
  Instance where the disks recovered from the snapshots are attached.
  
``--tablespace``
  Tablespace relocation rule.
  
``--target-lsn``
  The recovery target lsn, e.g., ``3/64000000``.
  
``--target-time``
  The recovery target timestamp with or without timezone, in the format ``%Y-%m-%d %H:%M:%S``.
  
``--target-tli``
  The recovery target timeline.

**Extra options for the AWS cloud provider**

``--endpoint-url``
  Override default S3 endpoint URL with the given one.

``--addressing-style``
  The addressing style to use for S3 requests. This is useful when connecting to
  S3-compatible services that require a specific addressing style.

  Allowed options are:

  * ``auto`` (default): Uses the addressing style determined by the underlying library.
  * ``virtual``: Uses virtual-hosted style addressing (e.g., bucket-name.s3.amazonaws.com).
  * ``path``: Uses path-style addressing (e.g., s3.amazonaws.com/bucket-name).

``-P`` / ``--aws-profile``
  Profile name (e.g. ``INI`` section in AWS credentials file).

``--profile`` (deprecated)
  Profile name (e.g. ``INI`` section in AWS credentials file) - replaced by
  ``--aws-profile``.

``--read-timeout``
  The time in seconds until a timeout is raised when waiting to read from a connection
  (defaults to ``60`` seconds).

``--sse-customer-key``
  The customer-provided encryption key (SSE-C) to use for decrypting data in S3. The
  value must be a ``file://`` URI pointing to a file containing a base64-encoded 256-bit
  (32-byte) key (e.g. ``file:///etc/barman/sse-c.b64``).

  .. important::
    All backups and WAL files for a given server must be encrypted with the same SSE-C
    key. Mixing SSE-C-encrypted and unencrypted objects, or using different keys across
    backups, will cause barman commands to fail when S3 rejects requests with a
    mismatched or missing key.

``--aws-region``
  The name of the AWS region containing the EC2 VM and storage volumes defined by the
  ``--snapshot-instance`` and ``--snapshot-disk`` arguments.

**Extra options for the Azure cloud provider**

``--azure-credential / --credential``
  Optionally specify the type of credential to use when authenticating with Azure. If
  omitted then Azure Blob Storage credentials will be obtained from the environment and
  the default Azure authentication flow will be used for authenticating with all other
  Azure services. If no credentials can be found in the environment then the default
  Azure authentication flow will also be used for Azure Blob Storage. 
  
  Allowed options are:

  * ``azure-cli``.
  * ``managed-identity``.
  * ``default``.

``--azure-resource-group``
  The name of the Azure resource group to which the compute instance and disks defined by
  the ``--snapshot-instance`` and ``--snapshot-disk`` arguments belong.

**Extra options for GCP cloud provider**

``--gcp-zone``
  Zone of the disks from which snapshots should be taken.

``--snapshot-recovery-zone`` (deprecated)
  Zone containing the instance and disks for the snapshot recovery - replaced by
  ``--gcp-zone``.
