.. _barman-cloud-barman-cloud-restore:

``barman-cloud-restore``
""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-restore
                  [ { -V | --version } ]
                  [ --help ]
                  [ { -v | --verbose } ]
                  [ { -q | --quiet } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ --aws-irsa ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ --azure-credential { azure-cli | managed-identity } ]
                  [ --snapshot-recovery-instance SNAPSHOT_RECOVERY_INSTANCE ]
                  [ --aws-region AWS_REGION ]
                  [ --gcp-zone GCP_ZONE ]
                  [ --azure-resource-group AZURE_RESOURCE_GROUP ]
                  [ --tablespace NAME:LOCATION ]
                  SOURCE_URL SERVER_NAME BACKUP_ID RECOVERY_DESTINATION

**Description**

Use this script to restore a backup directly from cloud storage that was created with
the ``barman-cloud-backup`` command. Additionally, this script can prepare for recovery
from a snapshot backup by verifying that attached disks were cloned from the correct
snapshots and by downloading the backup label from object storage.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.

**Parameters**

``SERVER_NAME``
  Name of the server that holds the backup to be restored.

``SOURCE_URL``
  URL of the cloud source, such as a bucket in AWS S3. For example:
  ``s3://bucket/path/to/folder``.

``BACKUP_ID``
  The ID of the backup to be restored.

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

**Extra options for the AWS cloud provider**

``--endpoint-url``
  Override default S3 endpoint URL with the given one.

``--aws-irsa``
  Uses IAM Role Service Account in AWS instead of Profile (running from an eks pod).

``-P`` / ``--aws-profile``
  Profile name (e.g. ``INI`` section in AWS credentials file).

``--profile`` (deprecated)
  Profile name (e.g. ``INI`` section in AWS credentials file) - replaced by
  ``--aws-profile``.

``--read-timeout``
  The time in seconds until a timeout is raised when waiting to read from a connection
  (defaults to ``60`` seconds).

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

``--azure-resource-group``
  The name of the Azure resource group to which the compute instance and disks defined by
  the ``--snapshot-instance`` and ``--snapshot-disk`` arguments belong.

**Extra options for GCP cloud provider**

``--gcp-zone``
  Zone of the disks from which snapshots should be taken.

``--snapshot-recovery-zone`` (deprecated)
  Zone containing the instance and disks for the snapshot recovery - replaced by
  ``--gcp-zone``.
