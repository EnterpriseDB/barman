.. _barman-cloud-barman-cloud-backup-keep:

``barman-cloud-backup-keep``
""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-backup-keep
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
                  [ { -r | --release } ]
                  [ { -s | --status } ]
                  [ --target { full | standalone } ]
                  SOURCE_URL SERVER_NAME BACKUP_ID

**Description**

Use this script to designate backups in cloud storage as archival backups, ensuring
their indefinite retention regardless of retention policies. 

This script allows you to mark backups previously created with ``barman-cloud-backup``
as archival backups. Once flagged as archival, these backups are preserved indefinitely
and are not subject to standard retention policies.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.

**Parameters**

``SERVER_NAME``
  Name of the server that holds the backup to be kept.

``SOURCE_URL``
  URL of the cloud source, such as a bucket in AWS S3. For example:
  ``s3://bucket/path/to/folder``.

``BACKUP_ID``
  The ID of the backup to be kept.

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

``-r`` / ``--release``
  If specified, the command will remove the keep annotation and the backup will be
  eligible for deletion.

``-s`` / ``--status``
  Print the keep status of the backup.

``--target``
  Specify the recovery target for this backup. Allowed options are:

  * ``full``
  * ``standalone``

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
