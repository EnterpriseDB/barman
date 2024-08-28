.. _barman_cloud_backup:

``barman-cloud-backup``
"""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-backup
                  [ { -V | --version } ]
                  [ --help ]
                  [ { -v | --verbose } ]
                  [ { -q | --quiet } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ { -z | --gzip } ]
                  [ { -j | --bzip2 } ]
                  [ --snappy ]
                  [ { -h | --host } HOST ]
                  [ { -p | --port } PORT ]
                  [ { -U | --user } USER ]
                  [ { -d | --dbname } DBNAME ]
                  [ { -n | --name } BACKUP_NAME ]
                  [ { -J | --jobs } JOBS ]
                  [ -S MAX_ARCHIVE_SIZE ]
                  [ --immediate-checkpoint ]
                  [ --min-chunk-size MIN_CHUNK_SIZE ]
                  [ --max-bandwidth MAX_BANDWIDTH ]
                  [ --snapshot-instance SNAPSHOT_INSTANCE ]
                  [ --snapshot-disk NAME ]
                  [ --tags [ TAGS ... ] ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ { -e | --encryption } ENCRYPTION ]
                  [ --sse-kms-key-id SSE_KMS_KEY_ID ]
                  [ --aws-region AWS_REGION ]
                  [ --azure-credential { azure-cli | managed-identity } ]
                  [ --encryption-scope ENCRYPTION_SCOPE ]
                  [ --azure-subscription-id AZURE_SUBSCRIPTION_ID ]
                  [ --azure-resource-group AZURE_RESOURCE_GROUP ]
                  [ --gcp-project GCP_PROJECT ]
                  [ --kms-key-name KMS_KEY_NAME ]
                  [ --gcp-zone GCP_ZONE ]
                  DESTINATION_URL SERVER_NAME

**Description**

The ``barman-cloud-backup`` script is used to create a local backup of a PostgreSQL
server and transfer it to a supported cloud provider, bypassing the Barman server. It
can also be utilized as a hook script for copying Barman backups from the Barman server 
to one of the supported clouds (post_backup_retry_script).

This script requires read access to PGDATA and tablespaces, typically run as the
postgres user. When used on a Barman server, it requires read access to the directory
where Barman backups are stored. If ``--snapshot-`` arguments are used and snapshots are
supported by the selected cloud provider, the backup will be performed using snapshots
of the specified disks (``--snapshot-disk``). The backup label and metadata will also be
uploaded to the cloud.
  
This script requires read access to the directory where Barman backups are stored. If
``--snapshot-`` arguments are used and snapshots are supported by the selected cloud
provider, the backup will be performed using snapshots of the specified disks
(``--snapshot-disk``). The backup label and metadata will also be uploaded to the cloud.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.

.. important::
  The cloud upload may fail if any file larger than the configured ``--max-archive-size``
  is present in the data directory or tablespaces. However, Postgres files up to
  ``1GB`` are always allowed, regardless of the ``--max-archive-size`` setting.

**Parameters**

``SERVER_NAME``
  Name of the server to be backed up.

``DESTINATION_URL``
  URL of the cloud destination, such as a bucket in AWS S3. For example:
  `s3://bucket/path/to/folder`.

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

``-z`` / ``--gzip``
  gzip-compress the backup while uploading to the cloud (should not be used with python <
  3.2).

``-j`` / ``--bzip2``
  bzip2-compress the backup while uploading to the cloud (should not be used with python <
  3.3).

``--snappy``
  snappy-compress the backup while uploading to the cloud (requires optional
  ``python-snappy`` library).

``-h`` / ``--host``
  Host or Unix socket for Postgres connection (default: libpq settings).

``-p`` / ``--port``
  Port for Postgres connection (default: libpq settings).

``-U`` / ``--user``
  User name for Postgres connection (default: libpq settings).

``-d`` / ``--dbname``
  Database name or conninfo string for Postgres connection (default: "postgres").

``-n`` / ``--name``
  A name which can be used to reference this backup in commands such as
  ``barman-cloud-restore`` and ``barman-cloud-backup-delete``.

``-J`` / ``--jobs``
  Number of subprocesses to upload data to cloud storage (default: ``2``).

``-S`` / ``--max-archive-size``
  Maximum size of an archive when uploading to cloud storage (default: ``100GB``).

``--min-chunk-size``
  Minimum size of an individual chunk when uploading to cloud storage (default: ``5MB``
  for ``aws-s3``, ``64KB`` for ``azure-blob-storage``, not applicable for
  ``google-cloud-storage``).

``--max-bandwidth``
  The maximum amount of data to be uploaded per second when backing up to object
  storages (default: ``0`` - no limit).

``--snapshot-instance``
  Instance where the disks to be backed up as snapshots are attached.

``--snapshot-disk``
  Name of a disk from which snapshots should be taken.

``--tags``
  Tags to be added to archived WAL files in cloud storage.

**Extra options for the AWS cloud provider**

``--endpoint-url``
  Override default S3 endpoint URL with the given one.

``-P`` / ``--aws-profile``
  Profile name (e.g. ``INI`` section in AWS credentials file).

``--profile`` (deprecated)
  Profile name (e.g. ``INI`` section in AWS credentials file) - replaced by
  ``--aws-profile``.

``--read-timeout``
  The time in seconds until a timeout is raised when waiting to read from a connection
  (defaults to ``60`` seconds).

``-e`` / ``--encryption``
  The encryption algorithm used when storing the uploaded data in S3.
  
  Allowed options:

  * ``AES256``.
  * ``aws:kms``.

``--sse-kms-key-id``
  The AWS KMS key ID that should be used for encrypting the uploaded data in S3. Can be
  specified using the key ID on its own or using the full ARN for the key. Only allowed if
  ``-e`` / ``--encryption`` is set to ``aws:kms``.

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

``--encryption-scope``
  The name of an encryption scope defined in the Azure Blob Storage service which is to
  be used to encrypt the data in Azure.

``--azure-subscription-id``
  The ID of the Azure subscription which owns the instance and storage volumes defined by
  the ``--snapshot-instance`` and ``--snapshot-disk`` arguments.
  
``--azure-resource-group``
  The name of the Azure resource group to which the compute instance and disks defined by
  the ``--snapshot-instance`` and ``--snapshot-disk`` arguments belong.

**Extra options for GCP cloud provider**

``--gcp-project``
  GCP project under which disk snapshots should be stored.

``--snapshot-gcp-project`` (deprecated)
  GCP project under which disk snapshots should be stored - replaced by
  ``--gcp-project``.

``--kms-key-name``
  The name of the GCP KMS key which should be used for encrypting the uploaded data in
  GCS.

``--gcp-zone``
  Zone of the disks from which snapshots should be taken.

``--snapshot-zone`` (deprecated)
  Zone of the disks from which snapshots should be taken - replaced by ``--gcp-zone``.
