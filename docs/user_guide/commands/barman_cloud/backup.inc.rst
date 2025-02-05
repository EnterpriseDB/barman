.. _barman-cloud-barman-cloud-backup:

``barman-cloud-backup``
"""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-backup
                  [ { -V | --version } ]
                  [ --help ]
                  [ { { -v | --verbose } | { -q | --quiet } } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ { { -z | --gzip } | { -j | --bzip2 } | --snappy } ]
                  [ { -h | --host } HOST ]
                  [ { -p | --port } PORT ]
                  [ { -U | --user } USER ]
                  [ { -d | --dbname } DBNAME ]
                  [ { -n | --name } BACKUP_NAME ]
                  [ { -J | --jobs } JOBS ]
                  [ { -S | --max-archive-size } MAX_ARCHIVE_SIZE ]
                  [ --immediate-checkpoint ]
                  [ --min-chunk-size MIN_CHUNK_SIZE ]
                  [ --max-bandwidth MAX_BANDWIDTH ]
                  [ --snapshot-instance SNAPSHOT_INSTANCE ]
                  [ --snapshot-disk NAME ]
                  [ --snapshot-zone GCP_ZONE ]
                  [ -snapshot-gcp-project GCP_PROJECT ]
                  [ --tags TAG [ TAG ... ] ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ { -e | --encryption } { AES256 | aws:kms } ]
                  [ --sse-kms-key-id SSE_KMS_KEY_ID ]
                  [ --aws-region AWS_REGION ]
                  [ --aws-await-snapshots-timeout AWS_AWAIT_SNAPSHOTS_TIMEOUT ]
                  [ --aws-snapshot-lock-mode { compliance | governance } ]
                  [ --aws-snapshot-lock-duration DAYS ]
                  [ --aws-snapshot-lock-cool-off-period HOURS ]
                  [ --aws-snapshot-lock-expiration-date DATETIME ]
                  [ { --azure-credential | --credential } { azure-cli | managed-identity | default } ]
                  [ --encryption-scope ENCRYPTION_SCOPE ]
                  [ --azure-subscription-id AZURE_SUBSCRIPTION_ID ]
                  [ --azure-resource-group AZURE_RESOURCE_GROUP ]
                  [ --gcp-project GCP_PROJECT ]
                  [ --kms-key-name KMS_KEY_NAME ]
                  [ --gcp-zone GCP_ZONE ]
                  DESTINATION_URL SERVER_NAME

**Description**

The ``barman-cloud-backup`` script is used to create a local backup of a Postgres
server and transfer it to a supported cloud provider, bypassing the Barman server. It
can also be utilized as a hook script for copying Barman backups from the Barman server 
to one of the supported clouds (post_backup_retry_script).

This script requires read access to PGDATA and tablespaces, typically run as the
postgres user. When used on a Barman server, it requires read access to the directory
where Barman backups are stored. If ``--snapshot-`` arguments are used and snapshots are
supported by the selected cloud provider, the backup will be performed using snapshots
of the specified disks (``--snapshot-disk``). The backup label and metadata will also be
uploaded to the cloud.

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
  ``s3://bucket/path/to/folder``.

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
  
  Allowed options:

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

``--immediate-checkpoint``
  Forces the initial checkpoint to be done as quickly as possible.

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
  Tags to be added to all uploaded files in cloud storage, and/or to snapshots created, if
  snapshots are used.

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

``--aws-await-snapshots-timeout``
  The length of time in seconds to wait for snapshots to be created in AWS before timing
  out (default: 3600 seconds).

``--aws-snapshot-lock-mode``
  The lock mode for the snapshot. This is only valid if ``--snapshot-instance`` and
  ``--snapshot-disk`` are set.
  
  Allowed options:

  * ``compliance``.
  * ``governance``.

``--aws-snapshot-lock-duration``
  The lock duration is the period of time (in days) for which the snapshot is to remain
  locked, ranging from 1 to 36,500. Set either the lock duration or the expiration date
  (not both).

``--aws-snapshot-lock-cool-off-period``
  The cooling-off period is an optional period of time (in hours) that you can specify
  when you lock a snapshot in ``compliance`` mode, ranging from 1 to 72.

``--aws-snapshot-lock-expiration-date``
  The lock duration is determined by an expiration date in the future. It must be at
  least 1 day after the snapshot creation date and time, using the format
  ``YYYY-MM-DDTHH:MM:SS.sssZ``. Set either the lock duration or the expiration date
  (not both).

**Extra options for the Azure cloud provider**

``--azure-credential / --credential``
  Optionally specify the type of credential to use when authenticating with Azure. If
  omitted then Azure Blob Storage credentials will be obtained from the environment and
  the default Azure authentication flow will be used for authenticating with all other
  Azure services. If no credentials can be found in the environment then the default
  Azure authentication flow will also be used for Azure Blob Storage. 
  
  Allowed options:

  * ``azure-cli``.
  * ``managed-identity``.
  * ``default``.

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
