.. _barman-cloud-barman-cloud-backup-keep:

``barman-cloud-backup-keep``
""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-backup-keep
                  [ { -V | --version } ]
                  [ --help ]
                  [ { { -v | --verbose } | { -q | --quiet } } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ --addressing-style { auto | virtual | path } ]
                  [ --sse-customer-key SSE_CUSTOMER_KEY ]
                  [ { --azure-credential | --credential } { azure-cli | managed-identity | default } ]
                  [ { { -r | --release } | { -s | --status } | --target { full | standalone } } ]
                  SOURCE_URL SERVER_NAME BACKUP_ID

**Description**

Use this script to designate backups in cloud storage as archival backups, ensuring
their indefinite retention regardless of retention policies. 

This script allows you to mark backups previously created with ``barman-cloud-backup``
as archival backups. Once flagged as archival, these backups are preserved indefinitely
and are not subject to standard retention policies.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.
  To use an alternative GCP universe (e.g. S3NS/T-Systems), set the
  ``GOOGLE_CLOUD_UNIVERSE_DOMAIN`` environment variable to the desired universe domain.

.. important::
  Starting with AWS boto3 1.36, the behavior of **Data Integrity Protection checks**
  has changed. Some methods used by Barman no longer require the ``Content-MD5``
  header.

  This means that **S3-compatible storage providers that have not updated their
  server-side code may fail** when used with newer boto3 versions. For example, MinIO
  addressed this change shortly after the boto3 1.36 announcement.

  If you are using MinIO, you **must upgrade** to the latest release (or at least
  ``RELEASE.2025-02-03T21-03-04Z`` or newer) to ensure compatibility and avoid
  failures when releasing a keep annotation.

**Parameters**

``SERVER_NAME``
  Name of the server that holds the backup to be kept.

``SOURCE_URL``
  URL of the cloud source, such as a bucket in AWS S3. For example:
  ``s3://bucket/path/to/folder``.

``BACKUP_ID``
  The ID of the backup to be kept. You can use a shortcut instead of the backup ID.

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

``--addressing-style``
  The S3 addressing style to use when connecting to S3-compatible cloud storage. This
  option is useful when using custom S3 endpoints (such as MinIO) that may require a
  specific addressing style.
  
  Allowed options:

  * ``auto`` (default): Uses the addressing style determined by the boto3 library.
  * ``virtual``: Uses virtual-hosted-style addressing (bucket-name.s3.amazonaws.com).
  * ``path``: Uses path-style addressing (s3.amazonaws.com/bucket-name).

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
