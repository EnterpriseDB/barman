.. _barman-cloud-barman-cloud-backup-show:

``barman-cloud-backup-show``
""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-backup-show
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
                  [ --format FORMAT ]
                  SOURCE_URL SERVER_NAME BACKUP_ID

**Description**

This script displays detailed information about a specific backup created with the
``barman-cloud-backup`` command. The output is similar to the ``barman show-backup``
from the :ref:`barman show-backup <commands-barman-show-backup>` command reference, 
but it has fewer information.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.
  To use an alternative GCP universe (e.g. S3NS/T-Systems), set the
  ``GOOGLE_CLOUD_UNIVERSE_DOMAIN`` environment variable to the desired universe domain.

**Parameters**

``BACKUP_ID``
  The ID of the backup. You can use a shortcut instead of the backup ID.

``SERVER_NAME``
  Name of the server that holds the backup to be displayed.

``SOURCE_URL``
  URL of the cloud source, such as a bucket in AWS S3. For example:
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
  
  Allowed options are:

  * ``aws-s3``.
  * ``azure-blob-storage``.
  * ``google-cloud-storage``.

``--format``
  Output format (``console`` or ``json``). Default ``console``.

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
