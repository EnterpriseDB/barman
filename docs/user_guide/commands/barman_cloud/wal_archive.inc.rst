.. _barman-cloud-barman-cloud-wal-archive:

``barman-cloud-wal-archive``
""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-wal-archive
                  [ { -V | --version } ]
                  [ --help ]
                  [ { { -v | --verbose } | { -q | --quiet } } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ { { -z | --gzip } | { -j | --bzip2 } | --xz | --snappy | --zstd | --lz4 } ]
                  [ --compression-level COMPRESSION_LEVEL ]
                  [ --tags TAG [ TAG ... ] ]
                  [ --history-tags HISTORY_TAG [ HISTORY_TAG ... ] ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ { -e | --encryption } ENCRYPTION ]
                  [ --sse-kms-key-id SSE_KMS_KEY_ID ]
                  [ { --azure-credential | --credential } { azure-cli | managed-identity |
                    default } ]
                  [ --encryption-scope ENCRYPTION_SCOPE ]
                  [ --max-block-size MAX_BLOCK_SIZE ]
                  [ --max-concurrency MAX_CONCURRENCY ]
                  [ --max-single-put-size MAX_SINGLE_PUT_SIZE ]
                  [ --kms-key-name KMS_KEY_NAME ]
                  DESTINATION_URL SERVER_NAME [ WAL_PATH ]

**Description**

The ``barman-cloud-wal-archive`` command is designed to be used in the
``archive_command`` of a Postgres server to directly ship WAL files to cloud storage.

.. note::
  If you are using Python 2 or unsupported versions of Python 3, avoid using the
  compression options ``--gzip`` or ``--bzip2``. The script cannot restore
  gzip-compressed WALs on Python < 3.2 or bzip2-compressed WALs on Python < 3.3.

This script enables the direct transfer of WAL files to cloud storage, bypassing the
Barman server. Additionally, it can be utilized as a hook script for WAL archiving
(pre_archive_retry_script).

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.

**Parameters**

``SERVER_NAME``
  Name of the server that will have the WALs archived.

``DESTINATION_URL``
  URL of the cloud destination, such as a bucket in AWS S3. For example: ``s3://bucket/path/to/folder``.

``WAL_PATH``
  The value of the '%p' keyword (according to ``archive_command``).

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
  gzip-compress the WAL while uploading to the cloud.

``-j`` / ``--bzip2``
  bzip2-compress the WAL while uploading to the cloud.

``--xz``
  xz-compress the WAL while uploading to the cloud.

``--snappy``
  snappy-compress the WAL while uploading to the cloud (requires the ``python-snappy``
  Python library to be installed).

``--zstd``
  zstd-compress the WAL while uploading to the cloud (requires the ``zstandard`` Python
  library to be installed).

``--lz4``
  lz4-compress the WAL while uploading to the cloud (requires the ``lz4`` Python
  library to be installed).

``--compression-level``
  A compression level to be used by the selected compression algorithm. Valid
  values are integers within the supported range of the chosen algorithm or one
  of the predefined labels: ``low``, ``medium``, and ``high``. The range of each
  algorithm as well as what level each predefined label maps to can be found in
  :ref:`compression_level <configuration-options-compression-level>`.

``--tags``
  Tags to be added to archived WAL files in cloud storage.

``--history-tags``
  Tags to be added to archived history files in cloud storage.

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

``--encryption-scope``
  The name of an encryption scope defined in the Azure Blob Storage service which is to
  be used to encrypt the data in Azure.

``--max-block-size``
  The chunk size to be used when uploading an object via the concurrent chunk method
  (default: ``4MB``).

``--max-concurrency``
  The maximum number of chunks to be uploaded concurrently (default: ``1``).

``--max-single-put-size``
  Maximum size for which the Azure client will upload an object in a single request
  (default: ``64MB``). If this is set lower than the Postgres WAL segment size after
  any applied compression then the concurrent chunk upload method for WAL archiving will
  be used.

**Extra options for GCP cloud provider**

``--kms-key-name``
  The name of the GCP KMS key which should be used for encrypting the uploaded data in
  GCS.
