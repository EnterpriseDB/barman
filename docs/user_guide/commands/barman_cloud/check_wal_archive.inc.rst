.. _barman-cloud-barman-cloud-check-wal-archive:

``barman-cloud-check-wal-archive``
""""""""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-check-wal-archive
                  [ { -V | --version } ]
                  [ --help ]
                  [ { { -v | --verbose } | { -q | --quiet } } ]
                  [ { -t | --test } ]
                  [ --assume-bucket-exists ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ --addressing-style { auto | virtual | path } ]
                  [ { --azure-credential | --credential } 
                    { azure-cli | managed-identity | default } ]
                  [ --timeline TIMELINE ]
                  DESTINATION_URL SERVER_NAME

**Description**

Verify that the WAL archive destination for a server is suitable for use with a new
Postgres cluster. By default, the check will succeed if the WAL archive is empty. If
any WAL files are present in the archive, the check will fail, unless the ``--timeline``
option is used and all existing WALs are from timelines earlier than the specified value.

.. note::
  By default, the ``barman-cloud-check-wal-archive`` command performs an initial
  ``HeadBucket`` call to verify whether the target bucket already exists in the S3
  storage. If the bucket does not exist, the command will attempt to automatically
  create it before performing the WAL archive check.

  This is the only Barman command that performs these operations (bucket existence check
  and automatic creation). These operations can be skipped using the
  ``--assume-bucket-exists`` flag.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.
  To use an alternative GCP universe (e.g. S3NS/T-Systems), set the
  ``GOOGLE_CLOUD_UNIVERSE_DOMAIN`` environment variable to the desired universe domain.

**Parameters**

``SERVER_NAME``
  Name of the server that needs to be checked.

``DESTINATION_URL``
  URL of the cloud destination, such as a bucket in AWS S3. For example: ``s3://bucket/path/to/folder``.

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

``--assume-bucket-exists``
  Assumes the bucket exists and can be accessed by the executing principal, skipping
  connectivity test and bucket creation.

``--cloud-provider``
  The cloud provider to use as a storage backend.
  
  Allowed options are:

  * ``aws-s3``.
  * ``azure-blob-storage``.
  * ``google-cloud-storage``.

``--timeline``
  The earliest timeline whose WALs should cause the check to fail.

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
