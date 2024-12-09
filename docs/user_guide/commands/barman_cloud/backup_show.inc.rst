.. _barman-cloud-barman-cloud-backup-show:

``barman-cloud-backup-show``
""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-backup-show
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
                  [ --format ]
                  SOURCE_URL SERVER_NAME

**Description**

This script displays detailed information about a specific backup created with the
``barman-cloud-backup`` command. The output is similar to the ``barman show-backup``
from the :ref:`barman show-backup <commands-barman-show-backup>` command reference, 
but it has fewer information.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.

**Parameters**

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
