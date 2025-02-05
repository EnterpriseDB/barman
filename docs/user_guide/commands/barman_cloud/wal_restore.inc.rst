.. _barman-cloud-barman-cloud-wal-restore:

``barman-cloud-wal-restore``
""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-wal-restore
                  [ { -V | --version } ]
                  [ --help ]
                  [ { { -v | --verbose } | { -q | --quiet } } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ { --azure-credential | --credential } { azure-cli | managed-identity
                    | default } ]
                  [ --no-partial ]
                  SOURCE_URL SERVER_NAME WAL_NAME WAL_DEST

**Description**

The ``barman-cloud-wal-restore`` script functions as the ``restore_command`` for
retrieving WAL files from cloud storage and placing them directly into a Postgres
standby server, bypassing the Barman server.

This script is used to download WAL files that were previously archived with the
``barman-cloud-wal-archive`` command. Disable automatic download of ``.partial`` files by
calling ``--no-partial`` option.

.. important::
  On the target Postgres node, when ``pg_wal`` and the spool directory are on the 
  same filesystem, files are moved via renaming, which is faster than copying and 
  deleting. This speeds up serving WAL files significantly. If the directories are on 
  different filesystems, the process still involves copying and deleting, so there's 
  no performance gain in that case.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.

**Parameters**

``SERVER_NAME``
  Name of the server that will have WALs restored.

``SOURCE_URL``
  URL of the cloud source, such as a bucket in AWS S3. For example: ``s3://bucket/path/to/folder``.

``WAL_NAME``
  The value of the '%f' keyword (according to ``restore_command``).

``WAL_DEST``
  The value of the '%p' keyword (according to ``restore_command``).

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

``--no-partial``
  Do not download partial WAL files

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
