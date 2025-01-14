.. _barman-cloud-barman-cloud-backup-delete:

``barman-cloud-backup-delete``
""""""""""""""""""""""""""""""

**Synopsis**

.. code-block:: text
    
  barman-cloud-backup-delete
                  [ { -V | --version } ]
                  [ --help ]
                  [ { { -v | --verbose } | { -q | --quiet } } ]
                  [ { -t | --test } ]
                  [ --cloud-provider { aws-s3 | azure-blob-storage | google-cloud-storage } ]
                  [ --endpoint-url ENDPOINT_URL ]
                  [ { -r | --retention-policy } RETENTION_POLICY ]
                  [ { -m | --minimum-redundancy } MINIMUM_REDUNDANCY ]
                  [ { -b | --backup-id } BACKUP_ID]
                  [ --dry-run ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ { --azure-credential | --credential } { azure-cli | managed-identity } ]
                  [--batch-size DELETE_BATCH_SIZE]
                  SOURCE_URL SERVER_NAME

**Description**

The ``barman-cloud-backup-delete`` script is used to delete one or more backups created
with the ``barman-cloud-backup`` command from cloud storage and to remove the associated
WAL files.

Backups can be specified for deletion either by their backup ID
(as obtained from ``barman-cloud-backup-list``) or by a retention policy. Retention
policies mirror those used by the Barman server, deleting all backups that are not required to
meet the specified policy. When a backup is deleted, any unused WAL files associated with
that backup are also removed. 

WALs are considered unused if:

* The WALs predate the begin_wal value of the oldest remaining backup.
* The WALs are not required by any archival backups stored in the cloud.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.

.. important::
  Each backup deletion involves three separate requests to the cloud provider: one for
  the backup files, one for the ``backup.info`` file, and one for the associated WALs.
  Deleting by retention policy may result in a high volume of delete requests if a
  large number of backups are accumulated in cloud storage.

**Parameters**

``SERVER_NAME``
  Name of the server that holds the backup to be deleted.

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

``-b`` / ``--backup-id``
  ID of the backup to be deleted

``-m`` / ``--minimum-redundancy``
  The minimum number of backups that should always be available.

``-r`` / ``--retention-policy``
  If specified, delete all backups eligible for deletion according to the supplied
  retention policy. 
  
  Syntax: ``REDUNDANCY value | RECOVERY WINDOW OF value { DAYS | WEEKS | MONTHS }``

``--batch-size``
  The maximum number of objects to be deleted in a single request to the cloud provider.
  If unset then the maximum allowed batch size for the specified cloud provider will be
  used (``1000`` for aws-s3, ``256`` for azure-blob-storage and ``100`` for
  google-cloud-storage).

``--dry-run``
  Find the objects which need to be deleted but do not delete them.

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
