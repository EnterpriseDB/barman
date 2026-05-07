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
                  [ --addressing-style { auto | virtual | path } ]
                  [ { -P | --aws-profile } AWS_PROFILE ]
                  [ --profile AWS_PROFILE ]
                  [ --read-timeout READ_TIMEOUT ]
                  [ --sse-customer-key SSE_CUSTOMER_KEY ]
                  [ { --azure-credential | --credential } { azure-cli | managed-identity
                    | default } ]
                  [ --no-partial ]
                  [ { -p, --parallel } PARALLEL ]
                  [ --spool-dir SPOOL_DIR ]
                  SOURCE_URL SERVER_NAME WAL_NAME WAL_DEST

**Description**

The ``barman-cloud-wal-restore`` script functions as the ``restore_command`` for
retrieving WAL files from cloud storage and placing them directly into a Postgres
standby server, bypassing the Barman server.

This script is used to download WAL files that were previously archived with the
``barman-cloud-wal-archive`` command. Disable automatic download of ``.partial`` files by
calling ``--no-partial`` option.

.. note::

  Partial WAL files (with a ``.partial`` suffix) can appear in the object store
  when a standby server is archiving WAL segments and is then promoted to primary.
  At the moment of promotion, the in-progress WAL segment has not been completed
  yet, so Postgres archives it as a ``.partial`` file before switching to a new
  timeline. These files represent an incomplete segment and are not suitable for
  crash recovery on their own.

  In most cases, downloading ``.partial`` files during WAL restore is harmless,
  because Postgres knows how to handle them correctly. However, when running
  ``barman-cloud-wal-restore`` on a cluster that has experienced a promotion, the
  ``restore_command`` may retrieve a ``.partial`` (after renaming it without
  the suffix) file where a complete WAL is expected.

  Use ``--no-partial`` to suppress the download of ``.partial`` files in this
  scenario, which will cause ``restore_command`` to signal to Postgres that the
  WAL is not available, prompting it to switch to the correct timeline via a
  timeline history file (``.history``) instead.

.. important::
  On the target Postgres node, when ``pg_wal`` and the spool directory are on the 
  same filesystem, files are moved via renaming, which is faster than copying and 
  deleting. This speeds up serving WAL files significantly. If the directories are on 
  different filesystems, the process still involves copying and deleting, so there's 
  no performance gain in that case.

.. note::
  For GCP, only authentication with ``GOOGLE_APPLICATION_CREDENTIALS`` env is supported.
  To use an alternative GCP universe (e.g. S3NS/T-Systems), set the
  ``GOOGLE_CLOUD_UNIVERSE_DOMAIN`` environment variable to the desired universe domain.

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

``-p`` / ``--parallel``
  Specifies the number of WAL files to fetch in parallel. Default is ``0`` (disabled).
  When set to a value greater than ``1``, fetches the requested WAL file along with the
  next ``N - 1`` files simultaneously. The additional files are staged in a local spool
  directory (see ``--spool-dir``) so that subsequent restore requests can be served
  immediately from local storage.

``--spool-dir``
  Directory used for staging extra WALs fetched when using ``--parallel``. Default is
  ``/var/tmp/walrestore``.

**Extra options for the AWS cloud provider**

``--endpoint-url``
  Override default S3 endpoint URL with the given one.

``--addressing-style``
  The addressing style to use for S3 requests. This is useful when connecting to
  S3-compatible services that require a specific addressing style.

  Allowed options are:

  * ``auto`` (default): Uses the addressing style determined by the underlying library.
  * ``virtual``: Uses virtual-hosted style addressing (e.g., bucket-name.s3.amazonaws.com).
  * ``path``: Uses path-style addressing (e.g., s3.amazonaws.com/bucket-name).

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
