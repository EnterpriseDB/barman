.. _commands-barman-cli-barman-wal-archive:

``barman-wal-archive``
""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    barman-wal-archive
        [ { -h | --help } ]
        [ { -V | --version } ]
        [ { -U | --user } USER ]
        [ --port PORT ]
        [ { { -z | --gzip } | { -j | --bzip2 } | --xz | --snappy | --zstd | --lz4 } ]
        [ --compression-level COMPRESSION_LEVEL ]
        [ { -c | --config } CONFIG ]
        [ { -t | --test } ]
        [ --md5 ]
        BARMAN_HOST SERVER_NAME WAL_PATH
    
Description
^^^^^^^^^^^

This script can be utilized in the ``archive_command`` of a Postgres server to
transfer WAL files to a Barman host using the ``put-wal`` command (introduced in Barman
2.6). It establishes an SSH connection to the Barman host, enabling seamless integration
of Barman within Postgres clusters for improved business continuity.

**Exit Statuses** are:

* ``0`` for ``SUCCESS``.
* ``non-zero`` for ``FAILURE``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    The server name configured in Barman for the Postgres server from which 
    the WAL file is retrieved.

``BARMAN_HOST``
    The host of the Barman server.

``WAL_PATH``
    The value of the '%p' keyword (according to ``archive_command``).

``-h`` / ``--help``
    Display a help message and exit.

``-V`` / ``--version``
    Display the program's version number and exit.

``-U`` / ``--user``
    Specify the user for the SSH connection to the Barman server (defaults to
    ``barman``).

``--port``
    Define the port used for the SSH connection to the Barman server.

``-z`` / ``--gzip``
  gzip-compress the WAL file before sending it to the Barman server.

``-j`` / ``--bzip2``
  bzip2-compress the WAL file before sending it to the Barman server.

``--xz``
  xz-compress the WAL file before sending it to the Barman server.

``--snappy``
  snappy-compress the WAL file before sending it to the Barman server (requires the
  ``python-snappy`` Python library to be installed).

``--zstd``
  zstd-compress the WAL file before sending it to the Barman server (requires the
  ``zstandard`` Python library to be installed).

``--lz4``
  lz4-compress the WAL file before sending it to the Barman server (requires the
  ``lz4`` Python library to be installed).

``--compression-level``
  A compression level to be used by the selected compression algorithm. Valid
  values are integers within the supported range of the chosen algorithm or one
  of the predefined labels: ``low``, ``medium``, and ``high``. The range of each
  algorithm as well as what level each predefined label maps to can be found in
  :ref:`compression_level <configuration-options-compression-level>`.

``-c`` /  ``--config``
    Specify the configuration file on the Barman server.

``-t`` / ``--test``
    Test the connection and configuration of the specified Postgres server in Barman to
    ensure it is ready to receive WAL files. This option ignores the mandatory argument
    ``WAL_PATH``.

``--md5``
    Use MD5 instead of SHA256 as the hash algorithm to calculate the checksum of the WAL
    file when transmitting it to the Barman server. This is used to maintain
    compatibility with older server versions, as older versions of Barman server used to
    support only MD5.

.. note::
  When compression is enabled in ``barman-wal-archive``, it takes precedence over the
  compression settings configured on the Barman server, if they differ.

.. important::
  When compression is enabled in ``barman-wal-archive``, it is performed on the client
  side, before the file is sent to Barman. Be mindful of the database server's load and
  the chosen compression algorithm and level, as higher compression can delay WAL
  shipping, causing WAL files to accumulate.
