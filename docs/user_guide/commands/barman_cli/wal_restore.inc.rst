.. _commands-barman-cli-barman-wal-restore:

``barman-wal-restore``
""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    barman-wal-restore
        [ { -h | --help } ]
        [ { -V | --version } ]
        [ { -U | --user } USER ]
        [ --port PORT ]
        [ { -s | --sleep } SECONDS ]
        [ { -p | --parallel } JOBS ]
        [ --spool-dir SPOOL_DIR ]
        [ { -P | --partial } ]
        [ { { -z | --gzip } | { -j | --bzip2 } | --keep-compression } ]
        [ --remote-barman-path DIR ]
        [ { -c | --config } CONFIG ]
        [ { -t | --test } ]
        BARMAN_HOST SERVER_NAME WAL_NAME WAL_DEST
    
Description
^^^^^^^^^^^

This script serves as a ``restore_command`` for Postgres servers, enabling the
retrieval of WAL files through Barman's ``get-wal`` feature. It establishes an SSH
connection to the Barman host and facilitates the integration of Barman within
Postgres clusters, enhancing business continuity.

**Exit Statuses** are:

* ``0`` for ``SUCCESS``.
* ``1`` for remote get-wal command ``FAILURE``, likely because the requested WAL could
  not be found.
* ``2`` for ssh connection ``FAILURE``.
* Any other ``non-zero`` for ``FAILURE``.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    The server name configured in Barman for the Postgres server from which the 
    WAL file is retrieved.

``BARMAN_HOST``
    The host of the Barman server.

``WAL_NAME``
    The value of the '%f' keyword (according to ``restore_command``).

``WAL_DEST``
    The value of the '%p' keyword (according to ``restore_command``).

``-h`` / ``--help``
    Display a help message and exit.

``-V`` / ``--version``
    Display the program's version number and exit.

``-U`` / ``--user``
    Specify the user for the SSH connection to the Barman server (defaults to
    ``barman``).

``--port``
    Define the port used for the SSH connection to the Barman server.

``-s`` / ``--sleep``
    Pause for ``SECONDS`` after a failed ``get-wal`` request (defaults to ``0`` - no
    wait).

``-p`` / ``--parallel``
  Specifies the number of WAL files to fetch in parallel. Default is ``0`` (disabled).
  When set to a value greater than ``1``, fetches the requested WAL file along with the
  next ``N - 1`` files simultaneously. The additional files are staged in a local spool
  directory (see ``--spool-dir``) so that subsequent restore requests can be served
  immediately from local storage.

``--spool-dir``
  Directory used for staging extra WALs fetched when using ``--parallel``. Default is
  ``/var/tmp/walrestore``.

``-P`` /  ``--partial``
    Include partial WAL files (.partial) in the retrieval.

``-z`` /  ``--gzip``
    Transfer WAL files compressed with ``gzip``.

``-j`` /  ``--bzip2``
    Transfer WAL files compressed with ``bzip2``.

``--keep-compression``
    If specified, compressed files will be trasnfered as-is and decompressed on arrival
    on the client-side.

``--remote-barman-path``
    Directory containing the ``barman`` executable on the Barman server. Use it when
    ``barman`` is not in ``PATH``.

    .. note::
        The value is passed to the remote shell quoted, so it must be an absolute path.
        Shell expansions such as ``~`` or ``$HOME`` are not performed on the Barman server.

``-c`` /  ``--config``
    Specify the configuration file on the Barman server.

``-t`` / ``--test``
    Test the connection and configuration of the specified Postgres server in Barman to
    ensure it is ready to receive WAL files. This option ignores the mandatory arguments
    ``WAL_NAME`` and ``WAL_DEST``.


.. warning::

    ``-z`` / ``--gzip`` and ``-j`` /  ``--bzip2`` options are deprecated and will be
    removed in the future. For WAL compression, please make sure to enable it directly
    on the Barman server via the ``compression`` configuration option.
