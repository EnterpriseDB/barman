.. _commands-barman-cli-barman-wal-archive:

``barman-wal-archive``
""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    barman-wal-archive
        [ { -h, --help } ]
        [ { -V, --version } ]
        [ { -U, --user } USER ]
        [ --port PORT ]
        [ { -c, --config } CONFIG ]
        [ { -t --test } ]
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

``-c`` /  ``--config``
    Specify the configuration file on the Barman server.

``-t`` / ``--test``
    Test the connection and configuration of the specified Postgres server in Barman to
    ensure it is ready to receive WAL files. This option ignores the mandatory arguments
    ``WAL_NAME`` and ``WAL_DEST``.

``--md5``
    Use MD5 instead of SHA256 as the hash algorithm to calculate the checksum of the WAL
    file when transmitting it to the Barman server. This is used to maintain
    compatibility with older server versions, as older versions of Barman server used to
    support only MD5.
