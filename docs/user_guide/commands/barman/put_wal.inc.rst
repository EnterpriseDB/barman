.. _commands-barman-put-wal:

``barman put-wal``
""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    put-wal
        [ { -h | --help } ]
        [ { -t | --test } ]
        SERVER_NAME

Description
^^^^^^^^^^^

Receive a WAL file from a remote server and securely save it into the server incoming
directory. The WAL file should be provided via ``STDIN``, encapsulated in a tar stream
along with a ``SHA256SUMS`` or ``MD5SUMS`` file for validation (``sha256`` is the default
hash algorithm, but the user can choose ``md5`` when setting the ``archive-command`` via
``barman-wal-archive``). This command is intended to be executed via SSH from a remote
``barman-wal-archive`` utility (included in the barman-cli package). Avoid using this
command directly unless you fully manage the content of the files.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

``-t`` / ``--test``
    Test both the connection and configuration of the specified Postgres
    server in Barman for WAL retrieval.
