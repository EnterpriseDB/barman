.. _commands-barman-archive-wal:

``barman archive-wal``
""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    archive-wal
        [ { -h | --help } ]
        SERVER_NAME
    
Description
^^^^^^^^^^^

Fetch WAL files received from either the standard ``archive_command`` or streaming
replication with ``pg_receivewal`` and store them in the server's WAL archive. If you
have enabled ``compression`` in the configuration file, the WAL files will be compressed
before they are archived.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.
