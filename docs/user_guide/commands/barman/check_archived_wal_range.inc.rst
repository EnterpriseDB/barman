.. _commands-barman-check-archived-wal-range:

``barman check-archived-wal-range``
""""""""""""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text

    check-archived-wal-range
        [ { -h | --help } ]
        SERVER_NAME BEGIN_WAL END_WAL

Description
^^^^^^^^^^^

Check that all WAL segments in the range from ``BEGIN_WAL`` to ``END_WAL`` (inclusive)
are present in the WAL archive of the specified server. If any segments are missing,
they are listed and the command exits with a non-zero status. Exits with status 0 when
the sequence is complete.

This command scans the WAL archive directory directly, so it reflects the actual
state of the storage rather than Barman's internal index.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``BEGIN_WAL``
    Name of the first WAL segment in the range to verify (inclusive).

``END_WAL``
    Name of the last WAL segment in the range to verify (inclusive).

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.
