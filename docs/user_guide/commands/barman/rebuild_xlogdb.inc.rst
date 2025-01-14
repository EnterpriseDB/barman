.. _commands-barman-rebuild-xlogdb:

``barman rebuild-xlogdb``
"""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    rebuild-xlogdb
        [ { -h | --help } ]
        SERVER_NAME

Description
^^^^^^^^^^^

Rebuild the WAL file metadata for a server (or for all servers using the ``all`` shortcut)
based on the disk content. The WAL archive metadata is stored in the ``xlog.db`` file,
with each Barman server maintaining its own copy.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

``-h`` / ``--help``
    Show a help message and exit. Provides information about command usage.

.. only:: man

    Shortcuts
    ^^^^^^^^^

    Use shortcuts instead of ``SERVER_NAME``.

    .. list-table::
        :widths: 25 100
        :header-rows: 1
    
        * - **Shortcut**
          - **Description**
        * - **all**
          - All available servers
