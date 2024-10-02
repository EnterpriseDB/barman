.. _commands-barman-rebuild-xlogdb:

``barman rebuild-xlogdb``
"""""""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    rebuild-xlogdb SERVER_NAME

Description
^^^^^^^^^^^

Rebuild the WAL file metadata for a server (or for all servers using the ``all`` shortcut)
based on the disk content. The WAL archive metadata is stored in the ``xlog.db`` file,
with each Barman server maintaining its own copy.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node.

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
