.. _commands-barman-sync-info:

``barman sync-info``
""""""""""""""""""""

Synopsis
^^^^^^^^

.. code-block:: text
    
    sync-info [ --primary ] SERVER_NAME [ LAST_WAL [ LAST_POS ] ]

Description
^^^^^^^^^^^

Gather information about the current status of a Barman server for synchronization
purposes. 

This command returns a JSON output for a server that includes: all successfully 
completed backups, all archived WAL files, the configuration, the last WAL file read from
``xlog.db``, and its position within the file.

Parameters
^^^^^^^^^^

``SERVER_NAME``
    Name of the server in barman node

``LAST_WAL``
    Instructs sync-info to skip any WAL files that precede the specified file (for
    incremental synchronization).

``LAST_POS``
    Hint for quickly positioning in the ``xlog.db`` file (for incremental synchronization).
